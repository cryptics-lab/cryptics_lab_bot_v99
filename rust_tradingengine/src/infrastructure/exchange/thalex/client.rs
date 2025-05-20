use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tungstenite::Message;
use url::Url;

use chrono::Utc;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::Serialize;
use log::{debug, error};
use anyhow::{anyhow};

use crate::domain::enums::*;
use crate::domain::model::exchange::*;

#[derive(Debug, Clone)]
pub struct ThalexKeys {
    pub kid: String,
    pub private_key: String,
}

#[derive(Debug, Clone)]
pub enum Network {
    TEST,
    PROD,
}

impl Network {
    pub fn url(&self) -> &'static str {
        match self {
            Network::TEST => "wss://testnet.thalex.com/ws/api/v2",
            Network::PROD => "wss://thalex.com/ws/api/v2",
        }
    }
}

impl ThalexKeys {
    pub fn from_env(env: &Network) -> Self {
        match env {
            Network::TEST => Self {
                kid: std::env::var("THALEX_KID_TEST").expect("Missing THALEX_KID_TEST"),
                private_key: std::env::var("THALEX_KEY_TEST")
                    .expect("Missing THALEX_KEY_TEST")
                    .replace("\\n", "\n"),
            },
            Network::PROD => Self {
                kid: std::env::var("THALEX_KID_PROD").expect("Missing THALEX_KID_PROD"),
                private_key: std::env::var("THALEX_KEY_PROD")
                    .expect("Missing THALEX_KEY_PROD")
                    .replace("\\n", "\n"),
            },
        }
    }

    pub fn make_auth_token(&self) -> Result<String, jsonwebtoken::errors::Error> {
        #[derive(Serialize)]
        struct Claims {
            iat: i64,
        }

        let claims = Claims {
            iat: Utc::now().timestamp() + (rand::random::<u8>() as i64 % 10),
        };

        let mut header = Header::new(Algorithm::RS512);
        header.kid = Some(self.kid.clone());

        let key = EncodingKey::from_rsa_pem(self.private_key.as_bytes())?;

        encode(&header, &claims, &key)
    }
}

pub struct ThalexClient {
    pub socket: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

impl Default for ThalexClient {
    fn default() -> Self {
        Self::new()
    }
}

impl ThalexClient {
    pub fn new() -> Self {
        ThalexClient { socket: None }
    }

    pub async fn connect(&mut self, network: Network) -> Result<()> {
        let url = Url::parse(network.url())?;
        let (socket, _) = connect_async(url).await?;
        self.socket = Some(socket);
        Ok(())
    }

    pub async fn disconnect(&mut self) -> Result<()> {
        if let Some(mut socket) = self.socket.take() {
            socket.close(None).await?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("WebSocket not connected"))
        }
    }

    pub fn connected(&self) -> bool {
        self.socket.is_some()
    }

    async fn send(
        &mut self,
        method: &str,
        id: Option<u64>,
        params: serde_json::Value,
    ) -> Result<()> {
        let mut request = json!({
            "method": method,
            "params": params,
        });

        if let Some(id_value) = id {
            request["id"] = json!(id_value)
        }

        let request_text = serde_json::to_string(&request)?;
        println!("Sending request: {}", request_text);

        if let Some(socket) = &mut self.socket {
            socket.send(Message::Text(request_text)).await?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("WebSocket not connected"))
        }
    }

    /// Receive a message from the WebSocket server and return as a String
    pub async fn receive(&mut self) -> Result<Option<String>> {
        if let Some(socket) = &mut self.socket {
            match socket.next().await {
                Some(Ok(msg)) => {
                    // Handle each type of WebSocket message
                    match msg {
                        Message::Text(text) => {
                            debug!("Received text: {}", text);
                            // Return the text message as a String
                            Ok(Some(text))
                        }
                        Message::Binary(_) => {
                            debug!("Received binary message");
                            Ok(None) // Binary messages are ignored, return None
                        }
                        Message::Ping(_) => {
                            debug!("Received ping, automatically responding with pong");
                            Ok(None) // No String to return for Ping, return None
                        }
                        Message::Pong(_) => {
                            debug!("Received pong");
                            Ok(None) // No String to return for Pong, return None
                        }
                        Message::Close(_) => {
                            debug!("Received close frame");
                            Ok(None) // No String to return for Close, return None
                        }
                        Message::Frame(_) => {
                            debug!("Received raw frame");
                            Ok(None) // No String to return for Frame, return None
                        }
                    }
                }
                Some(Err(e)) => {
                    error!("Error receiving message: {}", e);
                    // Connection likely broken, clear the socket and propagate the error
                    self.socket = None;
                    Err(anyhow!("WebSocket error: {}", e)) // Return the error wrapped in Result
                }
                None => {
                    debug!("WebSocket stream ended");
                    // Connection closed, clear the socket
                    self.socket = None;
                    Ok(None) // Return Ok(None) when the WebSocket stream ends
                }
            }
        } else {
            error!("Not connected to WebSocket server");
            Err(anyhow!("Not connected to WebSocket server")) // Handle if the socket is not available
        }
    }

    pub async fn login(
        &mut self,
        token: String,
        account: Option<String>,
        id: Option<u64>,
    ) -> Result<()> {
        let mut params = json!({"token": token});
        if let Some(account_id) = account {
            params["account"] = json!(account_id);
        }
        self.send("public/login", id, params).await
    }

    pub async fn insert(
        &mut self, 
        order: OrderRequest, 
        id: Option<u64>
    ) -> Result<()>{
       let mut params = json!({
                        "direction": match order.side{
                            OrderSide::Buy => "buy",
                            OrderSide::Sell => "sell"
                        },
                        "instrument_name": order.symbol,
                        "client_order_id": order.client_order_id,
                        "price": order.price,
                        "amount": order.quantity,
                        "order_type": match order.order_type{
                            OrderType::Limit => "limit",
                            OrderType::Market => "market"
                        }
                });

        // Only add time_in_force to params if it is exists in order
        if let Some(time_in_force) = order.time_in_force.map(|x| match x{
            TimeInForce::GTC => "good_till_cancelled",
            TimeInForce::IOC => "immediate_or_cancel"
        }) {
            params["time_in_force"]  = json!(time_in_force);
        }

        self.send("private/insert", id, params).await
    }

    pub async fn cancel(
        &mut self,
        order_id: Option<String>,
        client_order_id: Option<u64>,
        id: Option<u64>,
    ) -> Result<()> {
        let params = match (client_order_id, order_id) {
            (Some(cid), None) => json!({ "client_order_id": cid }),
            (None, Some(oid)) => json!({ "order_id": oid }),
            _ => return Err(anyhow::anyhow!("Exactly one of `client_order_id` or `order_id` must be specified.")),
        };
    
        self.send("private/cancel", id, params).await
    }

    pub async fn amend(
        &mut self,
        quantity: f64, 
        price: f64,
        order_id: Option<String>,
        client_order_id: Option<u64>,
        id: Option<u64>,
    ) -> Result<()> {
        // Correct: build an object with a key
        let mut params = match (order_id, client_order_id) {
            (Some(oid), None) => json!({ "order_id": oid }),
            (None, Some(cid)) => json!({ "client_order_id": cid }),
            _ => return Err(anyhow::anyhow!("Exactly one of `client_order_id` or `order_id` must be specified.")),
        };
    
        // Now safely mutate the JSON object
        if let Some(obj) = params.as_object_mut() {
            obj.insert("price".to_string(), json!(price));
            obj.insert("amount".to_string(), json!(quantity));
        } else {
            return Err(anyhow::anyhow!("Failed to build JSON params"));
        }
    
        self.send("private/amend", id, params).await
    }

    pub async fn set_cancel_on_disconnect(&mut self, timeout_secs: u64, id: Option<u64>)
     -> Result<()> {
        let params = json!({ "timeout_secs": timeout_secs });
        self.send("private/set_cancel_on_disconnect", id, params).await
    }

    // Bulk cancel all orders in session
    pub async fn cancel_session(&mut self, id: Option<u64>) -> Result<()>{
        self.send("private/cancel_session", id, json!({})).await?;
        Ok(())
    }

    pub async fn instruments(&mut self, id: Option<u64>) -> Result<()> {
        self.send("public/instruments",    id,  json!({})).await?;
        Ok(())
    }

    pub async fn private_subscribe(&mut self, channels: Vec<String>, id: Option<u64>)-> Result<()>
    {
        let params = json!({"channels": channels});
        self.send("private/subscribe", id, params).await
    }

    pub async fn public_subscribe(&mut self, channels: Vec<String>, id: Option<u64>)-> Result<()>
    {
        let params = json!({"channels": channels});
        self.send("public/subscribe", id, params).await
    }

    pub async fn unsubscribe(&mut self, channels: Vec<String>, id: Option<u64>)-> Result<()>
    {
        let params = json!({"channels": channels});
        self.send("unsubscribe", id, params).await
    }
}
