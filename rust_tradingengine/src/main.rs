// Standard library imports
use std::sync::Arc;
use std::path::Path;

// External crate imports
use anyhow::Result;
use dotenv::dotenv;
use log::{debug, error, info, warn};
use tokio::sync::{broadcast, Mutex};
use tokio::time::{sleep, Duration};
use tokio::select;

// Internal crate imports
use cryptics_lab_bot::config_loader::AppConfig;
use cryptics_lab_bot::infrastructure::exchange::thalex::client::{Network, ThalexClient, ThalexKeys};
use cryptics_lab_bot::domain::constants::*;
use cryptics_lab_bot::strategies::thalex_market_maker::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    dotenv().ok();
    // Use a more explicit Builder that doesn't check environment variables
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();
    info!("Logger initialized");

    // Load configuration from TOML file (first try relative path, then absolute path as backup)
    let config_path = Path::new("../config.toml");
    let config = match AppConfig::from_file(config_path) {
        Ok(config) => config,
        Err(e) => {
            warn!("Failed to load config from {}: {}", config_path.display(), e);
            
            // Try alternate path
            let alt_path = Path::new("./config.toml");
            info!("Attempting to load from alternate path: {}", alt_path.display());
            AppConfig::from_file(alt_path)?
        }
    };
    
    // Wrap config in Arc for thread-safe sharing
    let config = Arc::new(config);
    info!("Configuration loaded, running in docker: {}", config.app.rust_running_in_docker);
    
    // Run the bot with the configuration
    run_bot(config).await
}

/// Main bot run function
async fn run_bot(config: Arc<AppConfig>) -> Result<()> {
    let network = Network::TEST;
    let keys = ThalexKeys::from_env(&network);

    // Set up signal handler for SIGINT (Ctrl+C)
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;

    loop {
        info!("Launching bot with new session");
        let token = keys.make_auth_token()?;

        // Create and connect client
        let mut raw_client = ThalexClient::new();
        raw_client.connect(network.clone()).await?;

        if let Some(msg) = raw_client.receive().await? {
            debug!("Initial connection response: {}", msg);
        }

        raw_client.login(token.clone(), None, Some(CALL_ID_LOGIN)).await?;
        if let Some(msg) = raw_client.receive().await? {
            debug!("Login response: {}", msg);
        }

        // Create a broadcast channel for shutdown signaling
        let (shutdown_tx, _) = broadcast::channel::<()>(3);
        let shared_client = Arc::new(Mutex::new(raw_client));
        
        // Initialize the quoter with the client and config
        let quoter = Arc::new(ThalexQuoter::new(
            shared_client.clone(), 
            Some(config.clone())
        ).await);

        // Start the trading tasks
        let (should_exit, _) = run_tasks(quoter, shutdown_tx, &mut sigint).await?;

        // Clean up the client connection
        info!("Running cleanup...");
        cleanup(shared_client.clone()).await;

        // If we received a termination signal, exit the loop
        if should_exit {
            info!("Exiting program");
            break;
        }

        // Otherwise prepare to reconnect
        sleep(Duration::from_secs(1)).await;
        warn!("Reconnecting...");
    }

    Ok(())
}

/// Run the necessary trading tasks
async fn run_tasks(
    quoter: Arc<ThalexQuoter>,
    shutdown_tx: broadcast::Sender<()>,
    sigint: &mut tokio::signal::unix::Signal,
) -> Result<(bool, Option<anyhow::Error>)> {
    // Create separate variables for each task handle
    let mut quote_handle = tokio::spawn({
        let quoter = quoter.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        async move {
            if let Err(e) = quoter.quote_task(shutdown_rx).await {
                error!("Quote task failed: {:?}", e);
                return Err(e);
            }
            Ok(())
        }
    });

    let mut listen_handle = tokio::spawn({
        let quoter = quoter.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        async move {
            if let Err(e) = quoter.listen_task(shutdown_rx).await {
                error!("Listen task failed: {:?}", e);
                return Err(e);
            }
            Ok(())
        }
    });

    let mut ping_handle = tokio::spawn({
        let quoter = quoter.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        async move {
            if let Err(e) = quoter.ping_task(shutdown_rx).await {
                error!("Ping task failed: {:?}", e);
                return Err(e);
            }
            Ok(())
        }
    });
    
    // Flag to track if we need to break out of the main loop (e.g., after Ctrl+C)
    let mut should_exit = false;
    let mut err = None;

    // Wait for any task to finish or signals
    select! {
        res = &mut quote_handle => {
            match res {
                Ok(Ok(_)) => info!("Quote task completed successfully"),
                Ok(Err(e)) => {
                    error!("Quote task returned error: {:?}", e);
                    err = Some(e);
                },
                Err(e) => error!("Quote task panicked: {:?}", e),
            }
        }
        res = &mut listen_handle => {
            match res {
                Ok(Ok(_)) => info!("Listen task completed successfully"),
                Ok(Err(e)) => {
                    error!("Listen task returned error: {:?}", e);
                    err = Some(e);
                },
                Err(e) => error!("Listen task panicked: {:?}", e),
            }
        }
        res = &mut ping_handle => {
            match res {
                Ok(Ok(_)) => info!("Ping task completed successfully"),
                Ok(Err(e)) => {
                    error!("Ping task returned error: {:?}", e);
                    err = Some(e);
                },
                Err(e) => error!("Ping task panicked: {:?}", e),
            }
        }
        _ = sigint.recv() => {
            warn!("SIGINT (Ctrl+C) received. Attempting graceful shutdown...");
            should_exit = true; // We'll exit the main loop after cleanup
        }
    }

    // Signal all tasks to shut down
    if let Err(e) = shutdown_tx.send(()) {
        error!("Failed to send shutdown signal: {}", e);
    } else {
        info!("Shutdown signal sent to all tasks");
    }

    // Give tasks a moment to process the shutdown signal
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Abort the tasks if they're still running
    for (name, handle) in [
        ("quote", &mut quote_handle),
        ("listen", &mut listen_handle), 
        ("ping", &mut ping_handle)
    ] {
        if !handle.is_finished() {
            info!("Aborting {} task", name);
            handle.abort();
        }
    }

    Ok((should_exit, err))
}

async fn cleanup(shared_client: Arc<Mutex<ThalexClient>>) {
    // Add a timeout to make sure cleanup operations don't hang
    let cleanup_future = async {
        // Use lock() to ensure we block and wait until we acquire the lock
        let lock_result = tokio::time::timeout(
            Duration::from_secs(2),
            shared_client.lock()
        ).await;
        
        let mut client = match lock_result {
            Ok(guard) => guard,
            Err(_) => {
                error!("Timeout while waiting for client lock in cleanup");
                return;
            }
        };

        if client.connected() {
            info!("Attempting to cancel session...");
            match tokio::time::timeout(
                Duration::from_secs(3),
                client.cancel_session(Some(CALL_ID_CANCEL_SESSION))
            ).await {
                Ok(Ok(_)) => info!("Session cancellation successful."),
                Ok(Err(e)) => error!("Failed to cancel session: {}", e),
                Err(_) => error!("Timeout during session cancellation"),
            }

            info!("Attempting to disconnect client...");
            match tokio::time::timeout(
                Duration::from_secs(3),
                client.disconnect()
            ).await {
                Ok(Ok(_)) => info!("Client disconnected successfully."),
                Ok(Err(e)) => error!("Failed to disconnect client: {}", e),
                Err(_) => error!("Timeout during client disconnection"),
            }
        } else {
            info!("Client is not connected, skipping cancellation and disconnect.");
        }
    };
    
    // Set an overall timeout for the entire cleanup process
    match tokio::time::timeout(Duration::from_secs(10), cleanup_future).await {
        Ok(_) => info!("Cleanup completed"),
        Err(_) => error!("Cleanup timed out after 10 seconds"),
    }
}
