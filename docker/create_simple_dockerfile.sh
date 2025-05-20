#!/bin/bash
set -e

# Create a new Dockerfile with a simpler approach
cat > Dockerfile.rust.simple << 'EOL'
FROM rust:slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    build-essential \
    cmake \
    libclang-dev \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a new empty project
WORKDIR /app

# Copy over your manifests and source code
COPY Cargo.toml ./
COPY src/ ./src/
COPY .env ./

# Modify Cargo.toml to use edition 2021 instead of 2024
RUN sed -i 's/edition = "2024"/edition = "2021"/' Cargo.toml

# Run cargo update to generate Cargo.lock
RUN cargo update

# Build your application with release profile
RUN cargo build --release

# Runtime stage
FROM debian:12-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/target/release/cryptics_lab_bot /app/cryptics_lab_bot
# Copy the .env file
COPY .env /app/.env
# Copy the config file
COPY config.toml /app/config.toml

# Set the startup command
CMD ["/app/cryptics_lab_bot"]
EOL

# Update docker-compose.yml to use the new Dockerfile
sed -i.bak 's|dockerfile: docker/Dockerfile.rust.alt|dockerfile: docker/Dockerfile.rust.simple|' docker-compose.yml

echo "Created Dockerfile.rust.simple and updated docker-compose.yml"
echo "Run 'docker-compose build rust-bot' to build with the new Dockerfile"
