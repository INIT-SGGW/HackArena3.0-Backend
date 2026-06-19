# syntax=docker/dockerfile:1.4

#############################################
# 1️⃣ Build stage
#############################################
FROM rust:1.90 AS builder

# Create app directory
WORKDIR /usr/src/app

# Copy the workspace root context (passed in at build time)
COPY --from=repo_context / /usr/src/app

RUN ls -l

# Build only the game-server crate in release mode
RUN cargo build --release -p game-server

# ===============================
# 2️⃣ Runtime stage
# ===============================
FROM debian:bookworm-slim

# Install minimal dependencies (for dynamic linking)
RUN apt-get update && apt-get install -y \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a new app directory
WORKDIR /usr/local/bin

# Copy only the compiled binary
COPY --from=builder /usr/src/app/target/release/game-server .

# Copy the required resources from the builder stage
COPY --from=builder /usr/src/app/game-server/resources* ./resources

# Remove source code (it never exists in final image)
# Expose port if your server uses one (optional)
EXPOSE 50051

# Run the game server binary
CMD ["./game-server"]
