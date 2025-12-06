#!/bin/bash
# Production deployment script for Blockscout MDBX
# Target: root@aya
# Usage: ./scripts/deploy.sh [--dry-run]

set -e

# ============================================================================
# Configuration
# ============================================================================

SERVER="root@aya"
BINARY_PATH="/mnt/sepolia/blockscout-exex/bin"
CONFIG_PATH="/etc/blockscout"
DATA_PATH="/mnt/sepolia/mdbx"
SERVICE_PATH="/etc/systemd/system"
LOG_PATH="/var/log/blockscout"
BACKUP_PATH="/mnt/sepolia/backups"

DRY_RUN=false
if [[ "$1" == "--dry-run" ]]; then
    DRY_RUN=true
    echo "🔍 DRY RUN MODE - No changes will be made"
fi

# ============================================================================
# Helper Functions
# ============================================================================

log_info() {
    echo "ℹ️  $1"
}

log_success() {
    echo "✅ $1"
}

log_error() {
    echo "❌ $1" >&2
}

run_remote() {
    local cmd="$1"
    if $DRY_RUN; then
        log_info "Would run on $SERVER: $cmd"
    else
        ssh "$SERVER" "$cmd"
    fi
}

copy_file() {
    local src="$1"
    local dst="$2"
    if $DRY_RUN; then
        log_info "Would copy: $src -> $SERVER:$dst"
    else
        scp "$src" "$SERVER:$dst"
    fi
}

# ============================================================================
# Pre-deployment Checks
# ============================================================================

log_info "Starting pre-deployment checks..."

# Check SSH connectivity
if ! $DRY_RUN; then
    if ! ssh -o ConnectTimeout=5 "$SERVER" "echo 'SSH connection successful'" &>/dev/null; then
        log_error "Failed to connect to $SERVER via SSH"
        exit 1
    fi
    log_success "SSH connection verified"
fi

# Check if we're in the right directory
if [[ ! -f "Cargo.toml" ]] || ! grep -q "blockscout-exex" Cargo.toml; then
    log_error "Must run from blockscout-exex project root"
    exit 1
fi
log_success "Project directory verified"

# ============================================================================
# Build Release Binaries
# ============================================================================

log_info "Building release binaries with MDBX support..."

if ! $DRY_RUN; then
    cargo build --release --features mdbx --bin blockscout-api
    cargo build --release --features mdbx --bin blockscout-backfill
    cargo build --release --features mdbx --bin blockscout-migrate
    log_success "Binaries built successfully"
else
    log_info "Would build: cargo build --release --features mdbx"
fi

# Verify binaries exist
if ! $DRY_RUN; then
    for binary in blockscout-api blockscout-backfill blockscout-migrate; do
        if [[ ! -f "target/release/$binary" ]]; then
            log_error "Binary not found: target/release/$binary"
            exit 1
        fi
    done
    log_success "All binaries verified"
fi

# ============================================================================
# Create Remote Directories
# ============================================================================

log_info "Creating remote directories..."

run_remote "mkdir -p $BINARY_PATH $CONFIG_PATH $DATA_PATH $LOG_PATH $BACKUP_PATH"
log_success "Remote directories created"

# ============================================================================
# Backup Existing Installation
# ============================================================================

log_info "Backing up existing installation..."

BACKUP_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="$BACKUP_PATH/backup_$BACKUP_TIMESTAMP"

run_remote "mkdir -p $BACKUP_DIR"

# Backup binaries if they exist
run_remote "if [ -f $BINARY_PATH/blockscout-api ]; then cp $BINARY_PATH/blockscout-api $BACKUP_DIR/; fi"
run_remote "if [ -f $BINARY_PATH/blockscout-backfill ]; then cp $BINARY_PATH/blockscout-backfill $BACKUP_DIR/; fi"
run_remote "if [ -f $BINARY_PATH/blockscout-migrate ]; then cp $BINARY_PATH/blockscout-migrate $BACKUP_DIR/; fi"

# Backup systemd service if it exists
run_remote "if [ -f $SERVICE_PATH/blockscout-api.service ]; then cp $SERVICE_PATH/blockscout-api.service $BACKUP_DIR/; fi"

log_success "Backup created at $BACKUP_DIR"

# ============================================================================
# Stop Running Services
# ============================================================================

log_info "Stopping running services..."

# Check if service exists and is running
if ! $DRY_RUN; then
    if ssh "$SERVER" "systemctl is-active --quiet blockscout-api"; then
        run_remote "systemctl stop blockscout-api"
        log_success "Blockscout API service stopped"
    else
        log_info "Service not running, skipping stop"
    fi
fi

# ============================================================================
# Deploy Binaries
# ============================================================================

log_info "Deploying binaries..."

copy_file "target/release/blockscout-api" "$BINARY_PATH/blockscout-api"
copy_file "target/release/blockscout-backfill" "$BINARY_PATH/blockscout-backfill"
copy_file "target/release/blockscout-migrate" "$BINARY_PATH/blockscout-migrate"

# Set executable permissions
run_remote "chmod +x $BINARY_PATH/blockscout-api $BINARY_PATH/blockscout-backfill $BINARY_PATH/blockscout-migrate"

log_success "Binaries deployed"

# ============================================================================
# Deploy Systemd Service
# ============================================================================

log_info "Deploying systemd service..."

copy_file "scripts/blockscout-api.service" "$SERVICE_PATH/blockscout-api.service"

log_success "Systemd service deployed"

# ============================================================================
# Deploy Configuration (if exists)
# ============================================================================

if [[ -f "deploy/config.toml" ]]; then
    log_info "Deploying configuration..."
    copy_file "deploy/config.toml" "$CONFIG_PATH/config.toml"
    log_success "Configuration deployed"
fi

# ============================================================================
# Reload Systemd and Start Service
# ============================================================================

log_info "Reloading systemd daemon..."
run_remote "systemctl daemon-reload"
log_success "Systemd daemon reloaded"

log_info "Enabling blockscout-api service..."
run_remote "systemctl enable blockscout-api"
log_success "Service enabled"

log_info "Starting blockscout-api service..."
run_remote "systemctl start blockscout-api"
log_success "Service started"

# ============================================================================
# Health Check
# ============================================================================

log_info "Performing health check..."

if ! $DRY_RUN; then
    sleep 5  # Give service time to start

    # Check if service is running
    if ssh "$SERVER" "systemctl is-active --quiet blockscout-api"; then
        log_success "Service is active"
    else
        log_error "Service failed to start"
        log_info "Checking logs..."
        ssh "$SERVER" "journalctl -u blockscout-api -n 50 --no-pager"
        exit 1
    fi

    # Test API endpoint
    log_info "Testing API endpoint..."
    if ssh "$SERVER" "curl -sf http://localhost:4000/health > /dev/null"; then
        log_success "API health check passed"
    else
        log_error "API health check failed"
        exit 1
    fi
fi

# ============================================================================
# Display Service Status
# ============================================================================

log_info "Service status:"
run_remote "systemctl status blockscout-api --no-pager -l"

# ============================================================================
# Deployment Summary
# ============================================================================

echo ""
echo "=========================================="
echo "🚀 DEPLOYMENT COMPLETE"
echo "=========================================="
echo "Server:          $SERVER"
echo "Binaries:        $BINARY_PATH"
echo "Data:            $DATA_PATH"
echo "Logs:            $LOG_PATH"
echo "Backup:          $BACKUP_DIR"
echo "=========================================="
echo ""
echo "Useful commands:"
echo "  View logs:     ssh $SERVER journalctl -u blockscout-api -f"
echo "  Restart:       ssh $SERVER systemctl restart blockscout-api"
echo "  Stop:          ssh $SERVER systemctl stop blockscout-api"
echo "  Status:        ssh $SERVER systemctl status blockscout-api"
echo "  Rollback:      ./scripts/rollback.sh $BACKUP_TIMESTAMP"
echo "=========================================="
