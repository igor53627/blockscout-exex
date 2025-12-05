#!/bin/bash
# Rollback script for Blockscout deployment
# Usage: ./scripts/rollback.sh [backup_timestamp]
# Example: ./scripts/rollback.sh 20231205_143000

set -e

# ============================================================================
# Configuration
# ============================================================================

SERVER="root@aya"
BINARY_PATH="/usr/local/bin"
SERVICE_PATH="/etc/systemd/system"
BACKUP_PATH="/var/backups/blockscout"

BACKUP_TIMESTAMP="$1"

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

log_warning() {
    echo "⚠️  $1"
}

# ============================================================================
# Validate Input
# ============================================================================

if [[ -z "$BACKUP_TIMESTAMP" ]]; then
    log_error "Usage: $0 <backup_timestamp>"
    log_info "Available backups:"
    ssh "$SERVER" "ls -lh $BACKUP_PATH/ | grep backup_"
    exit 1
fi

BACKUP_DIR="$BACKUP_PATH/backup_$BACKUP_TIMESTAMP"

# ============================================================================
# Verify Backup Exists
# ============================================================================

log_info "Verifying backup exists..."

if ! ssh "$SERVER" "[ -d $BACKUP_DIR ]"; then
    log_error "Backup not found: $BACKUP_DIR"
    log_info "Available backups:"
    ssh "$SERVER" "ls -lh $BACKUP_PATH/ | grep backup_"
    exit 1
fi

log_success "Backup verified: $BACKUP_DIR"

# ============================================================================
# Display Backup Contents
# ============================================================================

log_info "Backup contents:"
ssh "$SERVER" "ls -lh $BACKUP_DIR/"

# ============================================================================
# Confirmation Prompt
# ============================================================================

echo ""
log_warning "This will rollback to backup from: $BACKUP_TIMESTAMP"
log_warning "Current deployment will be stopped and replaced"
echo ""
read -p "Continue with rollback? (yes/no): " -r
echo ""

if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    log_info "Rollback cancelled"
    exit 0
fi

# ============================================================================
# Stop Current Service
# ============================================================================

log_info "Stopping current service..."

if ssh "$SERVER" "systemctl is-active --quiet blockscout-api"; then
    ssh "$SERVER" "systemctl stop blockscout-api"
    log_success "Service stopped"
else
    log_info "Service not running"
fi

# ============================================================================
# Restore Binaries
# ============================================================================

log_info "Restoring binaries from backup..."

# Restore each binary if it exists in backup
for binary in blockscout-api blockscout-backfill blockscout-migrate; do
    if ssh "$SERVER" "[ -f $BACKUP_DIR/$binary ]"; then
        ssh "$SERVER" "cp $BACKUP_DIR/$binary $BINARY_PATH/$binary"
        ssh "$SERVER" "chmod +x $BINARY_PATH/$binary"
        log_success "Restored: $binary"
    else
        log_warning "Binary not in backup: $binary"
    fi
done

# ============================================================================
# Restore Systemd Service
# ============================================================================

log_info "Restoring systemd service..."

if ssh "$SERVER" "[ -f $BACKUP_DIR/blockscout-api.service ]"; then
    ssh "$SERVER" "cp $BACKUP_DIR/blockscout-api.service $SERVICE_PATH/blockscout-api.service"
    log_success "Service file restored"
else
    log_warning "Service file not in backup"
fi

# ============================================================================
# Reload Systemd and Start Service
# ============================================================================

log_info "Reloading systemd daemon..."
ssh "$SERVER" "systemctl daemon-reload"
log_success "Systemd daemon reloaded"

log_info "Starting blockscout-api service..."
ssh "$SERVER" "systemctl start blockscout-api"
log_success "Service started"

# ============================================================================
# Health Check
# ============================================================================

log_info "Performing health check..."

sleep 5  # Give service time to start

# Check if service is running
if ssh "$SERVER" "systemctl is-active --quiet blockscout-api"; then
    log_success "Service is active"
else
    log_error "Service failed to start after rollback"
    log_info "Checking logs..."
    ssh "$SERVER" "journalctl -u blockscout-api -n 50 --no-pager"
    exit 1
fi

# Test API endpoint
log_info "Testing API endpoint..."
if ssh "$SERVER" "curl -sf http://localhost:4000/health > /dev/null"; then
    log_success "API health check passed"
else
    log_warning "API health check failed, but service is running"
fi

# ============================================================================
# Display Service Status
# ============================================================================

log_info "Service status:"
ssh "$SERVER" "systemctl status blockscout-api --no-pager -l"

# ============================================================================
# Rollback Summary
# ============================================================================

echo ""
echo "=========================================="
echo "🔄 ROLLBACK COMPLETE"
echo "=========================================="
echo "Server:          $SERVER"
echo "Restored from:   $BACKUP_DIR"
echo "Timestamp:       $BACKUP_TIMESTAMP"
echo "=========================================="
echo ""
echo "Useful commands:"
echo "  View logs:     ssh $SERVER journalctl -u blockscout-api -f"
echo "  Status:        ssh $SERVER systemctl status blockscout-api"
echo "=========================================="
