#!/bin/bash
# Setup monitoring and metrics for Blockscout
# This script configures Prometheus, Grafana, and alerting
# Usage: ./scripts/setup-monitoring.sh

set -e

SERVER="root@aya"
MONITORING_PATH="/opt/monitoring"
CONFIG_PATH="/etc/blockscout"

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

# ============================================================================
# Create Prometheus Configuration
# ============================================================================

log_info "Creating Prometheus configuration..."

cat > /tmp/prometheus.yml << 'EOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    cluster: 'blockscout'
    environment: 'production'

# Alertmanager configuration
alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - localhost:9093

# Load rules
rule_files:
  - /etc/prometheus/alerts/*.yml

# Scrape configurations
scrape_configs:
  # Blockscout API metrics
  - job_name: 'blockscout-api'
    static_configs:
      - targets: ['localhost:4000']
    metrics_path: '/metrics'
    scrape_interval: 10s

  # Node exporter for system metrics
  - job_name: 'node'
    static_configs:
      - targets: ['localhost:9100']

  # Prometheus self-monitoring
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']
EOF

log_success "Prometheus configuration created"

# ============================================================================
# Create Alerting Rules
# ============================================================================

log_info "Creating alerting rules..."

cat > /tmp/blockscout-alerts.yml << 'EOF'
groups:
  - name: blockscout
    interval: 30s
    rules:
      # API Health
      - alert: BlockscoutAPIDown
        expr: up{job="blockscout-api"} == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Blockscout API is down"
          description: "Blockscout API has been down for more than 2 minutes"

      # API Latency
      - alert: HighAPILatency
        expr: blockscout_api_latency_seconds{quantile="0.99"} > 0.050
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High API latency detected"
          description: "P99 latency is {{ $value }}s (threshold: 50ms)"

      # Error Rate
      - alert: HighErrorRate
        expr: rate(blockscout_http_requests_total{status=~"5.."}[5m]) > 0.01
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate detected"
          description: "Error rate is {{ $value }} requests/sec"

      # Disk Usage
      - alert: HighDiskUsage
        expr: (node_filesystem_avail_bytes{mountpoint="/var/lib/blockscout"} / node_filesystem_size_bytes{mountpoint="/var/lib/blockscout"}) < 0.2
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Low disk space on /var/lib/blockscout"
          description: "Only {{ $value | humanizePercentage }} space remaining"

      # Memory Usage
      - alert: HighMemoryUsage
        expr: (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) > 0.9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High memory usage"
          description: "Memory usage is {{ $value | humanizePercentage }}"

      # Indexing Lag
      - alert: IndexingLag
        expr: (blockscout_chain_head_block - blockscout_last_indexed_block) > 100
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Indexing is lagging behind chain head"
          description: "Lag is {{ $value }} blocks"
EOF

log_success "Alerting rules created"

# ============================================================================
# Create Grafana Dashboard JSON
# ============================================================================

log_info "Creating Grafana dashboard..."

cat > /tmp/blockscout-dashboard.json << 'EOF'
{
  "dashboard": {
    "title": "Blockscout MDBX Performance",
    "tags": ["blockscout", "mdbx"],
    "timezone": "utc",
    "panels": [
      {
        "title": "API Request Rate",
        "type": "graph",
        "gridPos": {"x": 0, "y": 0, "w": 12, "h": 8},
        "targets": [
          {
            "expr": "rate(blockscout_http_requests_total[5m])",
            "legendFormat": "{{method}} {{path}}"
          }
        ]
      },
      {
        "title": "API Latency (P50, P95, P99)",
        "type": "graph",
        "gridPos": {"x": 12, "y": 0, "w": 12, "h": 8},
        "targets": [
          {
            "expr": "blockscout_api_latency_seconds{quantile=\"0.5\"}",
            "legendFormat": "P50"
          },
          {
            "expr": "blockscout_api_latency_seconds{quantile=\"0.95\"}",
            "legendFormat": "P95"
          },
          {
            "expr": "blockscout_api_latency_seconds{quantile=\"0.99\"}",
            "legendFormat": "P99"
          }
        ]
      },
      {
        "title": "Database Size",
        "type": "graph",
        "gridPos": {"x": 0, "y": 8, "w": 12, "h": 8},
        "targets": [
          {
            "expr": "blockscout_mdbx_size_bytes",
            "legendFormat": "MDBX Size"
          }
        ]
      },
      {
        "title": "Cache Hit Rate",
        "type": "graph",
        "gridPos": {"x": 12, "y": 8, "w": 12, "h": 8},
        "targets": [
          {
            "expr": "rate(blockscout_cache_hits_total[5m]) / (rate(blockscout_cache_hits_total[5m]) + rate(blockscout_cache_misses_total[5m]))",
            "legendFormat": "Hit Rate"
          }
        ]
      },
      {
        "title": "Indexing Progress",
        "type": "graph",
        "gridPos": {"x": 0, "y": 16, "w": 24, "h": 8},
        "targets": [
          {
            "expr": "blockscout_last_indexed_block",
            "legendFormat": "Indexed Block"
          },
          {
            "expr": "blockscout_chain_head_block",
            "legendFormat": "Chain Head"
          }
        ]
      }
    ]
  }
}
EOF

log_success "Grafana dashboard created"

# ============================================================================
# Deploy to Server
# ============================================================================

log_info "Deploying monitoring configuration to $SERVER..."

# Create directories
ssh "$SERVER" "mkdir -p $MONITORING_PATH/prometheus $MONITORING_PATH/grafana /etc/prometheus/alerts"

# Copy Prometheus config
scp /tmp/prometheus.yml "$SERVER:/etc/prometheus/prometheus.yml"
scp /tmp/blockscout-alerts.yml "$SERVER:/etc/prometheus/alerts/blockscout.yml"

log_success "Configuration deployed"

# ============================================================================
# Install Prometheus (if not installed)
# ============================================================================

log_info "Checking Prometheus installation..."

if ! ssh "$SERVER" "command -v prometheus &> /dev/null"; then
    log_info "Installing Prometheus..."

    ssh "$SERVER" << 'EOSSH'
        cd /tmp
        wget https://github.com/prometheus/prometheus/releases/download/v2.45.0/prometheus-2.45.0.linux-amd64.tar.gz
        tar xvfz prometheus-2.45.0.linux-amd64.tar.gz
        cp prometheus-2.45.0.linux-amd64/prometheus /usr/local/bin/
        cp prometheus-2.45.0.linux-amd64/promtool /usr/local/bin/
        rm -rf prometheus-2.45.0.linux-amd64*
EOSSH

    log_success "Prometheus installed"
else
    log_info "Prometheus already installed"
fi

# ============================================================================
# Install Node Exporter (if not installed)
# ============================================================================

log_info "Checking Node Exporter installation..."

if ! ssh "$SERVER" "command -v node_exporter &> /dev/null"; then
    log_info "Installing Node Exporter..."

    ssh "$SERVER" << 'EOSSH'
        cd /tmp
        wget https://github.com/prometheus/node_exporter/releases/download/v1.6.1/node_exporter-1.6.1.linux-amd64.tar.gz
        tar xvfz node_exporter-1.6.1.linux-amd64.tar.gz
        cp node_exporter-1.6.1.linux-amd64/node_exporter /usr/local/bin/
        rm -rf node_exporter-1.6.1.linux-amd64*
EOSSH

    log_success "Node Exporter installed"
else
    log_info "Node Exporter already installed"
fi

# ============================================================================
# Create Prometheus Systemd Service
# ============================================================================

log_info "Creating Prometheus systemd service..."

ssh "$SERVER" "cat > /etc/systemd/system/prometheus.service" << 'EOF'
[Unit]
Description=Prometheus Monitoring System
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/prometheus \
    --config.file=/etc/prometheus/prometheus.yml \
    --storage.tsdb.path=/var/lib/prometheus \
    --web.listen-address=:9090
Restart=always

[Install]
WantedBy=multi-user.target
EOF

log_success "Prometheus service created"

# ============================================================================
# Create Node Exporter Systemd Service
# ============================================================================

log_info "Creating Node Exporter systemd service..."

ssh "$SERVER" "cat > /etc/systemd/system/node_exporter.service" << 'EOF'
[Unit]
Description=Prometheus Node Exporter
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/node_exporter
Restart=always

[Install]
WantedBy=multi-user.target
EOF

log_success "Node Exporter service created"

# ============================================================================
# Start Services
# ============================================================================

log_info "Starting monitoring services..."

ssh "$SERVER" << 'EOSSH'
    systemctl daemon-reload
    systemctl enable prometheus node_exporter
    systemctl restart prometheus node_exporter
EOSSH

log_success "Monitoring services started"

# ============================================================================
# Verify Services
# ============================================================================

log_info "Verifying services..."

if ssh "$SERVER" "systemctl is-active --quiet prometheus"; then
    log_success "Prometheus is running"
else
    log_error "Prometheus failed to start"
fi

if ssh "$SERVER" "systemctl is-active --quiet node_exporter"; then
    log_success "Node Exporter is running"
else
    log_error "Node Exporter failed to start"
fi

# ============================================================================
# Cleanup
# ============================================================================

rm -f /tmp/prometheus.yml /tmp/blockscout-alerts.yml /tmp/blockscout-dashboard.json

# ============================================================================
# Summary
# ============================================================================

echo ""
echo "=========================================="
echo "📊 MONITORING SETUP COMPLETE"
echo "=========================================="
echo "Prometheus:      http://$SERVER:9090"
echo "Node Exporter:   http://$SERVER:9100/metrics"
echo "Blockscout API:  http://$SERVER:4000/metrics"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Import Grafana dashboard from /tmp/blockscout-dashboard.json"
echo "2. Configure Alertmanager for notifications"
echo "3. Set up SSL/TLS for Prometheus web UI"
echo "=========================================="
