# Blockscout MDBX Deployment Guide

## Overview

This guide covers the complete production deployment process for Blockscout with MDBX database backend, including monitoring, rollback procedures, and operational best practices.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Initial Deployment](#initial-deployment)
3. [Monitoring and Metrics](#monitoring-and-metrics)
4. [Rollback Procedures](#rollback-procedures)
5. [Operational Procedures](#operational-procedures)
6. [Troubleshooting](#troubleshooting)
7. [Performance Benchmarks](#performance-benchmarks)

---

## Prerequisites

### Server Requirements

- **OS**: Linux (Ubuntu 20.04+ or similar)
- **RAM**: 8GB minimum, 16GB recommended
- **Disk**: 100GB+ SSD (NVMe preferred)
- **CPU**: 4+ cores
- **Network**: Stable connection to Reth node

### Software Requirements

- Rust 1.70+ (for building)
- SSH access to deployment server
- systemd for service management
- Prometheus and Node Exporter (optional, for monitoring)

### Access Requirements

- SSH key configured for `root@aya`
- Running Reth node with accessible database
- (Optional) Meilisearch instance for search functionality

---

## Initial Deployment

### Step 1: Build and Deploy

Run the automated deployment script:

```bash
# From the project root
./scripts/deploy.sh
```

For a dry-run to preview changes:

```bash
./scripts/deploy.sh --dry-run
```

### Step 2: Verify Deployment

After deployment, verify the service is running:

```bash
# Check service status
ssh root@aya systemctl status blockscout-api

# Check API health
ssh root@aya curl http://localhost:4000/health

# View logs
ssh root@aya journalctl -u blockscout-api -f
```

### What the Deployment Script Does

1. **Pre-deployment checks**: Verifies SSH connectivity and project structure
2. **Build**: Compiles release binaries with MDBX support
3. **Backup**: Creates timestamped backup of existing installation
4. **Stop services**: Gracefully stops running Blockscout API
5. **Deploy binaries**: Copies new binaries to `/usr/local/bin`
6. **Deploy service**: Updates systemd service configuration
7. **Start services**: Enables and starts the Blockscout API service
8. **Health check**: Verifies API is responding correctly

### Deployment Directory Structure

```
/usr/local/bin/
├── blockscout-api          # Main API server
├── blockscout-backfill     # Historical data backfill
└── blockscout-subscriber   # Live block subscriber

/etc/blockscout/
└── config.toml             # Optional configuration file

/var/lib/blockscout/
└── mdbx/                   # MDBX database files

/var/log/blockscout/        # Application logs (via journald)

/var/backups/blockscout/
└── backup_YYYYMMDD_HHMMSS/ # Timestamped backups
```

---

## Initial Data Backfill

### Running the Backfill

```bash
# SSH to server
ssh root@aya

# Run backfill (adjust paths and block range as needed)
/usr/local/bin/blockscout-backfill \
    --mdbx-path /var/lib/blockscout/mdbx \
    --reth-db /path/to/reth/db \
    --reth-static-files /path/to/reth/static_files \
    --chain sepolia \
    --batch-size 100
```

**Expected Performance**:
- MDBX backfill: ~100 blocks/sec
- Total time for 20M blocks: ~55 hours

### Verify Data Integrity

```bash
# Check indexing status
curl http://localhost:4000/api/v2/main-page/indexing-status

# Test sample address
curl http://localhost:4000/api/v2/addresses/0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb/transactions
```

---

## Monitoring and Metrics

### Setup Monitoring Stack

Run the monitoring setup script:

```bash
./scripts/setup-monitoring.sh
```

This installs and configures:
- **Prometheus**: Metrics collection and alerting
- **Node Exporter**: System-level metrics
- **Alerting rules**: Pre-configured alerts for critical issues

### Access Monitoring Dashboards

```
Prometheus:   http://aya:9090
Metrics API:  http://aya:4000/metrics (when implemented)
```

### Key Metrics to Monitor

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `blockscout_last_indexed_block` | Latest indexed block | Lag > 100 blocks |
| `blockscout_api_latency_seconds{quantile="0.99"}` | P99 API latency | > 50ms |
| `blockscout_http_requests_total` | Total HTTP requests | Error rate > 1% |
| `blockscout_mdbx_size_bytes` | Database size | Disk usage > 80% |
| `blockscout_cache_hit_rate` | Cache effectiveness | < 80% |

### Alerting Rules

Pre-configured alerts include:

- **BlockscoutAPIDown**: Service unavailable for 2+ minutes
- **HighAPILatency**: P99 latency exceeds 50ms for 5+ minutes
- **HighErrorRate**: HTTP 5xx errors exceed 1% for 5+ minutes
- **HighDiskUsage**: < 20% free space on data partition
- **IndexingLag**: More than 100 blocks behind chain head

---

## Rollback Procedures

### When to Rollback

Consider rollback if:
- API health checks fail after deployment
- Critical bugs discovered in new version
- Performance degradation observed
- Data integrity issues detected

### Automatic Rollback

The deployment script creates automatic backups. To rollback:

```bash
# List available backups
ssh root@aya ls -lh /var/backups/blockscout/

# Rollback to specific backup
./scripts/rollback.sh 20231205_143000
```

### Manual Rollback Steps

If automatic rollback fails:

```bash
# 1. Stop service
ssh root@aya systemctl stop blockscout-api

# 2. Restore binaries manually
ssh root@aya "cp /var/backups/blockscout/backup_TIMESTAMP/blockscout-api /usr/local/bin/"

# 3. Restore service file
ssh root@aya "cp /var/backups/blockscout/backup_TIMESTAMP/blockscout-api.service /etc/systemd/system/"

# 4. Reload and restart
ssh root@aya systemctl daemon-reload
ssh root@aya systemctl start blockscout-api

# 5. Verify
ssh root@aya systemctl status blockscout-api
```

---

## Operational Procedures

### Daily Operations

#### View Logs

```bash
# Follow live logs
ssh root@aya journalctl -u blockscout-api -f

# View last 100 lines
ssh root@aya journalctl -u blockscout-api -n 100

# Filter by time
ssh root@aya journalctl -u blockscout-api --since "1 hour ago"

# Filter by severity
ssh root@aya journalctl -u blockscout-api -p err
```

#### Restart Service

```bash
# Graceful restart
ssh root@aya systemctl restart blockscout-api

# Check status
ssh root@aya systemctl status blockscout-api
```

#### Check Resource Usage

```bash
# Check disk usage
ssh root@aya du -sh /var/lib/blockscout/mdbx

# Check memory usage
ssh root@aya ps aux | grep blockscout

# Check MDBX database stats (if mdbx_stat available)
ssh root@aya mdbx_stat /var/lib/blockscout/mdbx
```

### Backup Procedures

#### Create Manual Backup

```bash
ssh root@aya "tar czf /var/backups/blockscout/manual-backup-$(date +%Y%m%d_%H%M%S).tar.gz /var/lib/blockscout/mdbx"
```

#### Restore from Backup

```bash
# Stop service
ssh root@aya systemctl stop blockscout-api

# Restore data
ssh root@aya "tar xzf /var/backups/blockscout/manual-backup-TIMESTAMP.tar.gz -C /"

# Start service
ssh root@aya systemctl start blockscout-api
```

### Performance Tuning

#### MDBX Configuration

Edit service file for tuning:

```bash
ssh root@aya vi /etc/systemd/system/blockscout-api.service
```

Add environment variables:

```ini
Environment=MDBX_MAX_SIZE=107374182400  # 100GB max DB size
Environment=MDBX_GROWTH_STEP=4294967296  # 4GB growth increments
```

#### Resource Limits

Adjust systemd limits:

```ini
LimitNOFILE=65536         # File descriptors
MemoryLimit=8G            # Memory limit
CPUQuota=400%             # 4 CPU cores
```

### Log Rotation

Configure journald log rotation:

```bash
ssh root@aya vi /etc/systemd/journald.conf
```

Set:

```ini
SystemMaxUse=2G           # Max journal size
MaxRetentionSec=7d        # Keep logs for 7 days
```

---

## Troubleshooting

### Service Won't Start

#### Check Logs

```bash
ssh root@aya journalctl -u blockscout-api -xe
```

Common issues:
- **Port already in use**: Another process is using port 4000
- **MDBX path missing**: `/var/lib/blockscout/mdbx` doesn't exist
- **Permission denied**: Service user lacks permissions

#### Verify Configuration

```bash
# Check service file
ssh root@aya cat /etc/systemd/system/blockscout-api.service

# Check binary exists
ssh root@aya ls -lh /usr/local/bin/blockscout-api

# Test binary manually
ssh root@aya "/usr/local/bin/blockscout-api --help"
```

### High Memory Usage

```bash
# Check process memory
ssh root@aya ps aux | grep blockscout-api

# Check for memory leaks
ssh root@aya journalctl -u blockscout-api | grep -i "out of memory"

# Restart to free memory
ssh root@aya systemctl restart blockscout-api
```

### Slow API Responses

#### Check Database Size

```bash
ssh root@aya du -sh /var/lib/blockscout/mdbx/*
```

#### Check System Load

```bash
ssh root@aya uptime
ssh root@aya iostat -x 1 5
```

#### Enable Query Logging

Edit service to increase log level:

```ini
Environment=RUST_LOG=debug
```

### Database Corruption

If MDBX database is corrupted:

```bash
# 1. Stop service
ssh root@aya systemctl stop blockscout-api

# 2. Backup corrupted DB
ssh root@aya mv /var/lib/blockscout/mdbx /var/lib/blockscout/mdbx.corrupted

# 3. Restore from backup or re-run backfill
ssh root@aya /usr/local/bin/blockscout-backfill \
    --mdbx-path /var/lib/blockscout/mdbx \
    --reth-db /path/to/reth/db \
    --reth-static-files /path/to/reth/static_files \
    --chain sepolia

# 4. Restart service
ssh root@aya systemctl start blockscout-api
```

---

## Performance Benchmarks

### Expected Performance Metrics

Based on benchmarking suite (`cargo bench --bench mdbx_bench`):

| Metric | Target |
|--------|--------|
| **Backfill Speed** | ~100 blocks/sec |
| **API Latency (P99)** | < 50ms |
| **Recovery Time** | < 1 minute |
| **Concurrent Reads** | > 200 QPS |

### Running Benchmarks

#### Comprehensive Benchmark Suite

```bash
# Run all benchmarks
cargo bench --bench mdbx_bench --no-default-features --features reth

# Run specific benchmark
cargo bench --bench mdbx_bench -- backfill_speed
```

#### Production Load Testing

```bash
# Install hey (HTTP load testing tool)
go install github.com/rakyll/hey@latest

# Test API endpoint
hey -n 10000 -c 100 -m GET "http://aya:4000/api/v2/addresses/0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb/transactions"

# Expected results:
# - P99 latency < 50ms
# - No errors
# - Consistent throughput > 1000 RPS
```

### Monitoring Performance in Production

```bash
# Check Prometheus metrics
curl http://aya:9090/api/v1/query?query=blockscout_api_latency_seconds

# Visualize in Grafana (after setup)
# Import dashboard from scripts/setup-monitoring.sh output
```

---

## Emergency Contacts and Escalation

### When to Escalate

- API down for > 5 minutes
- Data integrity issues detected
- Unrecoverable database corruption
- Security incidents

### Quick Command Reference

```bash
# Emergency restart
ssh root@aya systemctl restart blockscout-api

# Emergency rollback
./scripts/rollback.sh <latest_backup_timestamp>

# View critical errors
ssh root@aya journalctl -u blockscout-api -p err --since "10 minutes ago"

# Check if service is responding
ssh root@aya curl -f http://localhost:4000/health || echo "HEALTH CHECK FAILED"
```

---

## Appendix

### File Locations Quick Reference

```
Binaries:           /usr/local/bin/blockscout-*
Service Files:      /etc/systemd/system/blockscout-*.service
Configuration:      /etc/blockscout/
Database:           /var/lib/blockscout/mdbx/
Logs:               journalctl -u blockscout-api
Backups:            /var/backups/blockscout/
Monitoring Config:  /etc/prometheus/
```

### Useful Commands

```bash
# Service management
systemctl start blockscout-api
systemctl stop blockscout-api
systemctl restart blockscout-api
systemctl status blockscout-api
systemctl enable blockscout-api
systemctl disable blockscout-api

# Logs
journalctl -u blockscout-api -f              # Follow logs
journalctl -u blockscout-api -n 100          # Last 100 lines
journalctl -u blockscout-api --since "1h ago"  # Last hour
journalctl -u blockscout-api -p err          # Errors only

# Database
du -sh /var/lib/blockscout/mdbx              # Check size
ls -lh /var/lib/blockscout/mdbx/             # List files

# Monitoring
curl http://localhost:4000/health            # Health check
curl http://localhost:4000/metrics           # Prometheus metrics
curl http://localhost:9090/api/v1/query?query=up  # Prometheus status
```

### Related Documentation

- [README.md](README.md) - Project overview
- [BENCHMARKING.md](BENCHMARKING.md) - Benchmarking guide
- [QUICKSTART.md](QUICKSTART.md) - Quick start guide

---

**Last Updated**: 2024-12-06
**Version**: 2.0
**Maintained by**: Blockscout Development Team
