# Blockscout MDBX Quick Start Guide

## Production Deployment (5 minutes)

### 1. Deploy to Server

```bash
# From project root
./scripts/deploy.sh

# Or test first with dry-run
./scripts/deploy.sh --dry-run
```

**What it does:**
- Compiles release binaries with MDBX
- Creates backup of existing installation
- Deploys to root@aya
- Starts API service
- Runs health checks

### 2. Setup Monitoring (Optional)

```bash
./scripts/setup-monitoring.sh
```

**Provides:**
- Prometheus metrics at http://aya:9090
- System metrics via Node Exporter
- Pre-configured alerting rules

### 3. Verify Deployment

```bash
# Check service status
ssh root@aya systemctl status blockscout-api

# Test API
curl http://aya:4000/health

# View logs
ssh root@aya journalctl -u blockscout-api -f
```

---

## Running Benchmarks

### Quick Benchmark Run

```bash
# MDBX benchmarks
cargo bench --bench mdbx_bench --no-default-features --features reth

# Specific benchmark
cargo bench --bench mdbx_bench -- backfill_speed
```

### Expected Results

| Benchmark | Target |
|-----------|--------|
| Backfill Speed | 100+ blocks/sec |
| API Latency P99 | < 50ms |
| Concurrent Reads | > 200 QPS |

See [BENCHMARKING.md](BENCHMARKING.md) for details.

---

## Rollback Procedure

```bash
# List available backups
ssh root@aya ls -lh /var/backups/blockscout/

# Rollback to specific backup
./scripts/rollback.sh 20231205_143000
```

---

## Monitoring & Health Checks

### Quick Health Check

```bash
curl http://aya:4000/health
```

### View Metrics

```bash
# Prometheus
open http://aya:9090

# API metrics (when implemented)
curl http://aya:4000/metrics
```

### Check Logs

```bash
# Live logs
ssh root@aya journalctl -u blockscout-api -f

# Recent errors
ssh root@aya journalctl -u blockscout-api -p err --since "1 hour ago"
```

---

## Important Paths

```
Server: root@aya

Binaries:   /usr/local/bin/blockscout-*
Database:   /var/lib/blockscout/mdbx
Config:     /etc/blockscout/
Backups:    /var/backups/blockscout/
Logs:       journalctl -u blockscout-api
```

---

## Emergency Procedures

### Service Won't Start

```bash
# Check logs
ssh root@aya journalctl -u blockscout-api -xe

# Verify binary
ssh root@aya /usr/local/bin/blockscout-api --help

# Restart service
ssh root@aya systemctl restart blockscout-api
```

### Rollback to Previous Version

```bash
# List available backups
ssh root@aya ls -lh /var/backups/blockscout/

# Rollback
./scripts/rollback.sh <last_good_backup_timestamp>
```

---

## Full Documentation

- [DEPLOYMENT.md](DEPLOYMENT.md) - Complete deployment guide
- [BENCHMARKING.md](BENCHMARKING.md) - Benchmarking guide
- [README.md](README.md) - Project overview

---

## Performance Targets

| Metric | Target |
|--------|--------|
| **Backfill Speed** | 100+ blocks/sec |
| **Recovery Time** | < 1 minute |
| **API Latency P99** | < 50ms |

---

**Last Updated**: 2024-12-06
**Version**: 2.0
**Status**: Production Ready
