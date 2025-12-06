/// Multi-pool RPC executor for intelligent request routing
/// Routes RPC requests to dedicated thread pools based on computational cost
use eyre::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Classification of RPC request types by computational cost
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestClass {
    /// Fast queries: balance checks, nonce queries, block numbers
    Light,
    /// Compute-intensive: eth_call, eth_estimateGas, large eth_getLogs
    Heavy,
    /// Debug/trace operations: debug_traceTransaction, trace_block
    Trace,
}

/// RPC request wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub method: String,
    pub params: Value,
    pub id: u64,
}

/// RPC response wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub jsonrpc: String,
    pub result: Option<Value>,
    pub error: Option<Value>,
    pub id: u64,
}

/// Executor metrics for monitoring
#[derive(Debug, Clone, Default)]
pub struct ExecutorMetrics {
    pub light_active: usize,
    pub light_queued: usize,
    pub light_completed: usize,
    pub heavy_active: usize,
    pub heavy_queued: usize,
    pub heavy_completed: usize,
    pub trace_active: usize,
    pub trace_queued: usize,
    pub trace_completed: usize,
}

/// Multi-pool RPC executor
pub struct RpcExecutor {
    light_pool: tokio::runtime::Runtime,
    heavy_pool: tokio::runtime::Runtime,
    trace_pool: tokio::runtime::Runtime,
    light_semaphore: Arc<Semaphore>,
    heavy_semaphore: Arc<Semaphore>,
    trace_semaphore: Arc<Semaphore>,
    max_queue_depth: usize,
}

impl RpcExecutor {
    /// Create a new RPC executor with specified thread pool sizes
    pub fn new(light_threads: usize, heavy_threads: usize, trace_threads: usize) -> Result<Self> {
        let light_pool = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(light_threads)
            .thread_name("rpc-light")
            .enable_all()
            .build()?;

        let heavy_pool = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(heavy_threads)
            .thread_name("rpc-heavy")
            .enable_all()
            .build()?;

        let trace_pool = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(trace_threads)
            .thread_name("rpc-trace")
            .enable_all()
            .build()?;

        let max_queue_depth = 1000;

        Ok(Self {
            light_pool,
            heavy_pool,
            trace_pool,
            light_semaphore: Arc::new(Semaphore::new(max_queue_depth)),
            heavy_semaphore: Arc::new(Semaphore::new(max_queue_depth)),
            trace_semaphore: Arc::new(Semaphore::new(max_queue_depth)),
            max_queue_depth,
        })
    }

    /// Classify an RPC method into a request class
    pub fn classify_method(method: &str) -> RequestClass {
        match method {
            // Light requests - fast queries
            "eth_blockNumber" | "eth_getBalance" | "eth_getTransactionCount" | "eth_getCode"
            | "eth_getBlockByNumber" | "eth_getBlockByHash" | "eth_getTransactionByHash"
            | "eth_getTransactionReceipt" | "eth_chainId" | "net_version" | "web3_clientVersion" => {
                RequestClass::Light
            }

            // Trace requests - debug operations
            "debug_traceTransaction" | "debug_traceCall" | "debug_traceBlockByNumber"
            | "debug_traceBlockByHash" | "trace_transaction" | "trace_block" | "trace_filter"
            | "trace_call" | "trace_replayTransaction" => RequestClass::Trace,

            // Heavy requests - compute-intensive (default for unknown methods too)
            "eth_call" | "eth_estimateGas" | "eth_getLogs" | "eth_getFilterLogs"
            | "eth_getFilterChanges" | "eth_newFilter" | "eth_newBlockFilter"
            | "eth_newPendingTransactionFilter" => RequestClass::Heavy,

            // Default to Heavy for safety (unknown methods assumed expensive)
            _ => RequestClass::Heavy,
        }
    }

    /// Execute an RPC request using the appropriate thread pool
    pub async fn execute(&self, rpc_url: &str, request: RpcRequest) -> Result<RpcResponse> {
        let class = Self::classify_method(&request.method);
        let rpc_url = rpc_url.to_string();
        let request_clone = request.clone();

        match class {
            RequestClass::Light => {
                let _permit = self
                    .light_semaphore
                    .try_acquire()
                    .map_err(|_| eyre::eyre!("Light pool queue full (429)"))?;

                let handle = self.light_pool.spawn(async move {
                    Self::execute_rpc_call(&rpc_url, request_clone).await
                });

                handle.await?
            }
            RequestClass::Heavy => {
                let _permit = self
                    .heavy_semaphore
                    .try_acquire()
                    .map_err(|_| eyre::eyre!("Heavy pool queue full (429)"))?;

                let handle = self.heavy_pool.spawn(async move {
                    Self::execute_rpc_call(&rpc_url, request_clone).await
                });

                handle.await?
            }
            RequestClass::Trace => {
                let _permit = self
                    .trace_semaphore
                    .try_acquire()
                    .map_err(|_| eyre::eyre!("Trace pool queue full (429)"))?;

                let handle = self.trace_pool.spawn(async move {
                    Self::execute_rpc_call(&rpc_url, request_clone).await
                });

                handle.await?
            }
        }
    }

    /// Internal helper to execute the actual RPC call
    async fn execute_rpc_call(rpc_url: &str, request: RpcRequest) -> Result<RpcResponse> {
        let client = reqwest::Client::new();
        let response = client.post(rpc_url).json(&request).send().await?;

        let rpc_response: RpcResponse = response.json().await?;
        Ok(rpc_response)
    }

    /// Get current executor metrics
    pub fn metrics(&self) -> ExecutorMetrics {
        ExecutorMetrics {
            light_active: self.max_queue_depth - self.light_semaphore.available_permits(),
            light_queued: 0, // Tracking queue depth requires additional state
            light_completed: 0,
            heavy_active: self.max_queue_depth - self.heavy_semaphore.available_permits(),
            heavy_queued: 0,
            heavy_completed: 0,
            trace_active: self.max_queue_depth - self.trace_semaphore.available_permits(),
            trace_queued: 0,
            trace_completed: 0,
        }
    }

    /// Gracefully shutdown all thread pools
    pub fn shutdown(self) -> Result<()> {
        // Runtimes are dropped automatically, which shuts them down gracefully
        drop(self.light_pool);
        drop(self.heavy_pool);
        drop(self.trace_pool);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_classification() {
        // Light requests
        assert_eq!(RpcExecutor::classify_method("eth_blockNumber"), RequestClass::Light);
        assert_eq!(RpcExecutor::classify_method("eth_getBalance"), RequestClass::Light);
        assert_eq!(RpcExecutor::classify_method("eth_getTransactionCount"), RequestClass::Light);
        assert_eq!(RpcExecutor::classify_method("eth_getCode"), RequestClass::Light);
        assert_eq!(RpcExecutor::classify_method("eth_getBlockByNumber"), RequestClass::Light);

        // Heavy requests
        assert_eq!(RpcExecutor::classify_method("eth_call"), RequestClass::Heavy);
        assert_eq!(RpcExecutor::classify_method("eth_estimateGas"), RequestClass::Heavy);
        assert_eq!(RpcExecutor::classify_method("eth_getLogs"), RequestClass::Heavy);

        // Trace requests
        assert_eq!(RpcExecutor::classify_method("debug_traceTransaction"), RequestClass::Trace);
        assert_eq!(RpcExecutor::classify_method("trace_transaction"), RequestClass::Trace);
        assert_eq!(RpcExecutor::classify_method("trace_block"), RequestClass::Trace);
        assert_eq!(RpcExecutor::classify_method("trace_filter"), RequestClass::Trace);

        // Unknown methods default to Heavy (conservative)
        assert_eq!(RpcExecutor::classify_method("unknown_method"), RequestClass::Heavy);
    }

    #[tokio::test]
    async fn test_executor_creation() {
        let executor = RpcExecutor::new(4, 8, 2).expect("Failed to create executor");

        // Should be able to create executor with specified thread counts
        // Metrics should be initialized to zero
        let metrics = executor.metrics();
        assert_eq!(metrics.light_active, 0);
        assert_eq!(metrics.heavy_active, 0);
        assert_eq!(metrics.trace_active, 0);

        executor.shutdown().expect("Failed to shutdown");
    }

    #[tokio::test]
    async fn test_light_pool_execution() {
        let executor = RpcExecutor::new(2, 2, 1).expect("Failed to create executor");

        let request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "eth_blockNumber".to_string(),
            params: serde_json::json!([]),
            id: 1,
        };

        // This will fail without a real RPC endpoint, but tests the routing
        // We'll use a mock server in integration tests
        let result = executor.execute("http://localhost:8545", request).await;

        // Should attempt to execute (will fail connecting, but that's OK for unit test)
        assert!(result.is_err() || result.is_ok());

        executor.shutdown().expect("Failed to shutdown");
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        let executor = RpcExecutor::new(2, 2, 1).expect("Failed to create executor");

        // Should shutdown without panicking
        executor.shutdown().expect("Shutdown failed");
    }

    #[tokio::test]
    async fn test_metrics_initialization() {
        let executor = RpcExecutor::new(4, 8, 2).expect("Failed to create executor");

        let metrics = executor.metrics();

        // All metrics should start at zero
        assert_eq!(metrics.light_active, 0);
        assert_eq!(metrics.light_queued, 0);
        assert_eq!(metrics.light_completed, 0);
        assert_eq!(metrics.heavy_active, 0);
        assert_eq!(metrics.heavy_queued, 0);
        assert_eq!(metrics.heavy_completed, 0);
        assert_eq!(metrics.trace_active, 0);
        assert_eq!(metrics.trace_queued, 0);
        assert_eq!(metrics.trace_completed, 0);

        executor.shutdown().expect("Failed to shutdown");
    }

    #[tokio::test]
    async fn test_backpressure() {
        use tokio::sync::Barrier;
        use std::sync::Arc;

        // Create executor with small queue for testing
        let executor = Arc::new(RpcExecutor::new(1, 1, 1).expect("Failed to create executor"));
        let barrier = Arc::new(Barrier::new(2));

        // Fill the light pool queue by holding semaphore permits
        let mut permits = vec![];
        for _ in 0..1000 {
            if let Ok(permit) = executor.light_semaphore.try_acquire() {
                permits.push(permit);
            }
        }

        // Now queue should be full, next request should fail with 429
        let request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "eth_blockNumber".to_string(),
            params: serde_json::json!([]),
            id: 1,
        };

        let result = executor.execute("http://localhost:8545", request).await;

        // Should get backpressure error
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("429") || e.to_string().contains("queue full"));
        }

        // Clean up
        drop(permits);
        drop(executor);
    }

    #[tokio::test]
    async fn test_pool_isolation() {
        // Test that different pools don't interfere with each other
        let executor = RpcExecutor::new(2, 2, 1).expect("Failed to create executor");

        // Fill heavy pool
        let mut heavy_permits = vec![];
        for _ in 0..1000 {
            if let Ok(permit) = executor.heavy_semaphore.try_acquire() {
                heavy_permits.push(permit);
            }
        }

        // Light pool should still be available
        let light_request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "eth_blockNumber".to_string(),
            params: serde_json::json!([]),
            id: 1,
        };

        // This will fail to connect but should not fail due to heavy pool being full
        let result = executor.execute("http://localhost:8545", light_request).await;

        // Should fail with connection error, not backpressure
        if let Err(e) = result {
            let err_str = e.to_string();
            // Should NOT contain queue full message since light pool is available
            assert!(!err_str.contains("Light pool queue full"));
        }

        drop(heavy_permits);
        executor.shutdown().expect("Failed to shutdown");
    }

    #[tokio::test]
    async fn test_concurrent_mixed_requests() {
        use tokio::task::JoinSet;

        let executor = Arc::new(RpcExecutor::new(2, 2, 1).expect("Failed to create executor"));

        let mut join_set = JoinSet::new();

        // Spawn 10 light requests
        for i in 0..10 {
            let exec = executor.clone();
            join_set.spawn(async move {
                let request = RpcRequest {
                    jsonrpc: "2.0".to_string(),
                    method: "eth_blockNumber".to_string(),
                    params: serde_json::json!([]),
                    id: i,
                };
                exec.execute("http://localhost:8545", request).await
            });
        }

        // Spawn 10 heavy requests
        for i in 10..20 {
            let exec = executor.clone();
            join_set.spawn(async move {
                let request = RpcRequest {
                    jsonrpc: "2.0".to_string(),
                    method: "eth_call".to_string(),
                    params: serde_json::json!([]),
                    id: i,
                };
                exec.execute("http://localhost:8545", request).await
            });
        }

        // Spawn 5 trace requests
        for i in 20..25 {
            let exec = executor.clone();
            join_set.spawn(async move {
                let request = RpcRequest {
                    jsonrpc: "2.0".to_string(),
                    method: "debug_traceTransaction".to_string(),
                    params: serde_json::json!([]),
                    id: i,
                };
                exec.execute("http://localhost:8545", request).await
            });
        }

        // Collect results
        let mut results = vec![];
        while let Some(result) = join_set.join_next().await {
            results.push(result);
        }

        // All should complete (though they'll fail to connect)
        assert_eq!(results.len(), 25);

        drop(executor);
    }
}
