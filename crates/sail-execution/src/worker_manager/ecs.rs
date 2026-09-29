use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use aws_sdk_ecs::types::{
    AssignPublicIp, AwsVpcConfiguration, ContainerOverride, KeyValuePair, LaunchType,
    NetworkConfiguration, TaskOverride,
};
use fastrace::collector::SpanContext;
use futures::future::BoxFuture;
use sail_common::actor::ActorSystem;
use sail_common::config::{ClusterConfigEnv, ExecutionConfigEnv};
use sail_common::telemetry::ContextPropagationEnv;
use sail_common::utils::retry::RetryStrategy;
use tokio::sync::OnceCell;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::WorkerId;
use crate::shuffle::ShuffleBackendKind;
use crate::worker_manager::{WorkerLaunchOptions, WorkerManager};

#[derive(Debug, Clone)]
pub struct EcsWorkerManagerOptions {
    pub cluster: String,
    pub subnets: Vec<String>,
    pub security_groups: Vec<String>,
    pub task_definition: String,
    pub container_name: String,
    pub assign_public_ip: bool,
    pub launch_type: String,
}

struct EcsWorkerService {
    options: EcsWorkerManagerOptions,
    client: OnceCell<aws_sdk_ecs::Client>,
    launched_task_arns: tokio::sync::Mutex<Vec<String>>,
}

pub struct EcsWorkerManager {
    service: Arc<EcsWorkerService>,
}

impl EcsWorkerManager {
    pub fn new(options: EcsWorkerManagerOptions) -> Self {
        Self {
            service: Arc::new(EcsWorkerService {
                options,
                client: OnceCell::new(),
                launched_task_arns: tokio::sync::Mutex::new(Vec::new()),
            }),
        }
    }
}

impl EcsWorkerService {
    async fn client(&self) -> &aws_sdk_ecs::Client {
        self.client
            .get_or_init(|| async {
                let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                aws_sdk_ecs::Client::new(&config)
            })
            .await
    }

    fn build_env(&self, id: WorkerId, options: WorkerLaunchOptions) -> Vec<KeyValuePair> {
        let WorkerLaunchOptions {
            enable_tls,
            batch_size,
            driver_id,
            session_id,
            driver_external_host,
            driver_external_port,
            worker_heartbeat_interval,
            task_stream_buffer,
            task_stream_creation_timeout,
            rpc_retry_strategy,
            shuffle_backend,
        } = options;

        let w3c_traceparent =
            SpanContext::current_local_parent().map(|x| x.encode_w3c_traceparent());

        let rpc_retry_strategy = match rpc_retry_strategy {
            RetryStrategy::ExponentialBackoff {
                max_count,
                initial_delay,
                max_delay,
                factor,
            } => {
                format!(
                    "{{ exponential_backoff = {{ max_count = {}, initial_delay_secs = {}, max_delay_secs = {}, factor = {} }} }}",
                    max_count,
                    initial_delay.as_secs(),
                    max_delay.as_secs(),
                    factor,
                )
            }
            RetryStrategy::Fixed { max_count, delay } => {
                format!(
                    "{{ fixed = {{ max_count = {}, delay_secs = {} }} }}",
                    max_count,
                    delay.as_secs()
                )
            }
        };

        let kv = |name: &str, value: String| {
            KeyValuePair::builder()
                .name(name)
                .value(value)
                .build()
        };

        let mut env = vec![
            kv("RUST_LOG", env::var("RUST_LOG").unwrap_or("info".to_string())),
            kv(ClusterConfigEnv::ENABLE_TLS, enable_tls.to_string()),
            kv(ExecutionConfigEnv::BATCH_SIZE, batch_size.to_string()),
            kv(ClusterConfigEnv::DRIVER_EXTERNAL_HOST, driver_external_host),
            kv(
                ClusterConfigEnv::DRIVER_EXTERNAL_PORT,
                driver_external_port.to_string(),
            ),
            kv(
                ClusterConfigEnv::DRIVER_ID,
                u64::from(driver_id).to_string(),
            ),
            kv(ClusterConfigEnv::SESSION_ID, session_id),
            kv(ClusterConfigEnv::WORKER_ID, u64::from(id).to_string()),
            kv(
                ClusterConfigEnv::WORKER_LISTEN_HOST,
                "0.0.0.0".to_string(),
            ),
            kv(
                ClusterConfigEnv::WORKER_HEARTBEAT_INTERVAL_SECS,
                worker_heartbeat_interval.as_secs().to_string(),
            ),
            kv(
                ClusterConfigEnv::TASK_STREAM_BUFFER,
                task_stream_buffer.to_string(),
            ),
            kv(
                ClusterConfigEnv::TASK_STREAM_CREATION_TIMEOUT_SECS,
                task_stream_creation_timeout.as_secs().to_string(),
            ),
            kv(ClusterConfigEnv::RPC_RETRY_STRATEGY, rpc_retry_strategy),
            kv(
                ClusterConfigEnv::SHUFFLE_BACKEND__TYPE,
                match &shuffle_backend {
                    ShuffleBackendKind::Flight { .. } => "flight",
                    ShuffleBackendKind::Storage { .. } => "storage",
                    ShuffleBackendKind::Celeborn { .. } => "celeborn",
                }
                .to_string(),
            ),
        ];

        if let ShuffleBackendKind::Flight { compression } = &shuffle_backend {
            env.push(kv(
                ClusterConfigEnv::SHUFFLE_BACKEND__FLIGHT__COMPRESSION,
                compression.to_string(),
            ));
        }
        if let ShuffleBackendKind::Storage {
            path,
            max_file_size,
            compression,
        } = &shuffle_backend
        {
            if let Some(path) = path {
                env.push(kv(
                    ClusterConfigEnv::SHUFFLE_BACKEND__STORAGE__PATH,
                    path.clone(),
                ));
            }
            env.push(kv(
                ClusterConfigEnv::SHUFFLE_BACKEND__STORAGE__MAX_FILE_SIZE,
                max_file_size.to_string(),
            ));
            env.push(kv(
                ClusterConfigEnv::SHUFFLE_BACKEND__STORAGE__COMPRESSION,
                compression.to_string(),
            ));
        }
        if let ShuffleBackendKind::Celeborn {
            compression,
            heartbeat_interval_secs,
            partition_split_threshold,
            partition_split_mode,
            ..
        } = &shuffle_backend
        {
            env.extend([
                kv(
                    ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__MASTER_ENDPOINTS,
                    shuffle_backend.celeborn_master_endpoints_string(),
                ),
                kv(
                    ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__COMPRESSION,
                    compression.to_string(),
                ),
                kv(
                    ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__HEARTBEAT_INTERVAL_SECS,
                    heartbeat_interval_secs.to_string(),
                ),
                kv(
                    ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__ENDPOINT_OVERRIDES,
                    shuffle_backend.celeborn_endpoint_overrides_string(),
                ),
                kv(
                    ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__PARTITION_SPLIT_THRESHOLD,
                    partition_split_threshold.to_string(),
                ),
                kv(
                    ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__PARTITION_SPLIT_MODE,
                    partition_split_mode.to_string(),
                ),
            ]);
        }
        if let Some(traceparent) = w3c_traceparent {
            env.push(kv(ContextPropagationEnv::TRACEPARENT, traceparent));
        }
        env
    }

    async fn launch_worker(
        &self,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> ExecutionResult<()> {
        let client = self.client().await;
        let env_vars = self.build_env(id, options);

        let assign_public_ip = if self.options.assign_public_ip {
            AssignPublicIp::Enabled
        } else {
            AssignPublicIp::Disabled
        };

        let launch_type = match self.options.launch_type.to_uppercase().as_str() {
            "EC2" => LaunchType::Ec2,
            _ => LaunchType::Fargate,
        };

        let mut network_config = AwsVpcConfiguration::builder()
            .set_subnets(Some(self.options.subnets.clone()))
            .assign_public_ip(assign_public_ip);
        if !self.options.security_groups.is_empty() {
            network_config =
                network_config.set_security_groups(Some(self.options.security_groups.clone()));
        }

        let container_override = ContainerOverride::builder()
            .name(&self.options.container_name)
            .command("sail")
            .command("worker")
            .set_environment(Some(env_vars))
            .build();

        let task_override = TaskOverride::builder()
            .container_overrides(container_override)
            .build();

        let result = client
            .run_task()
            .cluster(&self.options.cluster)
            .task_definition(&self.options.task_definition)
            .launch_type(launch_type)
            .network_configuration(
                NetworkConfiguration::builder()
                    .awsvpc_configuration(network_config.build().map_err(|e| {
                        ExecutionError::InternalError(format!(
                            "failed to build awsvpc configuration: {e}"
                        ))
                    })?)
                    .build(),
            )
            .overrides(task_override)
            .set_tags(Some(vec![
                aws_sdk_ecs::types::Tag::builder()
                    .key("sail.worker-id")
                    .value(u64::from(id).to_string())
                    .build(),
                aws_sdk_ecs::types::Tag::builder()
                    .key("sail.component")
                    .value("worker")
                    .build(),
            ]))
            .send()
            .await
            .map_err(|e| {
                ExecutionError::InternalError(format!("failed to run ECS task: {e}"))
            })?;

        if let Some(failures) = &result.failures {
            if !failures.is_empty() {
                let reasons: Vec<String> = failures
                    .iter()
                    .map(|f| {
                        format!(
                            "{}: {}",
                            f.arn().unwrap_or("unknown"),
                            f.reason().unwrap_or("unknown")
                        )
                    })
                    .collect();
                return Err(ExecutionError::InternalError(format!(
                    "ECS RunTask failures: {}",
                    reasons.join("; ")
                )));
            }
        }

        if let Some(tasks) = &result.tasks {
            let mut arns = self.launched_task_arns.lock().await;
            for task in tasks {
                if let Some(arn) = task.task_arn() {
                    arns.push(arn.to_string());
                }
            }
        }

        Ok(())
    }

    async fn stop(&self) -> ExecutionResult<()> {
        let client = self.client().await;
        let arns = self.launched_task_arns.lock().await.clone();
        for arn in &arns {
            let _ = client
                .stop_task()
                .cluster(&self.options.cluster)
                .task(arn)
                .reason("driver shutdown")
                .send()
                .await;
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl WorkerManager for EcsWorkerManager {
    fn launch_worker(
        &self,
        _system: &mut ActorSystem,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> BoxFuture<'static, ExecutionResult<()>> {
        let service = self.service.clone();
        Box::pin(async move { service.launch_worker(id, options).await })
    }

    async fn stop(&self) -> ExecutionResult<()> {
        self.service.stop().await
    }
}
