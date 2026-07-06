use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub enum QueueMode {
    Fifo,
    Lifo,
}

#[derive(Clone, Copy, Debug)]
pub struct PoolConfig {
    pub max_size: usize,
    pub wait_timeout: Option<Duration>,
    pub create_timeout: Option<Duration>,
    pub recycle_timeout: Option<Duration>,
    pub queue_mode: QueueMode,
}

impl PoolConfig {
    pub fn new() -> Self {
        deadpool::managed::PoolConfig::default().into()
    }
}

impl From<PoolConfig> for deadpool::managed::PoolConfig {
    fn from(value: PoolConfig) -> Self {
        Self {
            max_size: value.max_size,
            timeouts: deadpool::managed::Timeouts {
                wait: value.wait_timeout,
                create: value.create_timeout,
                recycle: value.recycle_timeout,
            },
            queue_mode: match value.queue_mode {
                QueueMode::Fifo => deadpool::managed::QueueMode::Fifo,
                QueueMode::Lifo => deadpool::managed::QueueMode::Lifo,
            },
        }
    }
}

impl From<deadpool::managed::PoolConfig> for PoolConfig {
    fn from(value: deadpool::managed::PoolConfig) -> Self {
        Self {
            max_size: value.max_size,
            wait_timeout: value.timeouts.wait,
            create_timeout: value.timeouts.create,
            recycle_timeout: value.timeouts.recycle,
            queue_mode: match value.queue_mode {
                deadpool::managed::QueueMode::Fifo => QueueMode::Fifo,
                deadpool::managed::QueueMode::Lifo => QueueMode::Lifo,
            },
        }
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self::new()
    }
}
