use std::sync::Arc;
use std::sync::mpsc::{Sender, channel};
use std::thread;
use tokio::sync::oneshot;

type Job = Box<dyn FnOnce() + Send + 'static>;

/// 固定大小线程池，将阻塞型 RocksDB 操作隔离在独立线程，避免阻塞 Tokio I/O 任务。
#[derive(Clone)]
pub struct ThreadPool {
    sender: Arc<Sender<Job>>,
}

impl ThreadPool {
    pub fn new(size: usize) -> Self {
        let (sender, receiver) = channel::<Job>();
        let receiver = std::sync::Arc::new(std::sync::Mutex::new(receiver));
        for _ in 0..size {
            let rx = std::sync::Arc::clone(&receiver);
            thread::spawn(move || {
                while let Ok(job) = rx.lock().unwrap().recv() {
                    job();
                }
            });
        }
        Self {
            sender: Arc::new(sender),
        }
    }

    /// 在线程池中执行任务，返回的 Receiver 可作为 Future 等待结果。
    pub fn spawn<F, R>(&self, f: F) -> oneshot::Receiver<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let job = Box::new(move || {
            let _ = tx.send(f());
        });
        self.sender.send(job).expect("thread pool is closed");
        rx
    }
}
