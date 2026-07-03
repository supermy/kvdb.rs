use bytes::Bytes;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;

/// 发布到某个通道的事件。
#[derive(Clone, Debug)]
pub struct PublishEvent {
    pub channel: String,
    pub message: Bytes,
}

/// 内存级 Pub/Sub 中心：channel -> [(client_id, sender)]。
#[derive(Default)]
pub struct PubSubHub {
    next_id: AtomicU64,
    subscribers: DashMap<String, Vec<(u64, mpsc::UnboundedSender<PublishEvent>)>>,
}

impl PubSubHub {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn alloc_client_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// 为指定客户端订阅一个或多个 channel，返回实际新订阅的 channel 名称。
    pub fn subscribe(
        &self,
        client_id: u64,
        channels: Vec<String>,
        tx: mpsc::UnboundedSender<PublishEvent>,
    ) -> Vec<String> {
        let mut added = Vec::new();
        for ch in channels {
            let mut entry = self.subscribers.entry(ch.clone()).or_default();
            if entry.iter().any(|(id, _)| *id == client_id) {
                // 已经订阅过该 channel，跳过。
                drop(entry);
                continue;
            }
            entry.push((client_id, tx.clone()));
            drop(entry);
            added.push(ch);
        }
        added
    }

    /// 取消指定客户端对一个或多个 channel 的订阅。
    /// `channels` 为 None 时表示取消该客户端在所有 channel 上的订阅。
    /// 返回被移除的 channel 名称。
    pub fn unsubscribe(&self, client_id: u64, channels: Option<&[String]>) -> Vec<String> {
        let mut removed = Vec::new();
        match channels {
            Some(list) => {
                for ch in list {
                    if let Some(mut entry) = self.subscribers.get_mut(ch) {
                        let before = entry.len();
                        entry.retain(|(id, _)| *id != client_id);
                        if entry.len() < before {
                            removed.push(ch.clone());
                        }
                        if entry.is_empty() {
                            drop(entry);
                            self.subscribers.remove(ch);
                        }
                    }
                }
            }
            None => {
                for mut entry in self.subscribers.iter_mut() {
                    let before = entry.len();
                    entry.retain(|(id, _)| *id != client_id);
                    if entry.len() < before {
                        removed.push(entry.key().clone());
                    }
                }
                // 清理空列表。
                self.subscribers.retain(|_, v| !v.is_empty());
            }
        }
        removed
    }

    /// 向 channel 发布消息，返回成功投递的订阅者数量。
    /// 投递失败（发送端已关闭）的订阅者会被自动清理。
    pub fn publish(&self, channel: &str, message: Bytes) -> usize {
        let event = PublishEvent {
            channel: channel.to_string(),
            message,
        };
        let mut delivered = 0usize;
        let mut dead = Vec::new();
        if let Some(subs) = self.subscribers.get(channel) {
            for (id, tx) in subs.iter() {
                if tx.send(event.clone()).is_ok() {
                    delivered += 1;
                } else {
                    dead.push(*id);
                }
            }
        }
        if !dead.is_empty() {
            if let Some(mut subs) = self.subscribers.get_mut(channel) {
                subs.retain(|(id, _)| !dead.contains(id));
                if subs.is_empty() {
                    drop(subs);
                    self.subscribers.remove(channel);
                }
            }
        }
        delivered
    }

    /// 获取某个客户端在当前 hub 中的总订阅数（仅用于 SUBSCRIBE/UNSUBSCRIBE 响应计数）。
    pub fn subscription_count(&self, client_id: u64) -> usize {
        self.subscribers
            .iter()
            .filter(|entry| entry.iter().any(|(id, _)| *id == client_id))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pubsub_basic() {
        let hub = PubSubHub::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let id = hub.alloc_client_id();

        hub.subscribe(id, vec!["news".to_string()], tx);
        assert_eq!(hub.publish("news", Bytes::from("hello")), 1);

        let evt = rx.recv().await.unwrap();
        assert_eq!(evt.channel, "news");
        assert_eq!(evt.message, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn pubsub_unsubscribe() {
        let hub = PubSubHub::new();
        let (tx, _rx) = mpsc::unbounded_channel();
        let id = hub.alloc_client_id();

        hub.subscribe(id, vec!["a".to_string(), "b".to_string()], tx);
        assert_eq!(hub.subscription_count(id), 2);

        hub.unsubscribe(id, Some(&["a".to_string()]));
        assert_eq!(hub.subscription_count(id), 1);

        hub.unsubscribe(id, None);
        assert_eq!(hub.subscription_count(id), 0);
    }
}
