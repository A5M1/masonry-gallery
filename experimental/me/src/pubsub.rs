use std::collections::BTreeMap;
use tokio::sync::broadcast;

pub struct PubSub {
    pub channels: BTreeMap<Vec<u8>, broadcast::Sender<Message>>,
    pub patterns: Vec<(Vec<u8>, broadcast::Sender<Message>)>,
}

#[derive(Clone, Debug)]
pub struct Message {
    pub channel: Vec<u8>,
    pub pattern: Option<Vec<u8>>,
    pub payload: Vec<u8>,
}

impl PubSub {
    pub fn new() -> Self {
        PubSub {
            channels: BTreeMap::new(),
            patterns: Vec::new(),
        }
    }

    pub fn subscribe(&mut self, channel: Vec<u8>) -> broadcast::Receiver<Message> {
        let tx = self.channels
            .entry(channel)
            .or_insert_with(|| broadcast::channel(1024).0);
        tx.subscribe()
    }

    pub fn unsubscribe(&mut self, channel: &[u8]) -> bool {
        self.channels.remove(channel).is_some()
    }

    pub fn psubscribe(&mut self, pattern: Vec<u8>) -> broadcast::Receiver<Message> {
        let tx = broadcast::channel(1024).0;
        let rx = tx.subscribe();
        self.patterns.push((pattern, tx));
        rx
    }

    pub fn punsubscribe(&mut self, pattern: &[u8]) -> bool {
        if let Some(pos) = self.patterns.iter().position(|(p, _)| p == pattern) {
            self.patterns.remove(pos);
            true
        } else {
            false
        }
    }

    pub fn publish(&mut self, channel: &[u8], payload: Vec<u8>) -> usize {
        let msg = Message {
            channel: channel.to_vec(),
            pattern: None,
            payload: payload.clone(),
        };
        let mut count = 0usize;

        if let Some(tx) = self.channels.get(channel) {
            count += tx.receiver_count();
            let _ = tx.send(msg.clone());
        }

        for (pattern, tx) in &self.patterns {
            if crate::cmd_key::wildcard_match(pattern, channel) {
                let mut pattern_msg = msg.clone();
                pattern_msg.pattern = Some(pattern.clone());
                count += tx.receiver_count();
                let _ = tx.send(pattern_msg);
            }
        }

        count
    }

    pub fn num_channels(&self) -> usize {
        self.channels.len()
    }

    pub fn num_patterns(&self) -> usize {
        self.patterns.len()
    }

    pub fn numsub(&self, channels: &[Vec<u8>]) -> Vec<(Vec<u8>, usize)> {
        channels.iter().map(|c| {
            let count = self.channels.get(c).map(|tx| tx.receiver_count()).unwrap_or(0);
            (c.clone(), count)
        }).collect()
    }
}
