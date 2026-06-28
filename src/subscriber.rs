use std::collections::HashMap;

use rifts::message::SubscribeMode;

/// Tracks which topics the client is subscribed to so they can be
/// re-sent after a reconnect.
#[derive(Debug)]
pub struct SubscriptionTracker {
    topics: HashMap<String, SubscribeMode>,
}

impl SubscriptionTracker {
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
        }
    }

    /// Record a new subscription (or update the mode of an existing one).
    pub fn add(&mut self, topic: &str, mode: SubscribeMode) {
        self.topics.insert(topic.to_string(), mode);
    }

    /// Remove a subscription. Returns `true` if it existed.
    pub fn remove(&mut self, topic: &str) -> bool {
        self.topics.remove(topic).is_some()
    }

    /// Returns the mode for a topic, if subscribed.
    #[allow(dead_code)]
    pub fn get(&self, topic: &str) -> Option<SubscribeMode> {
        self.topics.get(topic).copied()
    }

    /// Iterate all tracked subscriptions.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &SubscribeMode)> {
        self.topics.iter()
    }

    /// Returns the number of tracked subscriptions.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.topics.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.topics.is_empty()
    }
}

impl Default for SubscriptionTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_get() {
        let mut t = SubscriptionTracker::new();
        t.add("room/1", SubscribeMode::Live);
        assert_eq!(t.get("room/1"), Some(SubscribeMode::Live));
        assert_eq!(t.get("room/2"), None);
    }

    #[test]
    fn remove() {
        let mut t = SubscriptionTracker::new();
        t.add("a", SubscribeMode::Replay);
        assert!(t.remove("a"));
        assert!(!t.remove("a"));
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn update_mode() {
        let mut t = SubscriptionTracker::new();
        t.add("x", SubscribeMode::Live);
        t.add("x", SubscribeMode::Replay);
        assert_eq!(t.get("x"), Some(SubscribeMode::Replay));
    }

    #[test]
    fn iter_yields_all() {
        let mut t = SubscriptionTracker::new();
        t.add("a", SubscribeMode::Live);
        t.add("b", SubscribeMode::Ephemeral);
        let names: Vec<&str> = t.iter().map(|(k, _)| k.as_str()).collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
    }
}
