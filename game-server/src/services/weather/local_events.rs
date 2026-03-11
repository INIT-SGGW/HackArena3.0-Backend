use tokio::sync::broadcast;

/// Local weather stream event kind for sandbox-targeted updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalWeatherEventKind {
    Updated,
    Deactivated,
}

/// Local weather stream event payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalWeatherEvent {
    pub sandbox_id: String,
    pub kind: LocalWeatherEventKind,
}

/// Local weather event hub shared between local admin and weather query services.
#[derive(Clone)]
pub struct LocalWeatherEventHub {
    tx: broadcast::Sender<LocalWeatherEvent>,
}

impl LocalWeatherEventHub {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(128);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<LocalWeatherEvent> {
        self.tx.subscribe()
    }

    pub fn publish(&self, event: LocalWeatherEvent) {
        let _ = self.tx.send(event);
    }
}

impl Default for LocalWeatherEventHub {
    fn default() -> Self {
        Self::new()
    }
}
