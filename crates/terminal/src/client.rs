use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use insider_common_types::MonoTime;
use insider_ipc::{CommandEnvelope, UnixSocketClient};

const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

pub struct EngineClient {
    client: UnixSocketClient,
    path: PathBuf,
    next_id: u64,
    state_version: u64,
    started: Instant,
    session_id: String,
}

impl EngineClient {
    pub fn connect(path: PathBuf) -> Result<Self, String> {
        let client = UnixSocketClient::new(path.clone(), MAX_PAYLOAD_BYTES)
            .map_err(|error| format!("engine IPC configuration: {error:?}"))?;
        Ok(Self {
            client,
            path,
            next_id: 1,
            state_version: 0,
            started: Instant::now(),
            session_id: format!("{}-{}", std::process::id(), wall_clock_ns()),
        })
    }

    pub fn now(&self) -> MonoTime {
        MonoTime::from_nanos(u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX))
    }

    pub fn next_identity(&mut self) -> u128 {
        let value = self.next_id.max(1);
        self.next_id = self.next_id.saturating_add(1);
        u128::from(value)
    }

    pub fn request(&mut self, payload: Vec<u8>) -> Result<Vec<u8>, String> {
        let result = self.request_with_key(&payload, None);
        drop(payload);
        result
    }

    pub fn request_with_key(
        &mut self,
        payload: &[u8],
        idempotency_key: Option<String>,
    ) -> Result<Vec<u8>, String> {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        let key = idempotency_key
            .unwrap_or_else(|| format!("terminal-{}-idempotency-{id}", self.session_id));
        let mut expected = self.state_version;
        for attempt in 0..4 {
            let command = CommandEnvelope {
                command_id: format!("terminal-{}-command-{id}", self.session_id),
                trace_id: format!("terminal-{}-trace-{id}", self.session_id),
                actor: "terminal".into(),
                issued_wall_ns: wall_clock_ns(),
                expected_state_version: expected,
                idempotency_key: key.clone(),
                payload: payload.to_owned(),
            };
            match self.client.request(&command) {
                Ok(response) => {
                    self.state_version = response.state_version;
                    return Ok(response.payload);
                }
                Err(error) => {
                    let diagnostic = format!("{error:?}");
                    if attempt < 3
                        && diagnostic.contains("StaleState")
                        && let Some(current) = stale_current_version(&diagnostic)
                    {
                        expected = current;
                        self.state_version = current;
                        continue;
                    }
                    return Err(format!("engine IPC request: {diagnostic}"));
                }
            }
        }
        Err("engine state changed continuously; retry later".into())
    }

    pub fn request_background(
        &self,
        primary: Vec<u8>,
        fallback: Option<Vec<u8>>,
    ) -> Result<Receiver<Result<Vec<u8>, String>>, String> {
        let mut client = Self::connect(self.path.clone())?;
        let (sender, receiver) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("insider-terminal-analyst".into())
            .spawn(move || {
                let mut result = client.request(primary);
                if result.is_err()
                    && let Some(fallback) = fallback
                {
                    result = client.request(fallback);
                }
                let _ = sender.send(result);
            })
            .map_err(|error| format!("start analyst worker: {error}"))?;
        Ok(receiver)
    }
}

fn wall_clock_ns() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    )
    .unwrap_or(u64::MAX)
}

fn stale_current_version(diagnostic: &str) -> Option<u64> {
    diagnostic
        .split("current: ")
        .nth(1)?
        .split(|character: char| !character.is_ascii_digit())
        .next()?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use super::{EngineClient, stale_current_version};

    #[test]
    fn parses_stale_state_version() {
        assert_eq!(
            stale_current_version("StaleState { expected: 2, current: 81 }"),
            Some(81)
        );
    }

    #[test]
    fn background_request_reports_transport_failure_without_blocking_caller() {
        let path = PathBuf::from(format!(
            "/tmp/insidertrader-missing-terminal-test-{}.sock",
            std::process::id()
        ));
        let Ok(client) = EngineClient::connect(path) else {
            return;
        };
        let Ok(receiver) = client.request_background(vec![0], None) else {
            return;
        };
        assert!(matches!(
            receiver.recv_timeout(Duration::from_secs(2)),
            Ok(Err(_))
        ));
    }
}
