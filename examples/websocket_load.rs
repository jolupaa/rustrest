use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

const ESTABLISHMENT_DEADLINE: Duration = Duration::from_secs(120);
const CLIENT_IO_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);
const ACTIVE_CADENCE: Duration = Duration::from_millis(10);
const MAX_LATENCY_SAMPLES: usize = 1_000_000;

#[derive(Debug, Clone)]
struct LoadConfig {
    url: String,
    idle: usize,
    active: usize,
    duration: Duration,
    message_bytes: usize,
    connect_concurrency: usize,
    json_out: PathBuf,
}

impl Default for LoadConfig {
    fn default() -> Self {
        Self {
            url: "ws://127.0.0.1:3000/load".into(),
            idle: 10_000,
            active: 1_000,
            duration: Duration::from_secs(900),
            message_bytes: 256,
            connect_concurrency: 256,
            json_out: PathBuf::from("target/ws-load-report.json"),
        }
    }
}

impl LoadConfig {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Option<Self>, String> {
        let mut config = Self::default();
        let mut args = args.into_iter();
        while let Some(argument) = args.next() {
            let value = |args: &mut dyn Iterator<Item = String>| {
                args.next()
                    .ok_or_else(|| format!("Falta el valor de {argument}"))
            };
            match argument.as_str() {
                "--url" => config.url = value(&mut args)?,
                "--idle" => config.idle = parse_number(&argument, value(&mut args)?)?,
                "--active" => config.active = parse_number(&argument, value(&mut args)?)?,
                "--duration-secs" => {
                    config.duration =
                        Duration::from_secs(parse_number(&argument, value(&mut args)?)?)
                }
                "--message-bytes" => {
                    config.message_bytes = parse_number(&argument, value(&mut args)?)?
                }
                "--connect-concurrency" => {
                    config.connect_concurrency = parse_number(&argument, value(&mut args)?)?
                }
                "--json-out" => config.json_out = PathBuf::from(value(&mut args)?),
                "-h" | "--help" => return Ok(None),
                _ => return Err(format!("Opcion desconocida: {argument}")),
            }
        }

        if config.idle + config.active == 0 {
            return Err("Debe solicitar al menos una conexion".into());
        }
        if config.connect_concurrency == 0 {
            return Err("--connect-concurrency debe ser mayor que cero".into());
        }
        if config.duration.is_zero() {
            return Err("--duration-secs debe ser mayor que cero".into());
        }
        if !(32..=1024 * 1024).contains(&config.message_bytes) {
            return Err("--message-bytes debe estar entre 32 y 1048576".into());
        }
        Ok(Some(config))
    }
}

fn parse_number<T>(option: &str, value: String) -> Result<T, String>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| format!("Valor invalido para {option}: {value}"))
}

#[derive(Debug, Serialize)]
struct LoadReport {
    requested_idle: usize,
    requested_active: usize,
    connected_idle: usize,
    connected_active: usize,
    connect_failures: usize,
    sent_messages: u64,
    received_messages: u64,
    send_failures: u64,
    receive_failures: u64,
    unexpected_closes: u64,
    p50_round_trip_micros: u64,
    p95_round_trip_micros: u64,
    p99_round_trip_micros: u64,
    elapsed_millis: u128,
}

#[derive(Default)]
struct Metrics {
    connected_idle: AtomicUsize,
    connected_active: AtomicUsize,
    connect_failures: AtomicUsize,
    sent_messages: AtomicU64,
    received_messages: AtomicU64,
    send_failures: AtomicU64,
    receive_failures: AtomicU64,
    unexpected_closes: AtomicU64,
    latencies: Mutex<LatencyReservoir>,
}

#[derive(Default)]
struct LatencyReservoir {
    samples: Vec<u64>,
    seen: u64,
}

impl LatencyReservoir {
    fn record(&mut self, micros: u64) {
        self.seen += 1;
        if self.samples.len() < MAX_LATENCY_SAMPLES {
            self.samples.push(micros);
            return;
        }

        let every = (self.seen / MAX_LATENCY_SAMPLES as u64).max(2);
        if self.seen % every == 0 {
            let index = ((self.seen / every) as usize) % MAX_LATENCY_SAMPLES;
            self.samples[index] = micros;
        }
    }

    fn percentiles(&self) -> (u64, u64, u64) {
        let mut samples = self.samples.clone();
        samples.sort_unstable();
        (
            percentile(&samples, 50),
            percentile(&samples, 95),
            percentile(&samples, 99),
        )
    }
}

fn percentile(samples: &[u64], percent: usize) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    samples[(samples.len() - 1) * percent / 100]
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RunState {
    Connecting,
    Running,
    Shutdown,
}

#[derive(Clone, Copy)]
enum ClientKind {
    Idle,
    Active,
}

#[tokio::main]
async fn main() {
    let config = match LoadConfig::parse(env::args().skip(1)) {
        Ok(Some(config)) => config,
        Ok(None) => {
            print_help();
            return;
        }
        Err(error) => {
            eprintln!("{error}\n");
            print_help();
            std::process::exit(2);
        }
    };

    match run(config).await {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(error) => {
            eprintln!("Error de carga WebSocket: {error}");
            std::process::exit(2);
        }
    }
}

async fn run(config: LoadConfig) -> Result<bool, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let metrics = Arc::new(Metrics::default());
    let semaphore = Arc::new(Semaphore::new(config.connect_concurrency));
    let (state_tx, state_rx) = watch::channel(RunState::Connecting);
    let (ready_tx, mut ready_rx) = mpsc::channel(config.connect_concurrency.max(1));
    let mut clients = JoinSet::new();

    for client_id in 0..config.idle + config.active {
        let kind = if client_id < config.idle {
            ClientKind::Idle
        } else {
            ClientKind::Active
        };
        let url = config.url.clone();
        let semaphore = semaphore.clone();
        let ready_tx = ready_tx.clone();
        let state_rx = state_rx.clone();
        let metrics = metrics.clone();
        let message_bytes = config.message_bytes;
        clients.spawn(async move {
            run_client(
                client_id,
                kind,
                &url,
                message_bytes,
                semaphore,
                ready_tx,
                state_rx,
                metrics,
            )
            .await;
        });
    }
    drop(ready_tx);

    let requested = config.idle + config.active;
    let ready_before_deadline = tokio::time::timeout(ESTABLISHMENT_DEADLINE, async {
        for _ in 0..requested {
            if ready_rx.recv().await.is_none() {
                return false;
            }
        }
        true
    })
    .await
    .unwrap_or(false);

    let all_connected = ready_before_deadline
        && metrics.connected_idle.load(Ordering::Relaxed) == config.idle
        && metrics.connected_active.load(Ordering::Relaxed) == config.active;

    if all_connected {
        println!(
            "Conectados {} idle y {} activos; iniciando {} segundos de carga.",
            config.idle,
            config.active,
            config.duration.as_secs()
        );
        state_tx.send_replace(RunState::Running);
        tokio::time::sleep(config.duration).await;
    } else {
        eprintln!(
            "No se establecieron todas las conexiones antes de {} segundos.",
            ESTABLISHMENT_DEADLINE.as_secs()
        );
    }

    state_tx.send_replace(RunState::Shutdown);
    let drained = tokio::time::timeout(SHUTDOWN_DEADLINE, async {
        while clients.join_next().await.is_some() {}
    })
    .await
    .is_ok();
    if !drained {
        clients.abort_all();
        while clients.join_next().await.is_some() {}
    }

    let report = build_report(&config, &metrics, started.elapsed());
    if let Some(parent) = config.json_out.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(&config.json_out, serde_json::to_vec_pretty(&report)?).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);

    let success = all_connected
        && drained
        && report.connect_failures == 0
        && report.send_failures == 0
        && report.receive_failures == 0
        && report.unexpected_closes == 0
        && report.received_messages == report.sent_messages;
    Ok(success)
}

#[allow(clippy::too_many_arguments)]
async fn run_client(
    client_id: usize,
    kind: ClientKind,
    url: &str,
    message_bytes: usize,
    semaphore: Arc<Semaphore>,
    ready_tx: mpsc::Sender<()>,
    mut state_rx: watch::Receiver<RunState>,
    metrics: Arc<Metrics>,
) {
    let permit = match semaphore.acquire_owned().await {
        Ok(permit) => permit,
        Err(_) => return,
    };
    let connected =
        tokio::time::timeout(CLIENT_IO_TIMEOUT, tokio_tungstenite::connect_async(url)).await;
    drop(permit);

    let mut socket = match connected {
        Ok(Ok((socket, _response))) => socket,
        _ => {
            metrics.connect_failures.fetch_add(1, Ordering::Relaxed);
            let _ = ready_tx.send(()).await;
            return;
        }
    };

    match kind {
        ClientKind::Idle => metrics.connected_idle.fetch_add(1, Ordering::Relaxed),
        ClientKind::Active => metrics.connected_active.fetch_add(1, Ordering::Relaxed),
    };
    let _ = ready_tx.send(()).await;

    while *state_rx.borrow() == RunState::Connecting {
        if state_rx.changed().await.is_err() {
            return;
        }
    }

    if *state_rx.borrow() == RunState::Running {
        match kind {
            ClientKind::Idle => run_idle_client(&mut socket, &mut state_rx, &metrics).await,
            ClientKind::Active => {
                run_active_client(
                    client_id,
                    message_bytes,
                    &mut socket,
                    &mut state_rx,
                    &metrics,
                )
                .await
            }
        }
    }

    close_client(&mut socket).await;
}

async fn run_idle_client<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    state_rx: &mut watch::Receiver<RunState>,
    metrics: &Metrics,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            changed = state_rx.changed() => {
                if changed.is_err() || *state_rx.borrow() == RunState::Shutdown {
                    return;
                }
            }
            message = socket.next() => match message {
                Some(Ok(Message::Close(_))) | None => {
                    metrics.unexpected_closes.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Some(Err(_)) => {
                    metrics.receive_failures.fetch_add(1, Ordering::Relaxed);
                    metrics.unexpected_closes.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                _ => {}
            }
        }
    }
}

async fn run_active_client<S>(
    client_id: usize,
    message_bytes: usize,
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    state_rx: &mut watch::Receiver<RunState>,
    metrics: &Metrics,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut sequence = 0_u64;
    while *state_rx.borrow() == RunState::Running {
        let payload = fixed_payload(client_id, sequence, message_bytes);
        sequence += 1;
        let started = Instant::now();
        if socket
            .send(Message::Text(payload.clone().into()))
            .await
            .is_err()
        {
            metrics.send_failures.fetch_add(1, Ordering::Relaxed);
            return;
        }
        metrics.sent_messages.fetch_add(1, Ordering::Relaxed);

        match receive_echo(socket, &payload).await {
            EchoResult::Received => {
                metrics.received_messages.fetch_add(1, Ordering::Relaxed);
                metrics
                    .latencies
                    .lock()
                    .unwrap()
                    .record(started.elapsed().as_micros().min(u64::MAX as u128) as u64);
            }
            EchoResult::Mismatch => {
                metrics.receive_failures.fetch_add(1, Ordering::Relaxed);
                return;
            }
            EchoResult::Closed => {
                metrics.unexpected_closes.fetch_add(1, Ordering::Relaxed);
                return;
            }
            EchoResult::Error => {
                metrics.receive_failures.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(ACTIVE_CADENCE) => {}
            changed = state_rx.changed() => {
                if changed.is_err() || *state_rx.borrow() == RunState::Shutdown {
                    return;
                }
            }
        }
    }
}

enum EchoResult {
    Received,
    Mismatch,
    Closed,
    Error,
}

async fn receive_echo<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    expected: &str,
) -> EchoResult
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let receive = async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(value))) => {
                    return if value.as_str() == expected {
                        EchoResult::Received
                    } else {
                        EchoResult::Mismatch
                    };
                }
                Some(Ok(Message::Close(_))) | None => return EchoResult::Closed,
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                Some(Ok(_)) => return EchoResult::Mismatch,
                Some(Err(_)) => return EchoResult::Error,
            }
        }
    };
    tokio::time::timeout(CLIENT_IO_TIMEOUT, receive)
        .await
        .unwrap_or(EchoResult::Error)
}

async fn close_client<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let close = CloseFrame {
        code: CloseCode::Normal,
        reason: "fin de la prueba".into(),
    };
    if socket.send(Message::Close(Some(close))).await.is_err() {
        return;
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(message) = socket.next().await {
            if matches!(message, Ok(Message::Close(_)) | Err(_)) {
                break;
            }
        }
    })
    .await;
}

fn fixed_payload(client_id: usize, sequence: u64, bytes: usize) -> String {
    let mut payload = format!("{client_id:012x}:{sequence:016x}:");
    payload.extend(std::iter::repeat_n('x', bytes - payload.len()));
    payload
}

fn build_report(config: &LoadConfig, metrics: &Metrics, elapsed: Duration) -> LoadReport {
    let (p50, p95, p99) = metrics.latencies.lock().unwrap().percentiles();
    LoadReport {
        requested_idle: config.idle,
        requested_active: config.active,
        connected_idle: metrics.connected_idle.load(Ordering::Relaxed),
        connected_active: metrics.connected_active.load(Ordering::Relaxed),
        connect_failures: metrics.connect_failures.load(Ordering::Relaxed),
        sent_messages: metrics.sent_messages.load(Ordering::Relaxed),
        received_messages: metrics.received_messages.load(Ordering::Relaxed),
        send_failures: metrics.send_failures.load(Ordering::Relaxed),
        receive_failures: metrics.receive_failures.load(Ordering::Relaxed),
        unexpected_closes: metrics.unexpected_closes.load(Ordering::Relaxed),
        p50_round_trip_micros: p50,
        p95_round_trip_micros: p95,
        p99_round_trip_micros: p99,
        elapsed_millis: elapsed.as_millis(),
    }
}

fn print_help() {
    println!(
        "Uso: cargo run --release --example websocket_load -- [opciones]\n\
         \n\
         --url URL                     ws://127.0.0.1:3000/load\n\
         --idle N                      10000\n\
         --active N                    1000\n\
         --duration-secs N             900\n\
         --message-bytes N             256\n\
         --connect-concurrency N       256\n\
         --json-out RUTA               target/ws-load-report.json"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_config_parses_all_options() {
        let config = LoadConfig::parse(
            [
                "--url",
                "ws://localhost:4000/ws",
                "--idle",
                "4",
                "--active",
                "2",
                "--duration-secs",
                "3",
                "--message-bytes",
                "64",
                "--connect-concurrency",
                "8",
                "--json-out",
                "target/test.json",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap()
        .unwrap();

        assert_eq!(config.url, "ws://localhost:4000/ws");
        assert_eq!(config.idle, 4);
        assert_eq!(config.active, 2);
        assert_eq!(config.duration, Duration::from_secs(3));
        assert_eq!(config.message_bytes, 64);
        assert_eq!(config.connect_concurrency, 8);
        assert_eq!(config.json_out, PathBuf::from("target/test.json"));
    }

    #[test]
    fn latency_reservoir_is_bounded_and_reports_percentiles() {
        let mut reservoir = LatencyReservoir::default();
        for value in 1..=MAX_LATENCY_SAMPLES as u64 + 100 {
            reservoir.record(value);
        }

        assert_eq!(reservoir.samples.len(), MAX_LATENCY_SAMPLES);
        let (p50, p95, p99) = reservoir.percentiles();
        assert!(p50 > 0);
        assert!(p50 <= p95 && p95 <= p99);
    }

    #[test]
    fn fixed_payload_has_exact_size_and_identity() {
        let first = fixed_payload(1, 2, 256);
        let second = fixed_payload(2, 2, 256);

        assert_eq!(first.len(), 256);
        assert_ne!(first, second);
    }
}
