//! Minimal HTTP load generator for RustRest benchmarks (no external tools).
//!
//! cargo run --release --example http_load -- --url http://127.0.0.1:3000/ \
//!     --mode keepalive|close|h2 --concurrency 64 --duration-secs 10 \
//!     [--warmup-secs 1] [--streams-per-conn 16]
//!
//! keepalive: one HTTP/1.1 connection per worker, reused until the server
//!            answers `connection: close` (then the worker reconnects).
//! close:     a fresh TCP connection per request (`connection: close`).
//! h2:        prior-knowledge HTTP/2; workers multiplex over
//!            ceil(concurrency / streams-per-conn) connections.
//! Latency is client-perceived and includes any (re)connect.

use std::sync::Arc;
use std::time::{Duration, Instant};

use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::client::conn::{http1, http2};
use hyper::header::{CONNECTION, HOST};
use hyper::{Request, Uri};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpStream;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Copy, PartialEq, Debug)]
enum Mode {
    KeepAlive,
    Close,
    H2,
}

struct Config {
    uri: Uri,
    authority: String,
    mode: Mode,
    concurrency: usize,
    duration: Duration,
    warmup: Duration,
    streams_per_conn: usize,
}

#[derive(Default)]
struct Stats {
    latencies_ns: Vec<u64>,
    errors: u64,
    connects: u64,
}

fn parse_args() -> Result<Config, String> {
    let (mut url, mut mode) = ("http://127.0.0.1:3000/".to_string(), Mode::KeepAlive);
    let (mut concurrency, mut duration, mut warmup, mut streams_per_conn) = (64, 10, 1, 16);
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("Falta el valor de {flag}"))?;
        let number = || value.parse::<u64>().map_err(|e| format!("{flag}: {e}"));
        match flag.as_str() {
            "--url" => url = value.clone(),
            "--concurrency" => concurrency = number()? as usize,
            "--duration-secs" => duration = number()?,
            "--warmup-secs" => warmup = number()?,
            "--streams-per-conn" => streams_per_conn = number()?.max(1) as usize,
            "--mode" => {
                mode = match value.as_str() {
                    "keepalive" => Mode::KeepAlive,
                    "close" => Mode::Close,
                    "h2" => Mode::H2,
                    other => return Err(format!("Modo desconocido: {other}")),
                }
            }
            _ => return Err(format!("Opcion desconocida: {flag}")),
        }
    }
    let uri: Uri = url.parse().map_err(|e| format!("--url: {e}"))?;
    let authority = uri
        .authority()
        .ok_or("La URL necesita host:puerto")?
        .to_string();
    Ok(Config {
        uri,
        authority,
        mode,
        concurrency: concurrency.max(1),
        duration: Duration::from_secs(duration),
        warmup: Duration::from_secs(warmup),
        streams_per_conn,
    })
}

fn request(cfg: &Config) -> Request<Empty<Bytes>> {
    let builder = match cfg.mode {
        // HTTP/2 derives :scheme/:authority from an absolute URI.
        Mode::H2 => Request::get(cfg.uri.clone()),
        _ => {
            let path = cfg.uri.path_and_query().map_or("/", |p| p.as_str());
            let builder = Request::get(path).header(HOST, cfg.authority.as_str());
            match cfg.mode {
                Mode::Close => builder.header(CONNECTION, "close"),
                _ => builder,
            }
        }
    };
    builder.body(Empty::new()).expect("valid request")
}

async fn connect(authority: &str) -> Result<TokioIo<TcpStream>, BoxError> {
    let stream = TcpStream::connect(authority).await?;
    stream.set_nodelay(true)?;
    Ok(TokioIo::new(stream))
}

async fn h1_worker(cfg: Arc<Config>, measure_from: Instant, deadline: Instant) -> Stats {
    let mut stats = Stats::default();
    let mut sender: Option<http1::SendRequest<Empty<Bytes>>> = None;
    loop {
        let started = Instant::now();
        if started >= deadline {
            return stats;
        }
        let outcome: Result<bool, BoxError> = async {
            let mut conn = match sender.take() {
                Some(conn) if !conn.is_closed() => conn,
                _ => {
                    stats.connects += 1;
                    let (conn, driver) = http1::handshake(connect(&cfg.authority).await?).await?;
                    tokio::spawn(driver);
                    conn
                }
            };
            conn.ready().await?;
            let response = conn.send_request(request(&cfg)).await?;
            let server_closes = response
                .headers()
                .get(CONNECTION)
                .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"close"));
            let ok = response.status().is_success();
            response.into_body().collect().await?;
            if cfg.mode == Mode::KeepAlive && !server_closes {
                sender = Some(conn);
            }
            Ok(ok)
        }
        .await;
        record(&mut stats, started, measure_from, outcome).await;
    }
}

async fn h2_worker(
    cfg: Arc<Config>,
    conn: http2::SendRequest<Empty<Bytes>>,
    measure_from: Instant,
    deadline: Instant,
) -> Stats {
    let mut stats = Stats::default();
    loop {
        let started = Instant::now();
        if started >= deadline {
            return stats;
        }
        let outcome: Result<bool, BoxError> = async {
            let mut conn = conn.clone();
            conn.ready().await?;
            let response = conn.send_request(request(&cfg)).await?;
            let ok = response.status().is_success();
            response.into_body().collect().await?;
            Ok(ok)
        }
        .await;
        record(&mut stats, started, measure_from, outcome).await;
    }
}

async fn record(
    stats: &mut Stats,
    started: Instant,
    from: Instant,
    outcome: Result<bool, BoxError>,
) {
    let ok = matches!(outcome, Ok(true));
    if started >= from {
        if ok {
            stats.latencies_ns.push(started.elapsed().as_nanos() as u64);
        } else {
            stats.errors += 1;
        }
    }
    if !ok {
        // Avoid a hot error loop (e.g. connection refused) skewing the run.
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::main]
async fn main() {
    let cfg = match parse_args() {
        Ok(cfg) => Arc::new(cfg),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    let mut senders = Vec::new();
    if cfg.mode == Mode::H2 {
        for _ in 0..cfg.concurrency.div_ceil(cfg.streams_per_conn) {
            let io = connect(&cfg.authority).await.expect("conexion h2");
            let (conn, driver) = http2::handshake(TokioExecutor::new(), io)
                .await
                .expect("h2");
            tokio::spawn(driver);
            senders.push(conn);
        }
    }
    let measure_from = Instant::now() + cfg.warmup;
    let deadline = measure_from + cfg.duration;
    let mut tasks = tokio::task::JoinSet::new();
    for worker in 0..cfg.concurrency {
        let cfg = Arc::clone(&cfg);
        match cfg.mode {
            Mode::H2 => {
                let conn = senders[worker % senders.len()].clone();
                tasks.spawn(h2_worker(cfg, conn, measure_from, deadline))
            }
            _ => tasks.spawn(h1_worker(cfg, measure_from, deadline)),
        };
    }
    let mut all = Stats {
        connects: senders.len() as u64,
        ..Stats::default()
    };
    while let Some(stats) = tasks.join_next().await {
        let stats = stats.expect("worker");
        all.latencies_ns.extend(stats.latencies_ns);
        all.errors += stats.errors;
        all.connects += stats.connects;
    }
    all.latencies_ns.sort_unstable();
    let pct = |p: f64| match all.latencies_ns.len() {
        0 => 0.0,
        n => all.latencies_ns[((n - 1) as f64 * p).round() as usize] as f64 / 1000.0,
    };
    println!(
        "mode={:?} c={} secs={} requests={} rps={:.0} p50_us={:.1} p90_us={:.1} p99_us={:.1} p999_us={:.1} errors={} connects={}",
        cfg.mode,
        cfg.concurrency,
        cfg.duration.as_secs(),
        all.latencies_ns.len(),
        all.latencies_ns.len() as f64 / cfg.duration.as_secs_f64(),
        pct(0.50),
        pct(0.90),
        pct(0.99),
        pct(0.999),
        all.errors,
        all.connects
    );
}
