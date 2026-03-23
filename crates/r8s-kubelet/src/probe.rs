use r8s_types::{IntOrString, Probe};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn int_or_string_to_port(ios: &IntOrString) -> i32 {
    match ios {
        IntOrString::Int(i) => *i,
        IntOrString::String(s) => s.parse().unwrap_or(0),
    }
}

/// Execute a probe against a container. Returns true if the probe succeeds.
pub async fn exec_probe(probe: &Probe, pod_ip: &str, _pid: u32) -> bool {
    let timeout = Duration::from_secs(probe.timeout_seconds.unwrap_or(1) as u64);

    if let Some(http) = &probe.http_get {
        let port = int_or_string_to_port(&http.port);
        let path = http.path.as_deref().unwrap_or("/");
        let host = http.host.as_deref().unwrap_or(pod_ip);
        return http_get_probe(host, port, path, timeout).await;
    }

    if let Some(tcp) = &probe.tcp_socket {
        let port = int_or_string_to_port(&tcp.port);
        return tcp_probe(pod_ip, port, timeout).await;
    }

    if let Some(exec_action) = &probe.exec
        && let Some(command) = &exec_action.command
    {
        return exec_command_probe(_pid, command, timeout).await;
    }

    // No probe handler configured — treat as success
    true
}

async fn http_get_probe(host: &str, port: i32, path: &str, timeout: Duration) -> bool {
    let addr = format!("{host}:{port}");
    let result = tokio::time::timeout(timeout, async {
        let mut stream = TcpStream::connect(&addr).await?;
        let request = format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await?;
        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await?;
        let response = String::from_utf8_lossy(&buf[..n]);
        // Check HTTP status line: "HTTP/1.x 2xx" or "HTTP/1.x 3xx"
        if let Some(code_str) = response
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            && let Ok(code) = code_str.parse::<u16>()
        {
            return Ok::<bool, std::io::Error>((200..400).contains(&code));
        }
        Ok(false)
    })
    .await;

    matches!(result, Ok(Ok(true)))
}

async fn tcp_probe(host: &str, port: i32, timeout: Duration) -> bool {
    let addr = format!("{host}:{port}");
    tokio::time::timeout(timeout, TcpStream::connect(&addr))
        .await
        .is_ok_and(|r| r.is_ok())
}

async fn exec_command_probe(pid: u32, command: &[String], timeout: Duration) -> bool {
    if command.is_empty() {
        return false;
    }

    let result = tokio::time::timeout(timeout, async {
        let mut args = vec![
            "-t".to_string(),
            pid.to_string(),
            "-m".to_string(),
            "-p".to_string(),
            "--".to_string(),
        ];
        args.extend(command.iter().cloned());

        tokio::process::Command::new("nsenter")
            .args(&args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await
    })
    .await;

    matches!(result, Ok(Ok(status)) if status.success())
}
