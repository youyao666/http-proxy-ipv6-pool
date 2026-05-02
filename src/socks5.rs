use crate::proxy::Source;
use std::{
    io,
    net::{IpAddr, Ipv6Addr, SocketAddr},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, TcpStream, lookup_host},
    time::{Duration, timeout},
};

#[derive(Clone)]
pub(crate) struct Socks5Config {
    pub bind_addr: SocketAddr,
    pub username: String,
    pub password: String,
}

pub(crate) async fn start_socks5(config: Socks5Config, source: Source) -> io::Result<()> {
    let listener = TcpListener::bind(config.bind_addr).await?;
    println!("SOCKS5 listening on {}", config.bind_addr);

    loop {
        let (stream, peer) = listener.accept().await?;
        let config = config.clone();
        let source = source.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, config, source).await {
                println!("SOCKS5 {peer} error: {e}");
            }
        });
    }
}

async fn handle_client(
    mut client: TcpStream,
    config: Socks5Config,
    source: Source,
) -> io::Result<()> {
    negotiate_auth(&mut client).await?;
    authenticate(&mut client, &config.username, &config.password).await?;
    let target = read_connect_request(&mut client).await?;

    let Some(bind_ip) = source.lease().await else {
        send_reply(&mut client, 0x01, None).await?;
        return Ok(());
    };

    let result = connect_and_relay(&mut client, target, bind_ip).await;
    source.release(bind_ip);
    result
}

async fn negotiate_auth(client: &mut TcpStream) -> io::Result<()> {
    let mut head = [0u8; 2];
    client.read_exact(&mut head).await?;
    if head[0] != 0x05 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid SOCKS version",
        ));
    }

    let mut methods = vec![0u8; head[1] as usize];
    client.read_exact(&mut methods).await?;
    if methods.contains(&0x02) {
        client.write_all(&[0x05, 0x02]).await
    } else {
        client.write_all(&[0x05, 0xff]).await?;
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "username/password auth not offered",
        ))
    }
}

async fn authenticate(client: &mut TcpStream, username: &str, password: &str) -> io::Result<()> {
    let mut ver = [0u8; 1];
    client.read_exact(&mut ver).await?;
    if ver[0] != 0x01 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid auth version",
        ));
    }

    let mut ulen = [0u8; 1];
    client.read_exact(&mut ulen).await?;
    let mut user = vec![0u8; ulen[0] as usize];
    client.read_exact(&mut user).await?;

    let mut plen = [0u8; 1];
    client.read_exact(&mut plen).await?;
    let mut pass = vec![0u8; plen[0] as usize];
    client.read_exact(&mut pass).await?;

    if user == username.as_bytes() && pass == password.as_bytes() {
        client.write_all(&[0x01, 0x00]).await
    } else {
        client.write_all(&[0x01, 0x01]).await?;
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "bad credentials",
        ))
    }
}

async fn read_connect_request(client: &mut TcpStream) -> io::Result<TargetAddr> {
    let mut head = [0u8; 4];
    client.read_exact(&mut head).await?;
    if head[0] != 0x05 || head[1] != 0x01 || head[2] != 0x00 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "only SOCKS5 CONNECT is supported",
        ));
    }

    let host = match head[3] {
        0x01 => {
            let mut addr = [0u8; 4];
            client.read_exact(&mut addr).await?;
            IpAddr::from(addr).to_string()
        }
        0x03 => {
            let mut len = [0u8; 1];
            client.read_exact(&mut len).await?;
            let mut domain = vec![0u8; len[0] as usize];
            client.read_exact(&mut domain).await?;
            String::from_utf8(domain)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid domain"))?
        }
        0x04 => {
            let mut addr = [0u8; 16];
            client.read_exact(&mut addr).await?;
            IpAddr::from(addr).to_string()
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported address type",
            ));
        }
    };

    let mut port = [0u8; 2];
    client.read_exact(&mut port).await?;
    Ok(TargetAddr {
        host,
        port: u16::from_be_bytes(port),
    })
}

struct TargetAddr {
    host: String,
    port: u16,
}

async fn connect_and_relay(
    client: &mut TcpStream,
    target: TargetAddr,
    bind_ip: Ipv6Addr,
) -> io::Result<()> {
    let addr = if target.host.contains(':') {
        format!("[{}]:{}", target.host, target.port)
    } else {
        format!("{}:{}", target.host, target.port)
    };
    println!("SOCKS5 {addr} via {bind_ip}");

    let mut last_err = None;
    let addrs = lookup_host(addr).await?;
    for addr in addrs {
        if !addr.is_ipv6() {
            continue;
        }

        let socket = TcpSocket::new_v6()?;
        socket.bind(SocketAddr::new(IpAddr::V6(bind_ip), 0))?;
        match timeout(Duration::from_secs(5), socket.connect(addr)).await {
            Ok(Ok(mut server)) => {
                send_reply(client, 0x00, server.local_addr().ok()).await?;
                tokio::io::copy_bidirectional(client, &mut server).await?;
                return Ok(());
            }
            Ok(Err(e)) => {
                last_err = Some(e);
            }
            Err(_) => {
                last_err = Some(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("connect to {addr} timed out"),
                ));
            }
        }
    }

    send_reply(client, 0x05, None).await?;
    Err(last_err
        .unwrap_or_else(|| io::Error::new(io::ErrorKind::NotFound, "target has no IPv6 address")))
}

async fn send_reply(client: &mut TcpStream, rep: u8, bind: Option<SocketAddr>) -> io::Result<()> {
    let bind = bind.unwrap_or_else(|| SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0));
    let mut resp = vec![0x05, rep, 0x00];
    match bind.ip() {
        IpAddr::V4(ip) => {
            resp.push(0x01);
            resp.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            resp.push(0x04);
            resp.extend_from_slice(&ip.octets());
        }
    }
    resp.extend_from_slice(&bind.port().to_be_bytes());
    client.write_all(&resp).await
}
