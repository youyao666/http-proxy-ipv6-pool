use crate::pool::Ipv6Pool;
use hyper::{
    Body, Client, Method, Request, Response, Server,
    client::HttpConnector,
    http::StatusCode,
    server::conn::AddrStream,
    service::{make_service_fn, service_fn},
};
use rand::Rng;
use std::net::{IpAddr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpSocket,
};

pub async fn start_proxy(
    listen_addr: SocketAddr,
    source: Source,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let make_service = make_service_fn(move |_: &AddrStream| {
        let source = source.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                let source = source.clone();
                Proxy { source }.proxy(req)
            }))
        }
    });

    Server::bind(&listen_addr)
        .http1_preserve_header_case(true)
        .http1_title_case_headers(true)
        .serve(make_service)
        .await
        .map_err(|err| err.into())
}

#[derive(Clone)]
pub(crate) enum Source {
    RandomCidr { ipv6: u128, prefix_len: u8 },
    Dynamic(Ipv6Pool),
}

impl Source {
    pub(crate) async fn lease(&self) -> Option<Ipv6Addr> {
        match self {
            Source::RandomCidr { ipv6, prefix_len } => Some(get_rand_ipv6(*ipv6, *prefix_len)),
            Source::Dynamic(pool) => pool.lease().await,
        }
    }

    pub(crate) fn release(&self, ip: Ipv6Addr) {
        if let Source::Dynamic(pool) = self {
            pool.release(ip);
        }
    }
}

#[derive(Clone)]
pub(crate) struct Proxy {
    source: Source,
}

impl Proxy {
    pub(crate) async fn proxy(self, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        match if req.method() == Method::CONNECT {
            self.process_connect(req).await
        } else {
            self.process_request(req).await
        } {
            Ok(resp) => Ok(resp),
            Err(e) => Err(e),
        }
    }

    async fn process_connect(self, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        tokio::task::spawn(async move {
            let Some(remote_addr) = req.uri().authority().map(|auth| auth.to_string()) else {
                return;
            };
            let Ok(mut upgraded) = hyper::upgrade::on(req).await else {
                return;
            };
            if let Err(e) = self.tunnel(&mut upgraded, remote_addr).await {
                println!("connect tunnel error: {e}");
            }
        });
        Ok(Response::new(Body::empty()))
    }

    async fn process_request(self, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        let Some(bind_ip) = self.source.lease().await else {
            return Ok(service_unavailable());
        };
        let bind_addr = IpAddr::V6(bind_ip);
        let mut http = HttpConnector::new();
        http.set_local_address(Some(bind_addr));
        println!("{} via {bind_addr}", req.uri().host().unwrap_or_default());

        let client = Client::builder()
            .http1_title_case_headers(true)
            .http1_preserve_header_case(true)
            .build(http);
        let res = match client.request(req).await {
            Ok(res) => res,
            Err(e) => {
                self.source.release(bind_ip);
                return Err(e);
            }
        };
        let (parts, body) = res.into_parts();
        let body = match hyper::body::to_bytes(body).await {
            Ok(body) => body,
            Err(e) => {
                self.source.release(bind_ip);
                return Err(e);
            }
        };
        self.source.release(bind_ip);
        Ok(Response::from_parts(parts, Body::from(body)))
    }

    async fn tunnel<A>(self, upgraded: &mut A, addr_str: String) -> std::io::Result<()>
    where
        A: AsyncRead + AsyncWrite + Unpin + ?Sized,
    {
        let Some(bind_ip) = self.source.lease().await else {
            println!("no IPv6 available for {addr_str}");
            return Ok(());
        };

        let source = self.source.clone();
        let result = self.tunnel_with_ip(upgraded, &addr_str, bind_ip).await;
        source.release(bind_ip);
        result
    }

    async fn tunnel_with_ip<A>(
        self,
        upgraded: &mut A,
        addr_str: &str,
        bind_ip: Ipv6Addr,
    ) -> std::io::Result<()>
    where
        A: AsyncRead + AsyncWrite + Unpin + ?Sized,
    {
        if let Ok(addrs) = addr_str.to_socket_addrs() {
            for addr in addrs {
                let socket = TcpSocket::new_v6()?;
                let bind_addr = SocketAddr::new(IpAddr::V6(bind_ip), 0);
                if socket.bind(bind_addr).is_ok() {
                    println!("{addr_str} via {bind_addr}");
                    if let Ok(mut server) = socket.connect(addr).await {
                        tokio::io::copy_bidirectional(upgraded, &mut server).await?;
                        return Ok(());
                    }
                }
            }
        } else {
            println!("error: {addr_str}");
        }

        Ok(())
    }
}

fn service_unavailable() -> Response<Body> {
    let mut resp = Response::new(Body::from("dynamic IPv6 pool exhausted"));
    *resp.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    resp
}

fn get_rand_ipv6(mut ipv6: u128, prefix_len: u8) -> Ipv6Addr {
    let rand: u128 = rand::thread_rng().gen();
    if prefix_len == 128 {
        return ipv6.into();
    }
    if prefix_len == 0 {
        return rand.into();
    }

    let host_bits = 128 - prefix_len;
    let net_mask = u128::MAX << host_bits;
    let host_mask = !net_mask;
    ipv6 = (ipv6 & net_mask) | (rand & host_mask);
    ipv6.into()
}
