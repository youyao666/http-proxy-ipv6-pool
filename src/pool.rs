use rand::Rng;
use std::{collections::HashSet, error::Error, net::Ipv6Addr, sync::Arc, time::Duration};
use tokio::{process::Command, sync::Mutex, time::sleep};

#[derive(Clone)]
pub(crate) struct PoolConfig {
    pub iface: String,
    pub ipv6: Ipv6Addr,
    pub prefix_len: u8,
    pub pool_size: usize,
    pub cooldown: Duration,
}

struct PoolState {
    available: Vec<Ipv6Addr>,
    active: HashSet<Ipv6Addr>,
    used: HashSet<Ipv6Addr>,
}

#[derive(Clone)]
pub(crate) struct Ipv6Pool {
    config: PoolConfig,
    state: Arc<Mutex<PoolState>>,
}

impl Ipv6Pool {
    pub(crate) async fn new(config: PoolConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let pool = Self {
            config,
            state: Arc::new(Mutex::new(PoolState {
                available: Vec::new(),
                active: HashSet::new(),
                used: HashSet::new(),
            })),
        };

        for _ in 0..pool.config.pool_size {
            pool.add_replacement().await?;
        }

        Ok(pool)
    }

    pub(crate) async fn lease(&self) -> Option<Ipv6Addr> {
        let mut state = self.state.lock().await;
        state.available.pop()
    }

    pub(crate) fn release(&self, ip: Ipv6Addr) {
        let pool = self.clone();
        tokio::spawn(async move {
            sleep(pool.config.cooldown).await;

            if let Err(e) = pool.del_addr(ip).await {
                println!("failed to delete {ip}: {e}");
            }

            {
                let mut state = pool.state.lock().await;
                state.active.remove(&ip);
                state.used.insert(ip);
            }

            if let Err(e) = pool.add_replacement().await {
                println!("failed to add replacement IPv6: {e}");
            }
        });
    }

    async fn add_replacement(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        for _ in 0..1000 {
            let ip = self.next_candidate().await;
            match self.add_addr(ip).await {
                Ok(()) => {
                    let mut state = self.state.lock().await;
                    state.active.insert(ip);
                    state.available.push(ip);
                    println!("added {ip} to dynamic pool");
                    return Ok(());
                }
                Err(e) => {
                    println!("failed to add candidate {ip}: {e}");
                }
            }
        }

        Err("could not add a replacement IPv6 after 1000 attempts".into())
    }

    async fn next_candidate(&self) -> Ipv6Addr {
        loop {
            let ip = random_ipv6(self.config.ipv6, self.config.prefix_len);
            let state = self.state.lock().await;
            if !state.active.contains(&ip) && !state.used.contains(&ip) {
                return ip;
            }
        }
    }

    async fn add_addr(&self, ip: Ipv6Addr) -> Result<(), Box<dyn Error + Send + Sync>> {
        run_ip_addr_cmd("add", ip, &self.config.iface).await
    }

    async fn del_addr(&self, ip: Ipv6Addr) -> Result<(), Box<dyn Error + Send + Sync>> {
        run_ip_addr_cmd("del", ip, &self.config.iface).await
    }
}

async fn run_ip_addr_cmd(
    action: &str,
    ip: Ipv6Addr,
    iface: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let addr = format!("{ip}/128");
    let output = Command::new("ip")
        .args(["-6", "addr", action, &addr, "dev", iface])
        .output()
        .await?;

    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!("ip -6 addr {action} {addr} dev {iface}: {stderr}").into())
    }
}

fn random_ipv6(ipv6: Ipv6Addr, prefix_len: u8) -> Ipv6Addr {
    let ipv6 = u128::from(ipv6);
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
    ((ipv6 & net_mask) | (rand & host_mask)).into()
}
