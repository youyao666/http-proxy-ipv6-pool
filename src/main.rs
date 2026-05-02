mod pool;
mod proxy;
mod socks5;

use cidr::Ipv6Cidr;
use getopts::Options;
use pool::{Ipv6Pool, PoolConfig};
use proxy::{Source, start_proxy};
use socks5::{Socks5Config, start_socks5};
use std::{env, process::exit, time::Duration};

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("b", "bind", "http proxy bind address", "BIND");
    opts.optflag(
        "",
        "dynamic-pool",
        "bind a pre-added dynamic IPv6 pool and recycle each used IP",
    );
    opts.optopt(
        "",
        "iface",
        "network interface for dynamic IPv6 addresses",
        "IFACE",
    );
    opts.optopt("", "pool-size", "dynamic IPv6 pool size", "SIZE");
    opts.optopt(
        "",
        "cooldown-secs",
        "seconds to wait before recycling a used IPv6",
        "SECONDS",
    );
    opts.optopt(
        "i",
        "ipv6-subnet",
        "IPv6 Subnet: 2001:19f0:6001:48e4::/64",
        "IPv6_SUBNET",
    );
    opts.optopt("", "socks5-bind", "SOCKS5 proxy bind address", "BIND");
    opts.optopt("", "socks5-user", "SOCKS5 username", "USER");
    opts.optopt("", "socks5-pass", "SOCKS5 password", "PASS");
    opts.optflag("h", "help", "print this help menu");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => {
            panic!("{}", f.to_string())
        }
    };
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    let bind_addr = matches.opt_str("b").unwrap_or("0.0.0.0:51080".to_string());
    let ipve_subnet = matches
        .opt_str("i")
        .unwrap_or("2001:19f0:6001:48e4::/64".to_string());
    let iface = matches.opt_str("iface").unwrap_or("eth0".to_string());
    let dynamic_pool = matches.opt_present("dynamic-pool");
    let pool_size = matches
        .opt_str("pool-size")
        .unwrap_or("1000".to_string())
        .parse::<usize>()
        .unwrap_or_else(|_| {
            println!("pool size not valid");
            exit(1);
        });
    let cooldown_secs = matches
        .opt_str("cooldown-secs")
        .unwrap_or("60".to_string())
        .parse::<u64>()
        .unwrap_or_else(|_| {
            println!("cooldown seconds not valid");
            exit(1);
        });
    let socks5_bind = matches.opt_str("socks5-bind");
    let socks5_user = matches.opt_str("socks5-user");
    let socks5_pass = matches.opt_str("socks5-pass");
    run(
        bind_addr,
        ipve_subnet,
        dynamic_pool,
        iface,
        pool_size,
        cooldown_secs,
        socks5_bind,
        socks5_user,
        socks5_pass,
    )
}

#[tokio::main]
async fn run(
    bind_addr: String,
    ipv6_subnet: String,
    dynamic_pool: bool,
    iface: String,
    pool_size: usize,
    cooldown_secs: u64,
    socks5_bind: Option<String>,
    socks5_user: Option<String>,
    socks5_pass: Option<String>,
) {
    let ipv6 = match ipv6_subnet.parse::<Ipv6Cidr>() {
        Ok(cidr) => {
            let a = cidr.first_address();
            let b = cidr.network_length();
            (a, b)
        }
        Err(_) => {
            println!("invalid IPv6 subnet");
            exit(1);
        }
    };

    let bind_addr = match bind_addr.parse() {
        Ok(b) => b,
        Err(e) => {
            println!("bind address not valid: {}", e);
            return;
        }
    };
    let source = if dynamic_pool {
        let config = PoolConfig {
            iface,
            ipv6: ipv6.0,
            prefix_len: ipv6.1,
            pool_size,
            cooldown: Duration::from_secs(cooldown_secs),
        };
        match Ipv6Pool::new(config).await {
            Ok(pool) => Source::Dynamic(pool),
            Err(e) => {
                println!("failed to initialize dynamic IPv6 pool: {e}");
                return;
            }
        }
    } else {
        Source::RandomCidr {
            ipv6: u128::from(ipv6.0),
            prefix_len: ipv6.1,
        }
    };

    if let Some(socks5_bind) = socks5_bind {
        let Some(username) = socks5_user else {
            println!("socks5 username is required when --socks5-bind is set");
            exit(1);
        };
        let Some(password) = socks5_pass else {
            println!("socks5 password is required when --socks5-bind is set");
            exit(1);
        };
        let socks5_bind = match socks5_bind.parse() {
            Ok(b) => b,
            Err(e) => {
                println!("socks5 bind address not valid: {e}");
                return;
            }
        };
        let socks5_config = Socks5Config {
            bind_addr: socks5_bind,
            username,
            password,
        };
        let socks5_source = source.clone();
        tokio::spawn(async move {
            if let Err(e) = start_socks5(socks5_config, socks5_source).await {
                println!("SOCKS5 server error: {e}");
            }
        });
    }

    if let Err(e) = start_proxy(bind_addr, source).await {
        println!("{}", e);
    }
}
