# Dynamic IPv6 Pool Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an in-process dynamic IPv6 pool that leases one pre-bound IPv6 per proxied request or CONNECT tunnel, then recycles it after use.

**Architecture:** The proxy owns a shared pool of IPv6 addresses. On startup the pool generates `pool-size` random addresses inside the configured subnet, adds each address as `/128` to the configured interface, and exposes async lease/release operations. HTTP requests and CONNECT tunnels lease an address before dialing upstream, bind outbound sockets to that address, and release the address after the request/tunnel ends; release waits for `cooldown-secs`, deletes the old address, generates a replacement, adds it to the interface, and returns the replacement to the pool.

**Tech Stack:** Rust 2021, Tokio, Hyper 0.14, std process commands for `ip -6 addr add/del`.

---

### Task 1: CLI Options

**Files:**
- Modify: `src/main.rs`

Add options:
- `--iface IFACE`, default `eth0`
- `--pool-size SIZE`, default `1000`
- `--cooldown-secs SECONDS`, default `60`

Pass these values into `start_proxy` through a `PoolConfig` struct.

### Task 2: Pool Implementation

**Files:**
- Create: `src/pool.rs`
- Modify: `src/main.rs`

Implement:
- `PoolConfig { iface, ipv6, prefix_len, pool_size, cooldown }`
- `Ipv6Pool::new(config)` that fills the initial pool by running `ip -6 addr add <ip>/128 dev <iface>`.
- `Ipv6Pool::lease()` returning a lease object with one IPv6.
- `Ipv6Pool::release(ip)` that spawns cooldown deletion and replacement.

Use `tokio::sync::Mutex<Vec<Ipv6Addr>>` and `tokio::process::Command`.

### Task 3: Proxy Integration

**Files:**
- Modify: `src/proxy.rs`

Replace direct CIDR random generation with pool leases:
- HTTP request leases IP, builds `HttpConnector` with that local address, awaits response, then releases IP.
- CONNECT leases IP before socket bind, binds `SocketAddr::new(ip, 0)`, and releases after tunnel copy completes or connect fails.
- If no IP is available, return `503 Service Unavailable`.

### Task 4: Verification

**Commands:**
- `cargo fmt`
- `cargo check` on Linux target server
- Server runtime test:
  - Start with `-b 127.0.0.1:51080 -i 2602:f66f:70:d968::/64 --iface eth0 --pool-size 20 --cooldown-secs 10`
  - Run `curl -x http://127.0.0.1:51080 https://ipv6.ip.sb` repeatedly.
  - Confirm outputs are different IPv6 addresses and that `ip -6 addr show dev eth0` contains only the active pool plus normal addresses.
