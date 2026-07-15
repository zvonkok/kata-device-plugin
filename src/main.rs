use kata_device_plugin::{config, labels, platform, plugin};

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Node;
use kube::{runtime::watcher, Api, Client};
use notify::{RecursiveMode, Watcher};
use plugin::{DeviceServer, Mode};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use config::Config;

#[derive(Parser)]
#[command(about = "NFD-driven passthrough GPU device plugin for Kata VMs")]
struct Args {
    /// Path to the TOML config file (mounted from a ConfigMap).
    /// Missing file is not an error — compiled-in defaults are used.
    #[arg(long, env, default_value = "/etc/kata-device-plugin/config.toml")]
    config: PathBuf,

    /// Node name injected via the downward API — not a config option.
    #[arg(long, env)]
    node_name: String,
}

fn select_mode(labels: &BTreeMap<String, String>) -> Mode {
    match labels
        .get(labels::LABEL_GPU_CLIQUE)
        .filter(|v| !v.is_empty())
    {
        Some(clique_id) => Mode::Imex {
            clique_id: clique_id.clone(),
        },
        None => Mode::Pgpu,
    }
}

/// Spawn a tokio channel that receives a `()` whenever the config file
/// (or its parent directory) changes.  The channel capacity is 1 so
/// rapid successive events — e.g. the double write from a ConfigMap
/// symlink swap — collapse into a single reload trigger.
fn config_file_watcher(path: &Path) -> anyhow::Result<(impl Watcher, mpsc::Receiver<()>)> {
    let (tx, rx) = mpsc::channel(1);
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if res.is_ok() {
            let _ = tx.try_send(());
        }
    })?;
    // Watch the parent directory: Kubernetes ConfigMap mounts use an atomic
    // symlink swap on the directory, not a write to the file itself.
    let dir = path.parent().unwrap_or(path);
    watcher.watch(dir, RecursiveMode::NonRecursive)?;
    Ok((watcher, rx))
}

/// Run one DeviceServer per enabled resource until the token fires.
async fn run_plugins(cfg: Config, mode: Mode, token: CancellationToken) {
    let mut servers = vec![DeviceServer::new(
        &cfg.gpu.resource_name,
        plugin::VFIO_DIR,
        plugin::SOCKET_DIR,
        &cfg.plugin.cdi_dir,
        mode,
    )];
    if cfg.nvswitch.enabled {
        // NVSwitch is always plain passthrough — it has no IMEX role.
        servers.push(DeviceServer::new(
            &cfg.nvswitch.resource_name,
            plugin::VFIO_DIR,
            plugin::SOCKET_DIR,
            &cfg.plugin.cdi_dir,
            Mode::Pgpu,
        ));
    }
    let results =
        futures::future::join_all(servers.into_iter().map(|s| s.run(token.clone()))).await;
    for res in results {
        if let Err(e) = res {
            warn!(%e, "plugin error");
        }
    }
}

/// Stop the running plugin task (if any), wait for it to release its sockets
/// and clean up its CDI specs, then start a fresh one.
async fn restart(
    task: &mut Option<(CancellationToken, tokio::task::JoinHandle<()>)>,
    cfg: Config,
    mode: Mode,
) {
    if let Some((tok, handle)) = task.take() {
        tok.cancel();
        let _ = handle.await;
    }
    info!(
        mode = mode.name(),
        nvswitch = cfg.nvswitch.enabled,
        "starting plugins"
    );
    let tok = CancellationToken::new();
    let handle = tokio::spawn(run_plugins(cfg, mode, tok.clone()));
    *task = Some((tok, handle));
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kata_device_plugin=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();
    let platform = platform::Platform::detect();
    info!(
        node = %args.node_name,
        config = %args.config.display(),
        platform = platform.name(),
        nvlink_partitions = ?platform.nvlink_partitions(),
        "kata-device-plugin starting"
    );

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("shutdown");
        sd.cancel();
    });

    run(&args, shutdown).await
}

async fn run(args: &Args, shutdown: CancellationToken) -> anyhow::Result<()> {
    let nodes: Api<Node> = Api::all(Client::try_default().await?);
    let watcher_cfg =
        watcher::Config::default().fields(&format!("metadata.name={}", args.node_name));
    let mut node_stream = watcher(nodes, watcher_cfg).boxed();

    let (_watcher, mut config_rx) = config_file_watcher(&args.config)?;

    let mut active_cfg = Config::load(&args.config);
    let mut active_mode: Option<Mode> = None;
    let mut task: Option<(CancellationToken, tokio::task::JoinHandle<()>)> = None;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,

            // Node label change.
            ev = node_stream.try_next() => {
                let labels = match ev.context("node watch")? {
                    Some(watcher::Event::Apply(n) | watcher::Event::InitApply(n)) => {
                        n.metadata.labels.unwrap_or_default()
                    }
                    _ => continue,
                };
                let mode = select_mode(&labels);
                if Some(&mode) == active_mode.as_ref() {
                    continue;
                }
                active_mode = Some(mode.clone());
                restart(&mut task, active_cfg.clone(), mode).await;
            }

            // Config file change (ConfigMap symlink swap).
            _ = config_rx.recv() => {
                // Brief pause so the atomic swap has time to complete.
                tokio::time::sleep(Duration::from_millis(200)).await;
                let new_cfg = Config::load(&args.config);
                if new_cfg == active_cfg {
                    continue;
                }
                info!("config changed, reloading plugins");
                active_cfg = new_cfg;
                if let Some(mode) = active_mode.clone() {
                    restart(&mut task, active_cfg.clone(), mode).await;
                }
            }
        }
    }

    if let Some((tok, h)) = task.take() {
        tok.cancel();
        let _ = h.await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn no_clique_label_gives_pgpu() {
        assert_eq!(select_mode(&labels(&[])), Mode::Pgpu);
        assert_eq!(select_mode(&labels(&[("unrelated", "value")])), Mode::Pgpu);
    }

    #[test]
    fn clique_label_gives_imex() {
        let l = labels(&[(
            labels::LABEL_GPU_CLIQUE,
            "7b968a6d-c8aa-45e1-9e70-e1e51be99c31.1",
        )]);
        assert_eq!(
            select_mode(&l),
            Mode::Imex {
                clique_id: "7b968a6d-c8aa-45e1-9e70-e1e51be99c31.1".to_owned()
            }
        );
    }

    #[test]
    fn empty_clique_label_gives_pgpu() {
        // GFD only writes the label when GPU_FABRIC_STATE_COMPLETED; an empty
        // value means the label exists but fabric init is not done.
        let l = labels(&[(labels::LABEL_GPU_CLIQUE, "")]);
        assert_eq!(select_mode(&l), Mode::Pgpu);
    }
}
