use kata_device_plugin::{plugin, vfio};

use std::path::Path;

use plugin::DeviceServer;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kata_device_plugin=info".parse().unwrap()),
        )
        .init();
    info!("kata-device-plugin starting");

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("shutdown");
        sd.cancel();
    });

    // Declared by the node, not configured: advertise exactly the RESOURCES
    // rows that have VFIO-bound devices.
    let mut servers = Vec::new();
    for res in vfio::RESOURCES {
        let devs = vfio::enumerate(Path::new(vfio::VFIO_DIR), Path::new(vfio::SYSFS_DIR), res);
        if devs.is_empty() {
            info!(
                resource = res.name,
                "no matching VFIO devices, not advertising"
            );
            continue;
        }
        info!(resource = res.name, count = devs.len(), "advertising");
        servers.push(DeviceServer::new(
            *res,
            vfio::VFIO_DIR,
            vfio::SYSFS_DIR,
            plugin::SOCKET_DIR,
            plugin::CDI_DIR,
        ));
    }
    if servers.is_empty() {
        // Nothing to declare.  Stay up quietly: the advertised set is fixed
        // for the plugin's lifetime, and a device-set change on the node
        // means this pod gets restarted.
        info!("no VFIO devices for any known resource");
        shutdown.cancelled().await;
        return Ok(());
    }

    let results =
        futures::future::join_all(servers.into_iter().map(|s| s.run(shutdown.clone()))).await;
    for res in results {
        if let Err(e) = res {
            // {:#} keeps the error's cause chain; bare Display drops it.
            warn!("plugin error: {e:#}");
        }
    }
    Ok(())
}
