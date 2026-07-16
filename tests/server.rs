/// Integration tests: a real DeviceServer against a mock kubelet, no GPU or
/// cluster required.  The mock registration service probes the plugin's
/// endpoint before acknowledging, exactly like kubelet's device manager — so
/// ordering bugs (register before serve) fail here instead of on a node.
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kata_device_plugin::dp::v1beta1::{
    device_plugin_client::DevicePluginClient,
    registration_server::{Registration, RegistrationServer},
    AllocateRequest, ContainerAllocateRequest, Empty, ListAndWatchResponse, RegisterRequest,
};
use kata_device_plugin::plugin::DeviceServer;
use kata_device_plugin::vfio::{Resource, RESOURCES};
use tempfile::TempDir;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint, Server, Uri};
use tonic::{Request, Response, Status, Streaming};
use tower::service_fn;

// ── mock kubelet ──────────────────────────────────────────────────────────────

struct MockKubelet {
    socket_dir: PathBuf,
    registered: Arc<Mutex<Option<RegisterRequest>>>,
}

#[tonic::async_trait]
impl Registration for MockKubelet {
    async fn register(&self, req: Request<RegisterRequest>) -> Result<Response<Empty>, Status> {
        let req = req.into_inner();
        // The real kubelet dials the endpoint and probes it inside Register —
        // a plugin that registers before serving deadlocks right here.
        let sock = self.socket_dir.join(&req.endpoint);
        let probe = async {
            let mut client = unix_client(sock).await;
            client
                .get_device_plugin_options(Request::new(Empty {}))
                .await
        };
        tokio::time::timeout(Duration::from_secs(2), probe)
            .await
            .map_err(|_| Status::deadline_exceeded("failed to dial device plugin"))?
            .map_err(|e| Status::unknown(format!("endpoint probe failed: {e}")))?;
        *self.registered.lock().unwrap() = Some(req);
        Ok(Response::new(Empty {}))
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Fake /dev/vfio layout under one temp root:
/// `<root>/devices/vfio<n>` + `<root>/sysfs/vfio<n>/device/{vendor,class}`.
fn add_dev(root: &std::path::Path, n: u32, vendor: &str, class: &str) {
    let devices = root.join("devices");
    std::fs::create_dir_all(&devices).unwrap();
    std::fs::write(devices.join(format!("vfio{n}")), b"").unwrap();
    let device = root.join("sysfs").join(format!("vfio{n}")).join("device");
    std::fs::create_dir_all(&device).unwrap();
    std::fs::write(device.join("vendor"), format!("{vendor}\n")).unwrap();
    std::fs::write(device.join("class"), format!("{class}\n")).unwrap();
}

/// n GPU cdevs (vfio0..vfio(n-1)); n = 0 gives an empty devices dir.
fn fake_vfio(n: u32) -> TempDir {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir(dir.path().join("devices")).unwrap();
    for i in 0..n {
        add_dev(dir.path(), i, "0x10de", "0x030200");
    }
    dir
}

/// Look up a row from the plugin's real resource table.
fn resource(name: &str) -> Resource {
    *RESOURCES.iter().find(|r| r.name == name).unwrap()
}

async fn unix_client(socket_path: PathBuf) -> DevicePluginClient<Channel> {
    let channel = Endpoint::try_from("http://[::]:0")
        .unwrap()
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket_path = socket_path.clone();
            async move {
                // tonic 0.12 speaks hyper 1.0 IO traits, not tokio's.
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    tokio::net::UnixStream::connect(socket_path).await?,
                ))
            }
        }))
        .await
        .unwrap();
    DevicePluginClient::new(channel)
}

/// Mock kubelet plus one running DeviceServer per resource name.
struct Node {
    // Keeps the fake /dev/vfio alive for the servers' lifetime.
    _vfio: TempDir,
    sockets: TempDir,
    cdi: TempDir,
    registered: Arc<Mutex<Option<RegisterRequest>>>,
    token: CancellationToken,
    servers: Vec<tokio::task::JoinHandle<()>>,
}

impl Node {
    async fn start(vfio: TempDir, resources: &[&str]) -> Node {
        let sockets = TempDir::new().unwrap();
        let cdi = TempDir::new().unwrap();
        let registered = Arc::new(Mutex::new(None));

        let listener = UnixListener::bind(sockets.path().join("kubelet.sock")).unwrap();
        let mock = MockKubelet {
            socket_dir: sockets.path().to_path_buf(),
            registered: registered.clone(),
        };
        tokio::spawn(async move {
            Server::builder()
                .add_service(RegistrationServer::new(mock))
                .serve_with_incoming(UnixListenerStream::new(listener))
                .await
                .ok();
        });

        let token = CancellationToken::new();
        let servers = resources
            .iter()
            .map(|name| {
                let server = DeviceServer::new(
                    resource(name),
                    vfio.path().to_str().unwrap(),
                    vfio.path().join("sysfs").to_str().unwrap(),
                    sockets.path().to_str().unwrap(),
                    cdi.path().to_str().unwrap(),
                );
                let tok = token.clone();
                tokio::spawn(async move {
                    server.run(tok).await.ok();
                })
            })
            .collect();
        // Let registration and the kubelet dial-back settle.
        tokio::time::sleep(Duration::from_millis(400)).await;

        Node {
            _vfio: vfio,
            sockets,
            cdi,
            registered,
            token,
            servers,
        }
    }

    async fn client(&self, sock: &str) -> DevicePluginClient<Channel> {
        unix_client(self.sockets.path().join(sock)).await
    }

    /// First ListAndWatch message plus the still-open stream.
    async fn snapshot(
        &self,
        sock: &str,
    ) -> (Streaming<ListAndWatchResponse>, ListAndWatchResponse) {
        let mut stream = self
            .client(sock)
            .await
            .list_and_watch(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner();
        let resp = tokio::time::timeout(Duration::from_secs(2), stream.message())
            .await
            .expect("ListAndWatch timeout")
            .unwrap()
            .expect("stream closed");
        (stream, resp)
    }

    fn spec(&self, file: &str) -> Option<String> {
        std::fs::read_to_string(self.cdi.path().join(file)).ok()
    }

    /// Cancel and require prompt exit: a graceful drain would hang forever on
    /// the held-open ListAndWatch streams.
    async fn shutdown(self) {
        self.token.cancel();
        for task in self.servers {
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .expect("server did not shut down after cancel")
                .unwrap();
        }
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn registers_with_kubelet_and_lists_devices() {
    let node = Node::start(fake_vfio(4), &["nvidia.com/gpu"]).await;

    let reg = node
        .registered
        .lock()
        .unwrap()
        .clone()
        .expect("plugin did not register");
    assert_eq!(reg.resource_name, "nvidia.com/gpu");
    assert_eq!(reg.endpoint, "kata-gpu.sock");
    assert_eq!(reg.version, "v1beta1");

    let (mut stream, resp) = node.snapshot("kata-gpu.sock").await;
    assert_eq!(resp.devices.len(), 4);

    // The stream must stay open after the snapshot: the kubelet treats a
    // completed ListAndWatch stream as endpoint death (resource unhealthy).
    let second = tokio::time::timeout(Duration::from_millis(300), stream.message()).await;
    assert!(second.is_err(), "ListAndWatch stream ended after snapshot");

    // IDs are positions in the sorted enumeration — the same indices the CDI
    // spec maps to cdev paths.
    let mut ids: Vec<u32> = resp.devices.iter().map(|d| d.id.parse().unwrap()).collect();
    ids.sort();
    assert_eq!(ids, vec![0, 1, 2, 3]);
    for d in &resp.devices {
        assert_eq!(d.health, "Healthy");
    }
    assert!(
        node.spec("kata.nvidia.com-gpu.yaml").is_some(),
        "CDI spec not written"
    );

    node.shutdown().await;
}

#[tokio::test]
async fn allocate_returns_cdi_device_names() {
    let node = Node::start(fake_vfio(2), &["nvidia.com/gpu"]).await;

    let resp = node
        .client("kata-gpu.sock")
        .await
        .allocate(Request::new(AllocateRequest {
            container_requests: vec![ContainerAllocateRequest {
                devices_i_ds: vec!["0".to_owned(), "1".to_owned()],
            }],
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.container_responses.len(), 1);
    let cdi_names: Vec<&str> = resp.container_responses[0]
        .cdi_devices
        .iter()
        .map(|d| d.name.as_str())
        .collect();
    assert_eq!(cdi_names, vec!["nvidia.com/gpu=0", "nvidia.com/gpu=1"]);
    // No DeviceSpec — device injection is the Kata shim's job, resolving CDI
    // names against the host spec, not the kubelet's via device paths.
    assert!(resp.container_responses[0].devices.is_empty());

    node.shutdown().await;
}

#[tokio::test]
async fn empty_vfio_dir_advertises_no_devices() {
    let node = Node::start(fake_vfio(0), &["nvidia.com/gpu"]).await;

    let (_stream, resp) = node.snapshot("kata-gpu.sock").await;
    assert_eq!(resp.devices.len(), 0);
    assert!(node.spec("kata.nvidia.com-gpu.yaml").is_none());

    node.shutdown().await;
}

#[tokio::test]
async fn gpu_and_nvswitch_filtered_by_pci_class() {
    // A mixed node: 2 GPUs, 3 NVSwitches, and a VFIO-bound NIC that matches
    // no RESOURCES row and must not be advertised anywhere.
    let vfio = TempDir::new().unwrap();
    for i in 0..2 {
        add_dev(vfio.path(), i, "0x10de", "0x030200");
    }
    for i in 2..5 {
        add_dev(vfio.path(), i, "0x10de", "0x068000");
    }
    add_dev(vfio.path(), 5, "0x15b3", "0x020000");

    let node = Node::start(vfio, &["nvidia.com/gpu", "nvidia.com/nvswitch"]).await;

    for (sock, expected) in [("kata-gpu.sock", 2), ("kata-nvswitch.sock", 3)] {
        let (_stream, resp) = node.snapshot(sock).await;
        assert_eq!(resp.devices.len(), expected, "{sock}");
    }

    // Each resource's CDI spec holds only its own devices.
    let gpu = node.spec("kata.nvidia.com-gpu.yaml").unwrap();
    let sw = node.spec("kata.nvidia.com-nvswitch.yaml").unwrap();
    assert!(gpu.contains("devices/vfio0") && gpu.contains("devices/vfio1"));
    assert!(sw.contains("devices/vfio2") && sw.contains("devices/vfio4"));
    assert!(!gpu.contains("devices/vfio2") && !sw.contains("devices/vfio0"));
    // The NIC appears in neither.
    assert!(!gpu.contains("devices/vfio5") && !sw.contains("devices/vfio5"));

    node.shutdown().await;
}
