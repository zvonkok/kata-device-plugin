use std::path::{Path, PathBuf};

/// Where the kernel exposes VFIO; IOMMUFD cdevs live under
/// `/dev/vfio/devices/`.  Not configurable — the kernel decides.
/// Tests inject a temp dir via `DeviceServer::new` instead.
pub const VFIO_DIR: &str = "/dev/vfio";

/// Sysfs class directory for VFIO cdevs; `<SYSFS_DIR>/vfio<N>/device`
/// links to the PCI device, whose `vendor` and `class` files identify
/// what is behind the cdev.  A kernel contract, like `VFIO_DIR`.
pub const SYSFS_DIR: &str = "/sys/class/vfio-dev";

/// One advertised resource: a Kubernetes extended-resource name declared by
/// a PCI (vendor, class prefix) match against `/sys/class/vfio-dev`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Resource {
    /// Kubernetes extended-resource name; also the CDI kind.
    pub name: &'static str,
    /// PCI vendor id as sysfs prints it, e.g. "0x10de".
    pub vendor: &'static str,
    /// PCI class code prefix as sysfs prints it, e.g. "0x0302".
    pub class_prefix: &'static str,
}

/// Everything this plugin can advertise.  A resource is advertised iff
/// matching devices are VFIO-bound — declared by the node, not configured.
/// Supporting a new device type is one new row; no other code changes.
pub const RESOURCES: &[Resource] = &[
    Resource {
        name: "nvidia.com/gpu",
        vendor: "0x10de",
        class_prefix: "0x0302", // 3D controller
    },
    Resource {
        name: "nvidia.com/nvswitch",
        vendor: "0x10de",
        class_prefix: "0x0680", // bridge: other
    },
];

/// One IOMMUFD cdev: `/dev/vfio/devices/vfio<num>`.
#[derive(Clone, Debug)]
pub struct VfioDev {
    pub num: u32,
    pub path: PathBuf,
}

/// Enumerate IOMMUFD cdevs under `<vfio_dir>/devices/` whose PCI identity
/// matches `res`, sorted numerically.  Positions in the returned Vec are the
/// advertised device IDs / CDI spec indices.
pub fn enumerate(vfio_dir: &Path, sysfs_dir: &Path, res: &Resource) -> Vec<VfioDev> {
    let devices_dir = vfio_dir.join("devices");
    let Ok(rd) = std::fs::read_dir(&devices_dir) else {
        return vec![];
    };
    let mut nums: Vec<u32> = rd
        .flatten()
        .filter_map(|e| {
            e.file_name()
                .to_str()?
                .strip_prefix("vfio")?
                .parse::<u32>()
                .ok()
        })
        .filter(|&n| matches(sysfs_dir, n, res))
        .collect();
    nums.sort();
    nums.into_iter()
        .map(|num| VfioDev {
            num,
            path: devices_dir.join(format!("vfio{num}")),
        })
        .collect()
}

fn matches(sysfs_dir: &Path, num: u32, res: &Resource) -> bool {
    let device = sysfs_dir.join(format!("vfio{num}")).join("device");
    let read = |name: &str| std::fs::read_to_string(device.join(name)).unwrap_or_default();
    read("vendor").trim() == res.vendor && read("class").trim().starts_with(res.class_prefix)
}

/// Fake node layout for unit tests, under one root:
///   `<root>/devices/vfio<n>`                       — the cdev entry
///   `<root>/sysfs/vfio<n>/device/{vendor,class}`   — fake sysfs
#[cfg(test)]
pub(crate) mod testfs {
    use std::path::{Path, PathBuf};

    pub fn sysfs(root: &Path) -> PathBuf {
        root.join("sysfs")
    }

    pub fn add(root: &Path, n: u32, vendor: &str, class: &str) {
        let devices = root.join("devices");
        std::fs::create_dir_all(&devices).unwrap();
        std::fs::write(devices.join(format!("vfio{n}")), b"").unwrap();
        let device = sysfs(root).join(format!("vfio{n}")).join("device");
        std::fs::create_dir_all(&device).unwrap();
        std::fs::write(device.join("vendor"), format!("{vendor}\n")).unwrap();
        std::fs::write(device.join("class"), format!("{class}\n")).unwrap();
    }

    pub fn add_gpu(root: &Path, n: u32) {
        add(root, n, "0x10de", "0x030200");
    }

    pub fn add_nvswitch(root: &Path, n: u32) {
        add(root, n, "0x10de", "0x068000");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn by_name(name: &str) -> &'static Resource {
        RESOURCES.iter().find(|r| r.name == name).unwrap()
    }

    #[test]
    fn classifies_and_sorts() {
        let root = TempDir::new().unwrap();
        testfs::add_gpu(root.path(), 42);
        testfs::add_gpu(root.path(), 7);
        testfs::add_nvswitch(root.path(), 9);
        // A VFIO-bound NIC must not be advertised under any resource.
        testfs::add(root.path(), 3, "0x15b3", "0x020000");

        let sysfs = testfs::sysfs(root.path());
        let gpus = enumerate(root.path(), &sysfs, by_name("nvidia.com/gpu"));
        assert_eq!(gpus.iter().map(|d| d.num).collect::<Vec<_>>(), vec![7, 42]);
        assert!(gpus[0].path.ends_with("devices/vfio7"));

        let switches = enumerate(root.path(), &sysfs, by_name("nvidia.com/nvswitch"));
        assert_eq!(switches.iter().map(|d| d.num).collect::<Vec<_>>(), vec![9]);
    }

    #[test]
    fn missing_sysfs_entry_is_not_advertised() {
        let root = TempDir::new().unwrap();
        let devices = root.path().join("devices");
        std::fs::create_dir_all(&devices).unwrap();
        std::fs::write(devices.join("vfio0"), b"").unwrap();

        for res in RESOURCES {
            assert!(enumerate(root.path(), &testfs::sysfs(root.path()), res).is_empty());
        }
    }

    #[test]
    fn missing_devices_dir_is_empty() {
        let root = TempDir::new().unwrap();
        for res in RESOURCES {
            assert!(enumerate(root.path(), &testfs::sysfs(root.path()), res).is_empty());
        }
    }
}
