use std::path::Path;

pub use pcilibs_rs::{
    enumerate_iommufd as enumerate_all, IommufdDev, IOMMUFD_SYSFS_CLASS as SYSFS_DIR,
    IOMMUFD_VFIO_DIR as VFIO_DIR,
};

/// One advertised Kubernetes resource: an extended-resource name bound to a
/// (PCI vendor, PCI class) pair.  A resource is advertised iff matching
/// IOMMUFD devices are present.  Supporting a new device type is one row —
/// no other code changes.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Resource {
    /// Kubernetes extended-resource name; also the CDI kind.
    pub name: &'static str,
    /// PCI vendor ID, e.g. `0x10de` for NVIDIA.
    pub vendor: u16,
    /// Upper 16 bits of the 24-bit PCI class (class byte | subclass byte),
    /// e.g. `0x0302` for 3D controller.
    pub class_prefix: u16,
}

/// Everything this plugin can advertise.  Declared by the node — a resource
/// is advertised iff matching devices are VFIO-bound.
pub const RESOURCES: &[Resource] = &[
    Resource {
        name: "nvidia.com/gpu",
        vendor: 0x10de,
        class_prefix: 0x0302, // 3D controller
    },
    Resource {
        name: "nvidia.com/nvswitch",
        vendor: 0x10de,
        class_prefix: 0x0680, // bridge: other
    },
];

/// Enumerate IOMMUFD cdevs under `vfio_dir` whose PCI identity matches `res`,
/// sorted numerically.
pub fn enumerate(vfio_dir: &Path, sysfs_dir: &Path, res: &Resource) -> Vec<IommufdDev> {
    pcilibs_rs::enumerate_iommufd(vfio_dir, sysfs_dir)
        .into_iter()
        .filter(|d| d.vendor == res.vendor && d.class_prefix() == res.class_prefix)
        .collect()
}

/// NVIDIA-flavoured wrappers over pcilibs-rs's `testfs` fixtures.
#[cfg(test)]
pub(crate) mod testfs {
    use std::path::Path;

    pub use pcilibs_rs::testfs::{add, sysfs};

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
