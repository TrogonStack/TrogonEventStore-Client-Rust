use std::{fs, path::Path};

use testcontainers::{Image, core::MountType};

use crate::images::EventStoreDB;

fn fixture_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
}

#[test]
fn cluster_uses_supported_server_options() {
    let compose = fs::read_to_string(fixture_root().join("docker-compose.yml")).unwrap();
    let shared = fs::read_to_string(fixture_root().join("vars.env")).unwrap();

    for option in [
        "EVENTSTORE_REPLICATION_IP=",
        "EVENTSTORE_ADVERTISE_NODE_PORT_TO_CLIENT_AS=",
    ] {
        assert!(compose.contains(option), "missing {option}");
    }

    for option in ["EVENTSTORE_REPLICATION_PORT=", "EVENTSTORE_NODE_PORT="] {
        assert!(shared.contains(option), "missing {option}");
    }

    for option in [
        "EVENTSTORE_INT_IP=",
        "EVENTSTORE_INT_TCP_PORT=",
        "EVENTSTORE_HTTP_PORT=",
        "EVENTSTORE_ADVERTISE_HTTP_PORT_TO_CLIENT_AS=",
        "EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP=",
    ] {
        assert!(!compose.contains(option), "obsolete {option}");
        assert!(!shared.contains(option), "obsolete {option}");
    }
}

#[test]
fn invalid_root_certificate_uses_generated_untrusted_ca() {
    let generator = fs::read_to_string(fixture_root().join("configure-tls-for-tests.yml")).unwrap();
    let test = fs::read_to_string(
        fixture_root().join("trogon-eventstore/tests/misc/root_certificates.rs"),
    )
    .unwrap();

    assert!(generator.contains("create-ca -out ./untrusted-ca"));
    assert!(test.contains("certs/untrusted-ca/ca.crt"));
}

#[test]
fn database_directory_uses_named_volume_mount() {
    let image = EventStoreDB::default().attach_volume_to_db_directory("fixture-volume".into());
    let mount = image.mounts().into_iter().next().expect("database mount");

    assert!(matches!(mount.mount_type(), MountType::Volume));
    assert_eq!(mount.source(), Some("fixture-volume"));
    assert_eq!(mount.target(), Some("/var/lib/eventstore"));
}
