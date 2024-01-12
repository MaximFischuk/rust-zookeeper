use crate::test::ZkCluster;
use std::time::Duration;
use tracing::info;
use zookeeper_async::{Acl, CreateMode, WatchedEvent, Watcher, ZooKeeper};

mod test;

struct LogWatcher;

impl Watcher for LogWatcher {
    fn handle(&self, event: WatchedEvent) {
        info!("{:?}", event);
    }
}

async fn create_zk(connection_string: &str) -> ZooKeeper {
    ZooKeeper::connect(connection_string, Duration::from_secs(10), LogWatcher)
        .await
        .unwrap()
}

#[tokio::test]
async fn zk_get_all_children_number_test() {
    // Create a test cluster
    let mut cluster = ZkCluster::start(3);

    // Connect to the test cluster
    let zk = create_zk(&cluster.connect_string).await;

    // Do the tests
    let _ = zk
        .create(
            "/test",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .await;
    // create few children of /test
    let _ = zk
        .create(
            "/test/child1",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .await;
    let _ = zk
        .create(
            "/test/child2",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .await;
    let _ = zk
        .create(
            "/test/child3",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .await;

    let children = zk.get_all_children_number("/test").await;

    assert_eq!(
        children,
        Ok(3),
        "get_all_children_number failed: {:?}",
        children
    );

    cluster.kill_an_instance();

    // After closing the client all operations return Err
    zk.close().await.unwrap();
}

#[tokio::test]
async fn zk_get_all_children_number_with_subtree_test() {
    // Create a test cluster
    let mut cluster = ZkCluster::start(3);

    // Connect to the test cluster
    let zk = create_zk(&cluster.connect_string).await;

    // Do the tests
    let _ = zk
        .create(
            "/test",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .await;
    // create few children of /test
    let _ = zk
        .create(
            "/test/child1",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .await;
    let _ = zk
        .create(
            "/test/child2",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .await;
    let _ = zk
        .create(
            "/test/child2/child21",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .await;
    let _ = zk
        .create(
            "/test/child3",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .await;
    let _ = zk
        .create(
            "/test/child3/child31",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .await;
    let _ = zk
        .create(
            "/test/child3/child31/child311",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .await;

    let children = zk.get_all_children_number("/test").await;

    assert_eq!(
        children,
        Ok(6),
        "get_all_children_number failed: {:?}",
        children
    );

    cluster.kill_an_instance();

    // After closing the client all operations return Err
    zk.close().await.unwrap();
}
