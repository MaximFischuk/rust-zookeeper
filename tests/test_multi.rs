use crate::test::ZkCluster;
use std::time::Duration;
use zookeeper_async::{Acl, CreateMode, ZkError, ZooKeeper};

mod test;

async fn create_zk(connection_string: &str) -> ZooKeeper {
    ZooKeeper::connect(connection_string, Duration::from_secs(10), |_ev| {})
        .await
        .unwrap()
}

#[tokio::test]
async fn zk_multi() {
    // Create a test cluster
    let cluster = ZkCluster::start(3);

    // Connect to the test cluster
    let zk = create_zk(&cluster.connect_string).await;

    let results = zk
        .transaction()
        .create(
            "/test",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .create(
            "/test/child1",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .check("/test", Some(0))
        .commit()
        .await;

    assert!(results.is_ok());
    let results = results.unwrap();
    assert_eq!(results.len(), 3);

    assert!(zk.exists("/test", false).await.unwrap().is_some());
    assert!(zk.exists("/test/child1", false).await.unwrap().is_some());

    // After closing the client all operations return Err
    zk.close().await.unwrap();
}

#[tokio::test]
async fn zk_multi_w_set_data() {
    // Create a test cluster
    let cluster = ZkCluster::start(3);

    // Connect to the test cluster
    let zk = create_zk(&cluster.connect_string).await;

    let results = zk
        .transaction()
        .create2(
            "/test",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .create2(
            "/test/child1",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .set_data("/test", vec![1, 2, 3], None)
        .check("/test", Some(1))
        .commit()
        .await;

    assert!(results.is_ok());
    let results = results.unwrap();
    assert_eq!(results.len(), 4);

    assert!(zk.exists("/test", false).await.unwrap().is_some());
    assert!(zk.exists("/test/child1", false).await.unwrap().is_some());

    let data = zk.get_data("/test", false).await.unwrap();
    assert_eq!(data.0, vec![1, 2, 3]);

    // After closing the client all operations return Err
    zk.close().await.unwrap();
}

#[tokio::test]
async fn zk_multi_w_delete() {
    // Create a test cluster
    let cluster = ZkCluster::start(3);

    // Connect to the test cluster
    let zk = create_zk(&cluster.connect_string).await;

    let results = zk
        .transaction()
        .create(
            "/test",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .create(
            "/test/child1",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .delete("/test/child1", None)
        .commit()
        .await;

    assert!(results.is_ok());
    let results = results.unwrap();
    assert_eq!(results.len(), 3);

    assert!(zk.exists("/test", false).await.unwrap().is_some());
    assert!(zk.exists("/test/child1", false).await.unwrap().is_none());

    // After closing the client all operations return Err
    zk.close().await.unwrap();
}

#[tokio::test]
async fn zk_multi_error() {
    // Create a test cluster
    let cluster = ZkCluster::start(3);

    // Connect to the test cluster
    let zk = create_zk(&cluster.connect_string).await;

    let results = zk
        .transaction()
        .create(
            "/test",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .check("/test", Some(2)) // This should fail because the version is wrong
        .commit()
        .await;

    let Err(error) = results else {
        panic!("Expected an error");
    };

    assert_eq!(error, ZkError::BadVersion);

    let results = zk
        .transaction()
        .create(
            "/test",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .check("/test-wrong", None)
        .commit()
        .await;

    let Err(error) = results else {
        panic!("Expected an error");
    };

    assert_eq!(error, ZkError::NoNode);

    let results = zk
        .transaction()
        .create(
            "/test",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .create(
            "/test",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .commit()
        .await;

    let Err(error) = results else {
        panic!("Expected an error");
    };

    assert_eq!(error, ZkError::NodeExists);

    // Ensure that the transaction was not committed

    assert!(zk.exists("/test", false).await.unwrap().is_none());

    // After closing the client all operations return Err
    zk.close().await.unwrap();
}

#[tokio::test]
async fn zk_multi_read() {
    // Create a test cluster
    let cluster = ZkCluster::start(3);

    // Connect to the test cluster
    let zk = create_zk(&cluster.connect_string).await;

    let results = zk
        .transaction()
        .create(
            "/test",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .create(
            "/test/child1",
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .commit()
        .await;

    assert!(results.is_ok());
    let results = results.unwrap();
    assert_eq!(results.len(), 2);

    let results = zk
        .read()
        .get_data("/test", false)
        .get_children("/test", false)
        .execute()
        .await;

    // assert!(results.is_ok());
    let results = results.unwrap();
    assert_eq!(results.len(), 2);

    assert!(zk.exists("/test", false).await.unwrap().is_some());
    assert!(zk.exists("/test/child1", false).await.unwrap().is_some());

    // After closing the client all operations return Err
    zk.close().await.unwrap();
}

#[tokio::test]
async fn zk_multi_read_error() {
    // Create a test cluster
    let cluster = ZkCluster::start(3);

    // Connect to the test cluster
    let zk = create_zk(&cluster.connect_string).await;

    let results = zk.read().get_data("/test", false).execute().await;

    let Err(error) = results else {
        panic!("Expected an error");
    };

    assert_eq!(error, ZkError::NoNode);

    // After closing the client all operations return Err
    zk.close().await.unwrap();
}
