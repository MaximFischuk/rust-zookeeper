use std::{env, time::Duration};

use zookeeper_async::{WatchedEvent, Watcher, ZooKeeper};

struct LoggingWatcher;
impl Watcher for LoggingWatcher {
    fn handle(&self, e: WatchedEvent) {
        println!("{:?}", e)
    }
}

#[tokio::main]
async fn main() {
    let zk_urls = zk_server_urls();
    println!("connecting to {}", zk_urls);

    let zk = ZooKeeper::connect(&zk_urls, Duration::from_secs(15), LoggingWatcher)
        .await
        .unwrap();

    // Create transaction that creates a node and a child node
    let results = zk
        .transaction()
        .create(
            "/test",
            vec![],
            zookeeper_async::Acl::open_unsafe().clone(),
            zookeeper_async::CreateMode::Persistent,
        )
        .create(
            "/test/child1",
            vec![],
            zookeeper_async::Acl::open_unsafe().clone(),
            zookeeper_async::CreateMode::Persistent,
        )
        // Check that the node exists
        .check("/test", None)
        .commit()
        .await
        .unwrap();

    for result in results {
        println!("{:?}", result);
    }

    // Create transaction that sets data on a node
    let results = zk
        .transaction()
        .create(
            "/test2",
            vec![],
            zookeeper_async::Acl::open_unsafe().clone(),
            zookeeper_async::CreateMode::Persistent,
        )
        .set_data("/test2", vec![1, 2, 3], None)
        .create(
            "/test2/child1",
            vec![],
            zookeeper_async::Acl::open_unsafe().clone(),
            zookeeper_async::CreateMode::Persistent,
        )
        .set_data("/test2/child1", vec![4, 5, 6], None)
        .commit()
        .await
        .unwrap();

    for result in results {
        println!("{:?}", result);
    }

    // Read the data from the node
    let results = zk
        .read()
        .get_data("/test2", false)
        .get_data("/test2/child1", false)
        .execute()
        .await
        .unwrap();

    for result in results {
        println!("{:?}", result);
    }

    // Delete all nodes
    let results = zk
        .transaction()
        .delete("/test/child1", None)
        .delete("/test", None)
        .delete("/test2/child1", None)
        .delete("/test2", None)
        .commit()
        .await
        .unwrap();

    for result in results {
        println!("{:?}", result);
    }
}

fn zk_server_urls() -> String {
    let key = "ZOOKEEPER_SERVERS";
    match env::var(key) {
        Ok(val) => val,
        Err(_) => "localhost:2181".to_string(),
    }
}
