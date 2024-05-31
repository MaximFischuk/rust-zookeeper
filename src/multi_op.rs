use std::time::Duration;

use crate::{
    proto::{
        CheckRequest, CreateRequest, CreateTTLRequest, DeleteRequest, GetDataRequest, Op,
        SetDataRequest,
    },
    Acl, CreateMode, Stat, ZkResult, ZooKeeper,
};

#[derive(Debug)]
pub enum OperationResult {
    Create(String),
    Create2(String, Stat),
    CreateTtl(String, Stat),
    SetData(Stat),
    Delete,
    Check,
}

#[derive(Debug)]
pub enum ReadOperationResult {
    GetData(Vec<u8>, Stat),
    GetChildren(Vec<String>),
}

pub struct Transaction<'a> {
    zookeeper: &'a ZooKeeper,
    operations: Vec<Op>,
}

pub struct Read<'a> {
    zookeeper: &'a ZooKeeper,
    operations: Vec<Op>,
}

impl<'a> Transaction<'a> {
    pub fn new(zookeeper: &'a ZooKeeper) -> Self {
        Self {
            zookeeper,
            operations: Vec::new(),
        }
    }

    /// See [ZooKeeper::create]
    pub fn create(mut self, path: &str, data: Vec<u8>, acl: Vec<Acl>, mode: CreateMode) -> Self {
        self.operations.push(Op::Create(CreateRequest {
            path: path.to_string(),
            data,
            acl,
            flags: mode as i32,
        }));
        self
    }

    /// See [ZooKeeper::create2]
    pub fn create2(mut self, path: &str, data: Vec<u8>, acl: Vec<Acl>, mode: CreateMode) -> Self {
        self.operations.push(Op::Create2(CreateRequest {
            path: path.to_string(),
            data,
            acl,
            flags: mode as i32,
        }));
        self
    }

    /// See [ZooKeeper::create_ttl]
    pub fn create_ttl(
        mut self,
        path: &str,
        data: Vec<u8>,
        acl: Vec<Acl>,
        mode: CreateMode,
        ttl: Duration,
    ) -> Self {
        self.operations.push(Op::CreateTtl(CreateTTLRequest {
            path: path.to_string(),
            data,
            acl,
            flags: mode as i32,
            ttl: ttl.as_millis() as i64,
        }));
        self
    }

    /// See [ZooKeeper::set_data]
    pub fn set_data(mut self, path: &str, data: Vec<u8>, version: Option<i32>) -> Self {
        self.operations.push(Op::SetData(SetDataRequest {
            path: path.to_string(),
            data,
            version: version.unwrap_or(-1),
        }));
        self
    }

    /// See [ZooKeeper::delete]
    pub fn delete(mut self, path: &str, version: Option<i32>) -> Self {
        self.operations.push(Op::Delete(DeleteRequest {
            path: path.to_string(),
            version: version.unwrap_or(-1),
        }));
        self
    }

    /// Check if the path exists and the version matches. If the version is not provided, it will
    /// check if the path exists.
    pub fn check(mut self, path: &str, version: Option<i32>) -> Self {
        self.operations.push(Op::Check(CheckRequest {
            path: path.to_string(),
            version: version.unwrap_or(-1),
        }));
        self
    }

    /// Commit the transaction
    ///
    /// # Errors
    ///
    /// If any of the operations fail, the first error will be returned.
    ///
    /// See [ZooKeeper] for more information on errors.
    /// See [crate::ZkError] for list of possible errrors.
    pub async fn commit(self) -> ZkResult<Vec<OperationResult>> {
        self.zookeeper.multi(self.operations).await
    }
}

impl<'a> Read<'a> {
    pub fn new(zookeeper: &'a ZooKeeper) -> Self {
        Self {
            zookeeper,
            operations: Vec::new(),
        }
    }
    /// See [ZooKeeper::get_data]
    pub fn get_data(mut self, path: &str, watch: bool) -> Self {
        self.operations.push(Op::GetData(GetDataRequest {
            path: path.to_string(),
            watch,
        }));
        self
    }

    /// See [ZooKeeper::get_children]
    pub fn get_children(mut self, path: &str, watch: bool) -> Self {
        self.operations.push(Op::GetChildren(GetDataRequest {
            path: path.to_string(),
            watch,
        }));
        self
    }

    /// # Errors
    ///
    /// If any of the operations fail, the first error will be returned.
    ///
    /// See [ZooKeeper] for more information on errors.
    /// See [crate::ZkError] for list of possible errrors.
    pub async fn execute(self) -> ZkResult<Vec<ReadOperationResult>> {
        self.zookeeper.multi_read(self.operations).await
    }
}
