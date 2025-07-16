use std::sync::{Arc};
use tokio::net::TcpStream;
use tokio::{sync::Mutex};
use anyhow::Result;
use tokio::io::WriteHalf;

use crate::{resp::{value::Value}};

pub struct Replication{
    role: Role,
    master_replid: String,
    master_repl_offset: usize,
    pub replication_handlers: Vec<Arc<Mutex<WriteHalf<TcpStream>>>>
}
#[derive(Clone, PartialEq, Eq)]
pub enum Role {
    Master,
    Slave,
}
impl Role {
    fn to_string(&self) -> String {
        match self {
            Role::Master => "master".to_string(),
            Role::Slave => "slave".to_string(),
        }
    }
}

impl Replication  {
    pub fn new() -> Self {
        Replication {
            role: Role::Master,
            master_replid: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"), //random alphanumeric 40 characters
            master_repl_offset: 0,
            replication_handlers: Vec::new()
        }
    }
    pub fn set_role(&mut self, role: Role) -> Result<()> {
        self.role = role;
        Ok(())
    }
    // pub fn get_role(&self) -> Result<Role> {
    //     Ok(self.role.clone())
    // }
    pub fn display_to_value(&self) -> Result<Value> {
        Ok(Value::BulkString(format!(
            "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
            self.role.to_string(),
            self.master_replid,
            self.master_repl_offset
        )))
    }
    pub fn get_master_replid(&self) -> Result<String> {
        Ok(self.master_replid.clone())
    }
    pub fn add_repl_handler(&mut self, handler: Arc<Mutex<WriteHalf<TcpStream>>>) -> Result<()>{
        // println!("Say hi from add_repl_handler --- handler: {:#?}", handler);
        self.replication_handlers.push(handler);
        Ok(())
    }
}
