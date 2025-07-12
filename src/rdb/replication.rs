use std::sync::{Arc};
use tokio::sync::{Mutex};

use anyhow::Result;

use crate::{resp::{resp::RespHandler, value::Value}};

pub struct Replication {
    role: Role,
    master_replid: String,
    master_repl_offset: usize,
    replication_endpoint: Vec<String>,
    pub replication_handlers: Vec<Arc<Mutex<RespHandler>>>
}
#[derive(Clone, PartialEq, Eq)]
pub enum Role {
    master,
    slave,
}
impl Role {
    fn to_string(&self) -> String {
        match self {
            Role::master => "master".to_string(),
            Role::slave => "slave".to_string(),
            _ => "nothing".to_string(),
        }
    }
}

impl Replication {
    pub fn new() -> Self {
        Replication {
            role: Role::master,
            master_replid: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"), //random alphanumeric 40 characters
            master_repl_offset: 0,
            replication_endpoint: Vec::new(),
            replication_handlers: Vec::new()
        }
    }
    pub fn set_role(&mut self, role: Role) -> Result<()> {
        self.role = role;
        Ok(())
    }
    pub fn get_role(&self) -> Result<Role> {
        Ok(self.role.clone())
    }
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
    pub fn get_all_repl_endpoint(&self) -> Result<Vec<(String, usize)>> {
        let result = self
            .replication_endpoint
            .iter()
            .filter_map(|endpoint| {
                let mut e = endpoint.split(':');
                let address = e.next()?.to_string();
                let port = e.next()?.parse::<usize>().ok()?;
                Some((address, port))
            })
            .collect::<Vec<(String, usize)>>();
        Ok(result)
    }
    pub fn add_repl_endpoint(&mut self, address: String, port: usize) -> Result<()> {
        println!("HI from add repl_endpoint -- address: {} port {}", address, port);
        //check valid endpoint
        self.replication_endpoint
            .push(format!("{}:{}", address, port));
        Ok(())
    }
    pub fn add_repl_handler(&mut self, handler: Arc<Mutex<RespHandler>>) -> Result<()>{
        // println!("Say hi from add_repl_handler --- handler: {:#?}", handler);
        self.replication_handlers.push(handler);
        Ok(())
    }
}
pub async fn propagation(replication: Arc<Mutex<Replication>>, key: String, value: String) {
    println!("Say hi from propagation");
    let mut replication = replication.lock().await;
    println!("LOG_FROM_propagation --- handlers: {:?}", replication.replication_handlers);
    for handler in &mut replication.replication_handlers {
        let payload = Value::Array(vec![
            Value::BulkString("SET".to_string()),
            Value::BulkString(key.clone()),
            Value::BulkString(value.clone()),
        ]);
        let mut handler = handler.lock().await;
        
        handler.write_value(Value::serialize(&payload)).await;
    }
}
