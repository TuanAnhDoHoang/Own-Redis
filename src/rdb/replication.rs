use anyhow::Result;

use crate::resp::value::{Value};

#[derive(Clone)]
pub struct Replication {
    role: Role,
    master_replid: String,
    master_repl_offset: usize
}
#[derive(Clone)]
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
    pub fn new() -> Self{
        Replication { 
            role: Role::master,
            master_replid: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"), //random alphanumeric 40 characters
            master_repl_offset: 0
        }
    }
    pub fn set_role(&mut self, role: Role) -> Result<()> {
        self.role = role;
        Ok(())
    }
    pub fn display_to_value(&self) -> Result<Value> {
        // Ok(Value::Array(vec![Value::BulkString(format!(
        //     "role:{}",
        //     self.role.to_string()
        // ))]))
        Ok(Value::BulkString(
            format!(
                "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}", 
                self.role.to_string(),
                self.master_replid,
                self.master_repl_offset
            )
        ))
    }
}
