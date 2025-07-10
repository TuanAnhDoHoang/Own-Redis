use anyhow::Result;

use crate::resp::value::{self, Value};

#[derive(Clone)]
pub struct Replication {
    role: Role,
}
#[derive(Clone)]
pub enum Role {
    master,
    slave,
    none,
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
        Replication { role: Role::master }
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
        Ok(Value::BulkString(format!("role:{}", self.role.to_string())))
    }
}
