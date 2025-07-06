use std::collections::HashMap;
use anyhow::Result;
use crate::resp::{resp::unwrap_value_to_string, value::Value};

pub struct Store {
    collections: HashMap<String, String>,
}
impl Store {
    pub fn new() -> Self {
        Store {
            collections: HashMap::new(),
        }
    }

    pub fn set_value(&mut self, key: Value, value: Value) -> Result<Value>{
        let key = unwrap_value_to_string(&key).expect("error when unwrap value of key");
        let value = unwrap_value_to_string(&value).expect("Error when unwrap value of value");
        self.collections.insert(key, value);
        Ok(Value::SimpleString("OK".to_string()))
    }

    pub fn get_value(&self, key: Value) -> Result<Value>{
        let key = unwrap_value_to_string(&key).expect("error when unwrap value of key");
        match self.collections.get::<String>(&key){
            Some(value ) => Ok(Value::SimpleString(value.to_owned())),
            None => Err(anyhow::anyhow!("Error when get_value -- key {} dont has any value", key))
        }
    }
}
