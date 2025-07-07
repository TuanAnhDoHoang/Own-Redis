use std::collections::HashMap;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use crate::resp::{resp::unwrap_value_to_string, value::Value};

pub struct Store {
    collections: HashMap<String, String>,
    px_collection: HashMap<String, DateTime<Utc>>
}
impl Store {
    pub fn new() -> Self {
        Store {
            collections: HashMap::new(),
            px_collection: HashMap::new()
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
        //get in expired time collection first and check if it is expired or not
        //if it not, then get value in collection
        match self.px_collection.get::<String>(&key){
            Some(px_time) => {
                if px_time < &chrono::Utc::now(){
                    //return $-1\r\n
                    return Err(anyhow::anyhow!("Error when get_value -- key {} is expired", key));

                }
                if let Some(value) = self.collections.get::<String>(&key){
                    return Ok(Value::SimpleString(value.to_owned()));
                }
                else{
                    Err(anyhow::anyhow!("Error when get_value -- key {} dont has any value", key))
                }
            }
            None => {
                if let Some(value) = self.collections.get::<String>(&key){
                    return Ok(Value::SimpleString(value.to_owned()));
                }
                else {Err(anyhow::anyhow!("Error when get_value -- key {} dont has any value", key))} 
            }
        }
    }

    pub fn set_value_with_px(&mut self, key: Value, value: Value, px: Value) -> Result<()>{
        let key: String = unwrap_value_to_string(&key).expect("error when unwrap value of key");
        let value: String = unwrap_value_to_string(&value).expect("Error when unwrap value of value");
        let px: String = unwrap_value_to_string(&px).expect("Error when unwrap value of px");
        let px_time: i64 = px.parse::<i64>().expect(format!("Got error when parse px time : {}", px).as_str());

        self.px_collection.insert(key.clone(), chrono::Utc::now() + Duration::milliseconds(px_time));
        self.collections.insert(key, value);
        Ok(())
    }
}
