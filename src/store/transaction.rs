use std::{collections::VecDeque, sync::Arc};
use anyhow::Result;
use tokio::sync::Mutex;
use crate::{resp::value::Value, store::store::Store};

#[derive(Clone)]
pub struct Transaction{
    queue: VecDeque<Value>
}
impl Transaction{
    pub fn new() -> Self{
        Transaction { queue: VecDeque::new() }
    }
    pub fn push_back(&mut self, value: &Value) -> Result<()>{
        self.queue.push_back(value.clone());
        Ok(())
    }
    pub fn get_font(&mut self) -> Option<&mut Value>{
        self.queue.get_mut(0)
    }
    pub fn get_font_value(&mut self) -> Option<Value>{
        let value = match self.queue.get(0){
            Some(value) => value.clone(),
            None => return None
        };
        self.queue.remove(0).unwrap();
        Some(value)
    }
    pub fn len(&self) -> usize{
        self.queue.len()
    }
}

pub async fn put(command: &str, command_content: &mut Vec<Value>, storage: Arc<Mutex<Store>>) -> Result<Option<Value>> {
    let mut storage = storage.lock().await;
    if let Some(value) = storage.transaction.get_font(){
        if value ==  &Value::BulkString("MULTI".to_string()){
            let mut full_cmd = vec![Value::BulkString(command.to_string())];
            full_cmd.append(command_content);
            storage.transaction.push_back(&Value::Array(full_cmd)).unwrap();
            return Ok(Some(Value::SimpleString("QUEUED".to_string())));
        }
        //if it has no multi, then return none to execute agular
        return Ok(None);
    }
    else{
        return Ok(None);
    }
}