use crate::{resp::value::Value};
use anyhow::Result;
use std::{collections::VecDeque};

#[derive(Clone)]
pub struct Transaction {
    queue: VecDeque<Value>,
}
impl Transaction {
    pub fn new() -> Self {
        Transaction {
            queue: VecDeque::new(),
        }
    }
    pub fn push_back(&mut self, value: &Value) -> Result<()> {
        self.queue.push_back(value.clone());
        Ok(())
    }
    pub fn get_font(&mut self) -> Option<&mut Value> {
        self.queue.get_mut(0)
    }
    pub fn get_font_value(&mut self) -> Option<Value> {
        let value = match self.queue.get(0) {
            Some(value) => value.clone(),
            None => return None,
        };
        self.queue.remove(0).unwrap();
        Some(value)
    }
    pub fn len(&self) -> usize {
        self.queue.len()
    }
    pub fn put(&mut self, command: &str, command_content: &mut Vec<Value>) -> Result<Option<Value>>{
        if let Some(value) = self.get_font() {
            if value == &Value::BulkString("MULTI".to_string()) {
                let mut full_cmd = vec![Value::BulkString(command.to_string())];
                full_cmd.append(command_content);
                self
                    .push_back(&Value::Array(full_cmd))
                    .unwrap();
                return Ok(Some(Value::SimpleString("QUEUED".to_string())));
            }
            //if it has no multi, then return none to execute agular
            return Ok(None);
        }
        else{ Ok(None) }
    }
}