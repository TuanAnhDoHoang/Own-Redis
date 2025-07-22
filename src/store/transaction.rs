use std::collections::VecDeque;
use anyhow::Result;
use crate::resp::value::Value;

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
    // pub fn get_font(&mut self) -> Option<&mut Value>{
    //     Some(self.queue.get_mut(0).unwrap())
    // }
    pub fn get_font_value(&mut self) -> Option<Value>{
        let value = match self.queue.get(0){
            Some(value) => value.clone(),
            None => return None
        };
        self.queue.remove(0).unwrap();
        Some(value)
    }
    // pub fn len(&self) -> usize{
    //     self.queue.len()
    // }
}