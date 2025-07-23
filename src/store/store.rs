use crate::store::{entry::Entry};
use anyhow::{Result};
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub enum StoreValueType {
    String(String),
    Interger(i64),
    List(Vec<String>)
}
impl StoreValueType {
    pub fn to_string(&self) -> String {
        match self {
            StoreValueType::String(s) => s.clone(),
            StoreValueType::Interger(i) => i.to_string(),
            _ => "".to_string()
        }
    }
}

#[derive(Clone)]
pub struct Store {
    collections: HashMap<String, (StoreValueType, Option<DateTime<Utc>>)>,
    pub entry: Entry
}
impl Store {
    pub fn new() -> Self {
        Store {
            collections: HashMap::new(),
            entry: Entry::new(),
            // transaction: Transaction::new()
        }
    }

    pub fn set_value(&mut self, key: &str, value: &str, px: Option<&str>) -> Result<String> {
        let px = match px {
            Some(px_time) => {
                Some(chrono::Utc::now() + Duration::milliseconds(px_time.parse::<i64>().unwrap()))
            }
            None => None,
        };
        let value = match value.parse::<i64>() {
            Ok(value) => StoreValueType::Interger(value),
            _ => StoreValueType::String(value.to_string()),
        };
        println!("LOG_FROM_set_value value and px: {:?}:{:?}", value, px);
        self.collections.insert(key.to_string(), (value, px));
        Ok(String::from("OK"))
    }

    pub fn get_value(&self, key: &str) -> Result<StoreValueType> {
        let (value, px_time) = if let Some(values) = self.collections.get(key) {
            values
        } else {
            return Err(anyhow::anyhow!(
                "Error when get_value -- key {} dont has any value",
                key
            ));
        };
        if px_time.is_none() || (px_time.unwrap() >= chrono::Utc::now()) {
            Ok(value.to_owned())
        } else {
            return Err(anyhow::anyhow!(
                "Error when get_value -- key {} dont has any value",
                key
            ));
        }
    }
    pub fn increase(&mut self, key: &str) -> Result<()> {
        let value = match self.collections.get_mut(key){
            Some(value) => &mut value.0,
            None => {
                self.set_value(key, "0", None).unwrap();
                &mut self.collections.get_mut(key).unwrap().0
            }
        };
        match value {
            StoreValueType::Interger(num) => {
                *num += 1i64;
                Ok(())
            }
            _ => Err(anyhow::anyhow!("Value of key {} is not interger", key)),
        }
    }

    pub fn push(&mut self, key: &str, value: &str) -> Result<usize> {
        if self.collections.get(key).is_none(){
            self.collections.insert(key.to_string(), (StoreValueType::List(Vec::new()), None));
        }
        let (list, _) = self.collections.get_mut(key).unwrap();
        match list{
            StoreValueType::List(list) => {
                list.push(value.to_string());
                Ok(list.len())
            }
            _ => {
                Err(anyhow::anyhow!("Key {} has no type list anymore", key))
            }
        }
    }
    pub fn get_list_size(&self, key: &str) -> Result<usize>{
        let (list, _) = self.collections.get(key).unwrap();
        match list{
            StoreValueType::List(list) => {Ok(list.len())}
            _ => {Ok(0)}
        }
    }
    pub fn get_list_range(&self, key: &str, mut start: i64, mut end: i64) -> Result<Vec<String>> { 
        if let Some((list,_)) = self.collections.get(key){
            match list{
                StoreValueType::List(list) => {
                    if end > list.len() as i64{ end = list.len() as i64 - 1;}

                    if start < 0 { start = list.len() as i64 + start;}
                    if end < 0 { end = list.len() as i64 + end;}

                    if start >= 0 && end >= 0{
                        if end >= start{
                            Ok(list[start as usize..=end as usize].to_vec())
                        }
                        else{
                            Ok(Vec::new())
                        }
                    }
                    else{
                        Ok(Vec::new())
                    }
                }
                _ => {Ok(Vec::new())}
            }
        }
        else{ Ok(Vec::new())}
    }
    // pub fn get_all(&self) -> Result<Vec<(String, String)>>{
    //     let mut result = Vec::new();
    //     for (key, value) in self.collections.iter() {
    //         result.push((key.clone(), value.clone()));
    //     }
    //     // self.px_collection.iter().map(|(key, value)| if result.push((key.clone(), value.clone())));
    //     println!("LOG_FROM_store --- collection : {:?}", result);
    //     Ok(result)
    // }
}
