use std::{collections::HashMap, hash::Hash};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};

#[derive(Clone)]
pub struct StreamType{
    stream_id: String,
    collection: HashMap<String, String>
}
impl StreamType{
    pub fn new() -> Self{
        StreamType { stream_id: String::new(), collection: HashMap::new() }
    }
    pub fn new_with_stream_id(stream_id: &str) -> Self{
        StreamType { stream_id: stream_id.to_string(), collection: HashMap::new() }
    }
    pub fn get_stream_id(&self) -> Result<String>{
        Ok(self.stream_id.clone())
    }
    pub fn set_stream_id(&mut self, stream_id: &str) -> Result<()>{
        self.stream_id = stream_id.to_string();
        Ok(())
    }
    pub fn add_to_collection(&mut self, key: &str, value: &str) -> Result<()>{
        self.collection.insert(key.to_string(), value.to_string());
        Ok(())
    }
}

#[derive(Clone)]
pub struct Store {
    collections: HashMap<String, String>,
    px_collection: HashMap<String, DateTime<Utc>>,
    stream_collections: HashMap<String, StreamType>
}
impl Store {
    pub fn new() -> Self {
        Store {
            collections: HashMap::new(),
            px_collection: HashMap::new(),
            stream_collections: HashMap::new()
        }
    }

    pub fn set_value(&mut self, key: String, value: String) -> Result<String>{
        println!("LOG_FROM_set_value --- {}:{}", key, value);
        self.collections.insert(key, value);
        Ok(String::from("OK"))
    }

    pub fn get_value(&self, key: String) -> Result<String>{
        //get in expired time collection first and check if it is expired or not
        //if it not, then get value in collection
        match self.px_collection.get::<String>(&key){
            Some(px_time) => {
                if px_time < &chrono::Utc::now(){
                    //return $-1\r\n
                    return Err(anyhow::anyhow!("Error when get_value -- key {} is expired", key));

                }
                if let Some(value) = self.collections.get::<String>(&key){
                    return Ok(value.to_owned());
                }
                else{
                    Err(anyhow::anyhow!("Error when get_value -- key {} dont has any value", key))
                }
            }
            None => {
                if let Some(value) = self.collections.get::<String>(&key){
                    return Ok(value.to_owned());
                }
                else {Err(anyhow::anyhow!("Error when get_value -- key {} dont has any value", key))} 
            }
        }
    }

    pub fn set_value_with_px(&mut self, key: String, value: String, px: String) -> Result<String>{
        let px_time: i64 = px.parse::<i64>().expect(format!("Got error when parse px time : {}", px).as_str());

        self.px_collection.insert(key.clone(), chrono::Utc::now() + Duration::milliseconds(px_time));
        self.collections.insert(key, value);
        Ok(String::from("OK"))
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

    pub fn get_stream_by_key(&mut self, stream_key: &str) -> Option<&mut StreamType> {
        self.stream_collections.get_mut(stream_key)
    }
    pub fn add_stream(&mut self, stream_key: &str, stream: StreamType) -> Result<()>{
        self.stream_collections.insert(stream_key.to_string(), stream);
        Ok(())
    }
}
