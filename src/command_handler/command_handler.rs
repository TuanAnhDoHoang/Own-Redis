use crate::rdb::parse_rdb::RdbFile;
use crate::rdb::argument::Argument;
use crate::rdb::replication::{Replication};
use crate::store::store::StreamType;
use crate::unwrap_value_to_string;
use crate::{resp::value::Value, store::store::Store};
use anyhow::{Result};
use std::collections::HashMap;
use tokio::sync::{Mutex};
use std::sync::{Arc};

pub async fn command_handler (
    command: String,
    command_content: Vec<Value>,
    storage: &mut Store,
    rdb_argument: &mut Argument,
    rdb_file: &mut RdbFile,
    replication: Arc<Mutex<Replication>> 
) -> Value {
    match command.as_str() {
        "PING" => handle_ping().expect("Error when handle PING"),
        "ECHO" => handle_echo(command_content).expect("Error when handle ECHO"),
        "SET" => {
            handle_set(command_content, storage).await.expect("Error when handle SET")
        }
        "GET" => handle_get(command_content, storage, rdb_file).expect("Error when handle GET"),
        "CONFIG" => {
            handle_config(command_content, rdb_argument).expect("Error when handle CONFIG")
        }
        "KEYS" => handle_key(command_content, rdb_file).expect("Error when handle KEY"),
        "INFO" => handle_info(command_content, replication).await.expect("Error when handle KEY"),
        "REPLCONF" => handle_replconf().expect("Error when handle replconf"),
        "PSYNC" => handle_psync(replication).await.expect("Error when handle psync"),
        "TYPE" => handle_type(command_content, storage).expect("Error when handle type"),
        "XADD" => handle_xadd(command_content, storage).expect("Error when handle xadd"),
        c => {
            eprintln!("Invalid command: {}", c);
            Value::NullBulkString
        }
    }
}
pub fn handle_ping() -> Result<Value> {
    Ok(Value::SimpleString("PONG".to_string()))
}
pub fn handle_echo(command_content: Vec<Value>) -> Result<Value> {
    Ok(command_content.get(0).unwrap().clone())
}
pub async fn handle_set(
    command_content: Vec<Value>,
    storage: &mut Store,
) -> Result<Value> {
    let key = command_content.get(0).unwrap().clone();
    let value = command_content.get(1).unwrap().clone();
    let key = unwrap_value_to_string(&key).expect(&format!(
        "Get error when unwrap Value key {:?} to string",
        key
    ));
    let value = unwrap_value_to_string(&value).expect(&format!(
        "Get error when unwrap Value value {:?} to string",
        value
    ));

    match command_content.get(2) {
        //"PX" "Px" "px" "pX" //pattern regrex
        Some(px_command) => {
            if px_command == &Value::BulkString("px".to_string())
                || px_command == &Value::BulkString("Px".to_string())
                || px_command == &Value::BulkString("pX".to_string())
                || px_command == &Value::BulkString("PX".to_string())
            {
                if let Some(px) = command_content.get(3) {
                    let px = unwrap_value_to_string(&px).expect(&format!(
                        "Get error when unwrap Value px {:?} to string",
                        px
                    ));
                    match storage.set_value_with_px(key.clone(), value.clone(), px.clone()) {
                        Ok(value) => Ok(Value::SimpleString(value)),
                        Err(_) => Ok(Value::NullBulkString),
                    }
                } else {
                    Ok(Value::NullBulkString)
                }
            } else {
                Ok(Value::NullBulkString)
            }
        }
        None => Ok(Value::SimpleString(
            storage.set_value(key.clone(), value.clone()).unwrap(),
        )),
    }
}
pub fn handle_get(command_content: Vec<Value>, storage: &mut Store, rdb_file: &mut RdbFile) -> Result<Value> {
    let key = command_content.get(0).unwrap().clone();
    let key = unwrap_value_to_string(&key).expect(&format!(
        "Get error when unwrap Value key {:?} to string",
        key
    ));
    let rdb_file_collections: HashMap<String, String> = rdb_file.map.iter().map(|(key, entry)| {
        if let Some(px) = entry.1 {
            if px > chrono::Utc::now(){ return (key.to_owned(), entry.0.clone()); }
            else{ ("".to_string(), "".to_string())}
        }
        else { return (key.to_owned(), entry.0.clone()); }
    }).collect::<HashMap<String, String>>();

    match storage.get_value(key.clone()) {
        Ok(value) => Ok(Value::BulkString(value)),
        Err(_) => {
            if let Some(value) = rdb_file_collections.get(&key){ Ok(Value::BulkString(value.to_owned()))}
            else {Ok(Value::NullBulkString)}
        },
    }
}
pub fn handle_config(command_content: Vec<Value>, rdb_argument: &mut Argument) -> Result<Value> {
    match command_content.get(0) {
        Some(value) => match unwrap_value_to_string(value).unwrap().as_str() {
            "GET" => {
                if let Some(name) = command_content.get(1) {
                    if name == &Value::BulkString("dir".to_string()) {
                        Ok(Value::Array(vec![
                            Value::BulkString("dir".to_string()),
                            Value::BulkString(rdb_argument.get_dir().unwrap()),
                        ]))
                    } else if name == &Value::BulkString("dbfilename".to_string()) {
                        Ok(Value::Array(vec![
                            Value::BulkString("dbfilename".to_string()),
                            Value::BulkString(rdb_argument.get_dir_file_name().unwrap()),
                        ]))
                    } else {
                        Ok(Value::NullBulkString)
                    }
                } else {
                    Ok(Value::NullBulkString)
                }
            }
            // "SET" => {}
            _ => Ok(Value::NullBulkString),
        },
        None => Ok(Value::NullBulkString),
    }
}
pub fn handle_key(command_content: Vec<Value>, rdb_file: &mut RdbFile) -> Result<Value> {
    let pattern: Value = command_content.get(0).unwrap().clone();
    let pattern: String = unwrap_value_to_string(&pattern).expect(&format!(
        "Get error when unwrap Value pattern {:?} to string",
        pattern
    ));
    match pattern.as_str() {
        "*" => {
            let keys = rdb_file
                .map
                .iter()
                .map(|(key, entry)| {
                    if let Some(px) = entry.1 {
                        if px > chrono::Utc::now() {
                            return key.to_owned();
                        } else {
                            String::from("")
                        }
                    } else {
                        return key.to_owned();
                    }
                })
                .collect::<Vec<String>>();

            let keys = keys
                .into_iter()
                .map(|key| Value::BulkString(key))
                .collect::<Vec<Value>>();
            Ok(Value::Array(keys))
        }
        _ => Err(anyhow::anyhow!("Pattern error")),
    }
}
pub async fn handle_info(command_content: Vec<Value>, replication: Arc<Mutex<Replication>>) -> Result<Value>{
    let replication = replication.lock().await;
    if let Some(arg) = command_content.get(0){
        let arg: String = unwrap_value_to_string(arg).unwrap();
        match arg.as_str(){
            "replication" => Ok(replication.display_to_value().unwrap()),
            _ => Ok(Value::BulkString("role:master".to_string()))
        }
    }
    else {
        Ok(Value::BulkString("role:master".to_string()))
    }
}
pub fn handle_replconf() -> Result<Value> {
    Ok(Value::SimpleString("OK".to_string()))
}
pub async fn handle_psync(replication: Arc<Mutex<Replication>>) -> Result<Value> {
    let replication = replication.lock().await;
    Ok(Value::SimpleString(format!(
        "FULLRESYNC {} 0",
        replication.get_master_replid().unwrap()
    )))
}
pub fn handle_type(command_content: Vec<Value>, storage: &mut Store) -> Result<Value>{
    let key = unwrap_value_to_string(command_content.get(0).unwrap()).unwrap();

    let key_type: &str = match storage.get_stream_by_key(&key){
        Some(_) => "stream",
        None => {
            match storage.get_value(key.clone()){
                Ok(_) => "string",
                Err(e) => {
                    // Err(anyhow::anyhow!("Can not get key {} - error {}", key, e))
                    eprintln!("Error when get value of {} -- error {}", key, e);
                    "none"
                }
            }
        }
    };
    Ok(Value::SimpleString(key_type.to_string()))
}


pub fn handle_xadd(command_content: Vec<Value>, storage: &mut Store) -> Result<Value> {
    let stream_key = unwrap_value_to_string(command_content.get(0).unwrap()).unwrap();
    let stream_id = unwrap_value_to_string(command_content.get(1).unwrap()).unwrap();
    let stream = if let Some(stream) = storage.get_stream_by_key(&stream_key){ stream } else{
        storage.add_stream(&stream_key, StreamType::new_with_stream_id(&stream_id)).unwrap();
        storage.get_stream_by_key(&stream_key).unwrap()
    };

    let mut index = 2;
    while index < command_content.len(){
        let key = unwrap_value_to_string(command_content.get(index).unwrap()).unwrap();
        let value = unwrap_value_to_string(command_content.get(index + 1).unwrap()).unwrap();
        stream.add_to_collection(&key, &value).unwrap();
        index += 2;
    }
    Ok(Value::BulkString(stream_id.to_string()))
}