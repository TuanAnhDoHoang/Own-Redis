use crate::{
    rdb::{argument::Argument, parse_rdb::RdbFile, replication::Replication},
    store::entry::StreamEntryValidate,
    unwrap_value_to_string,
    {resp::value::Value, store::store::Store},
};
use anyhow::Result;
use std::{collections::HashMap, time::Duration};
use std::sync::Arc;
use tokio::{sync::Mutex, time::sleep};

pub async fn command_handler(
    command: String,
    command_content: Vec<Value>,
    storage: Arc<Mutex<Store>>,
    rdb_argument: &mut Argument,
    rdb_file: &mut RdbFile,
    replication: Arc<Mutex<Replication>>,
) -> Value {
    match command.as_str() {
        "PING" => handle_ping().expect("Error when handle PING"),
        "ECHO" => handle_echo(command_content).expect("Error when handle ECHO"),
        "SET" => handle_set(command_content, storage)
            .await
            .expect("Error when handle SET"),
        "GET" => handle_get(command_content, storage, rdb_file)
            .await
            .expect("Error when handle GET"),
        "CONFIG" => handle_config(command_content, rdb_argument).expect("Error when handle CONFIG"),
        "KEYS" => handle_key(command_content, rdb_file).expect("Error when handle KEY"),
        "INFO" => handle_info(command_content, replication)
            .await
            .expect("Error when handle KEY"),
        "REPLCONF" => handle_replconf().expect("Error when handle replconf"),
        "PSYNC" => handle_psync(replication)
            .await
            .expect("Error when handle psync"),
        "TYPE" => handle_type(command_content, storage)
            .await
            .expect("Error when handle type"),
        "XADD" => handle_xadd(command_content, storage)
            .await
            .expect("Error when handle xadd"),
        "XRANGE" => handle_xrange(command_content, storage)
            .await
            .expect("Error when handle xrange"),
        "XREAD" => handle_xread(command_content, storage)
            .await
            .expect("Error when handle xread"),
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
pub async fn handle_set(command_content: Vec<Value>, storage: Arc<Mutex<Store>>) -> Result<Value> {
    let mut storage = storage.lock().await;
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
pub async fn handle_get(
    command_content: Vec<Value>,
    storage: Arc<Mutex<Store>>,
    rdb_file: &mut RdbFile,
) -> Result<Value> {
    let storage = storage.lock().await;
    let key = command_content.get(0).unwrap().clone();
    let key = unwrap_value_to_string(&key).expect(&format!(
        "Get error when unwrap Value key {:?} to string",
        key
    ));
    let rdb_file_collections: HashMap<String, String> = rdb_file
        .map
        .iter()
        .map(|(key, entry)| {
            if let Some(px) = entry.1 {
                if px > chrono::Utc::now() {
                    return (key.to_owned(), entry.0.clone());
                } else {
                    ("".to_string(), "".to_string())
                }
            } else {
                return (key.to_owned(), entry.0.clone());
            }
        })
        .collect::<HashMap<String, String>>();

    match storage.get_value(key.clone()) {
        Ok(value) => Ok(Value::BulkString(value)),
        Err(_) => {
            if let Some(value) = rdb_file_collections.get(&key) {
                Ok(Value::BulkString(value.to_owned()))
            } else {
                Ok(Value::NullBulkString)
            }
        }
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
pub async fn handle_info(
    command_content: Vec<Value>,
    replication: Arc<Mutex<Replication>>,
) -> Result<Value> {
    let replication = replication.lock().await;
    if let Some(arg) = command_content.get(0) {
        let arg: String = unwrap_value_to_string(arg).unwrap();
        match arg.as_str() {
            "replication" => Ok(replication.display_to_value().unwrap()),
            _ => Ok(Value::BulkString("role:master".to_string())),
        }
    } else {
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
pub async fn handle_type(command_content: Vec<Value>, storage: Arc<Mutex<Store>>) -> Result<Value> {
    let storage = storage.lock().await;
    let key = unwrap_value_to_string(command_content.get(0).unwrap()).unwrap();

    let key_type: &str = match storage.entry.check_stream_key_exist(&key) {
        true => "stream",
        false => {
            match storage.get_value(key.clone()) {
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

pub async fn handle_xadd(command_content: Vec<Value>, storage: Arc<Mutex<Store>>) -> Result<Value> {

    //parse arguments
    let stream_key = unwrap_value_to_string(command_content.get(0).unwrap()).unwrap();
    let mut stream_id = unwrap_value_to_string(command_content.get(1).unwrap()).unwrap();

    let mut storage = storage.lock().await;
    //get stream by key stream and stream id to add new key value pairs
    if !storage.entry.check_stream_key_exist(&stream_key) {
        storage.entry.add_new_stream_key(&stream_key).unwrap();
    }
    if !storage.entry.check_stream_id_exist(&stream_key, &stream_id) {
        match storage.entry.add_stream(&stream_key, &stream_id) {
            StreamEntryValidate::Successfull(stream_id_change) => {
                stream_id = stream_id_change;
            }
            c => match c {
                StreamEntryValidate::EGreaterThan0_0 => return Ok(Value::SimpleError(c.as_msg())),
                StreamEntryValidate::EIsSmallerOrEqual => {
                    return Ok(Value::SimpleError(c.as_msg()))
                }
                _ => (),
            },
        };

        let mut index = 2;
        while index < command_content.len() {
            let key = unwrap_value_to_string(command_content.get(index).unwrap()).unwrap();
            let value = unwrap_value_to_string(command_content.get(index + 1).unwrap()).unwrap();
            storage
                .entry
                .add_to_stream(&stream_key, &stream_id, &key, &value)
                .unwrap();
            index += 2;
        }
        Ok(Value::BulkString(stream_id.to_string()))
    } else {
        Ok(Value::SimpleError(
            StreamEntryValidate::EIsSmallerOrEqual.as_msg(),
        ))
    }
}
pub async fn handle_xrange(
    command_content: Vec<Value>,
    storage: Arc<Mutex<Store>>,
) -> Result<Value> {
    let storage = storage.lock().await;
    let stream_key = unwrap_value_to_string(command_content.get(0).unwrap()).unwrap();
    let start = unwrap_value_to_string(command_content.get(1).unwrap()).unwrap();
    let end = unwrap_value_to_string(command_content.get(2).unwrap()).unwrap();

    let (st_time, st_seq) = if start.contains('-') && start.len() > 1 {
        let mut start = start.split('-');
        (
            start.next().unwrap().parse::<usize>().unwrap(),
            start.next().unwrap().parse::<usize>().unwrap(),
        )
    } else if start == "-" {
        (0, 0)
    } else {
        (start.parse::<usize>().unwrap(), 0 as usize)
    };

    let (end_time, end_seq) = if end.contains('-') {
        let mut end = end.split('-');
        (
            end.next().unwrap().parse::<usize>().unwrap(),
            end.next().unwrap().parse::<usize>().unwrap(),
        )
    } else if end == "+" {
        let end_stream = storage.entry.get_max_sequece_number(&stream_key).unwrap();
        (end_stream, end_stream)
    } else {
        (
            end.parse::<usize>().unwrap(),
            storage.entry.get_max_sequece_number(&stream_key).unwrap(),
        )
    };

    let result =
        storage
            .entry
            .get_streams_in_range(&stream_key, st_time, end_time, st_seq, end_seq);
    // println!("LOG_FROM_handle_xrange --- result {:?}", result);
    let mut streams: Vec<Value> = Vec::new();
    for stream in result {
        let collection = stream.get_collection().unwrap();
        let mut s = Vec::new();
        s.push(Value::BulkString(stream.get_stream_id().unwrap()));
        for pair in collection {
            s.push(Value::Array(vec![
                Value::BulkString(pair.0.to_owned()),
                Value::BulkString(pair.1.to_owned()),
            ]));
        }
        streams.push(Value::Array(s));
    }

    Ok(Value::Array(streams))
}
pub async fn handle_xread(
    command_content: Vec<Value>,
    storage: Arc<Mutex<Store>>,
) -> Result<Value> {
    let first_arg = unwrap_value_to_string(command_content.get(0).unwrap()).unwrap();
    let mut stream_keys_argument_start = 1;
    
    if first_arg == "streams" {
        stream_keys_argument_start = 1;
    } else if first_arg == "block" {
        stream_keys_argument_start = 3;
        let block_time = unwrap_value_to_string(command_content.get(1).unwrap())
            .unwrap()
            .parse::<u64>()
            .unwrap();
        if block_time == 0{
            let stream_keys = command_content[stream_keys_argument_start..=((command_content.len() + stream_keys_argument_start + 1)/2 - 1)]
                    .iter()
                    .map(|c| unwrap_value_to_string(c).unwrap())
                    .collect::<Vec<String>>();
            //save current status of storage
            let storage_guard = storage.lock().await;
            let current_len =  {
                let mut len = 0;
                for stream_key in &stream_keys {
                    len += storage_guard.entry.get_len(&stream_key).unwrap();
                }
                len
            };
            drop(storage_guard);
            loop {
                sleep(Duration::from_millis(500)).await;
                let storage_guard = storage.lock().await;
                //check if it differ with previous status then return else we sleep again
                let next_len =  {
                    let mut len = 0;
                    for stream_key in &stream_keys {
                        len += storage_guard.entry.get_len(&stream_key).unwrap();
                    }
                    len
                };
                drop(storage_guard);
                if next_len > current_len{ break }
            }
        }
        if block_time > 0{
            sleep(Duration::from_millis(block_time)).await;
        }
    }

    let stream_keys = command_content[stream_keys_argument_start..=((command_content.len() + stream_keys_argument_start + 1)/2 - 1)]
        .iter()
        .map(|c| unwrap_value_to_string(c).unwrap())
        .collect::<Vec<String>>();

    let mut stream_id_index = (command_content.len() + stream_keys_argument_start + 1)/2;
    let storage = storage.lock().await;
    let mut result = Vec::new();
    for stream_key in stream_keys {
        let mut array_stream_key: Vec<Value> = Vec::new();
        array_stream_key.push(Value::BulkString(stream_key.clone()));
        let start = unwrap_value_to_string(command_content.get(stream_id_index).unwrap()).unwrap();
        //get time and sequence from start value
        let (st_time, st_seq) = if start.contains('-') && start.len() > 1 {
            let mut start = start.split('-');
            (
                start.next().unwrap().parse::<usize>().unwrap(),
                start.next().unwrap().parse::<usize>().unwrap(),
            )
        } else {
            (start.parse::<usize>().unwrap(), 0 as usize)
        };
        let streams = storage
            .entry
            .get_streams_from_start(&stream_key, st_time, st_seq);

        //if cannot get any streams by condition
        if streams.len() == 0{ return Ok(Value::NullBulkString);}
        
        let mut streams_array = Vec::new();
        for stream in streams {
            let mut stream_array = Vec::new();
            stream_array.push(Value::BulkString(stream.get_stream_id().unwrap()));
            let collection = stream.get_collection().unwrap();
            let mut pair_array = Vec::new();
            for pair in collection {
                pair_array.push(Value::BulkString(pair.0.to_owned()));
                pair_array.push(Value::BulkString(pair.1.to_owned()));
            }
            stream_array.push(Value::Array(pair_array));
            streams_array.push(Value::Array(stream_array));
        }
        array_stream_key.push(Value::Array(streams_array));
        result.push(Value::Array(array_stream_key));
        stream_id_index += 1;
    }
    Ok(Value::Array(result))
}
