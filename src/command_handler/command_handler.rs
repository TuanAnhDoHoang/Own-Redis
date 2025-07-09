use crate::rdb::parse_rdb::RdbFile;
use crate::rdb::rdb::RedisDatabase;
use crate::unwrap_value_to_string;
use crate::{resp::value::Value, store::store::Store};
use anyhow::Result;

pub fn command_handler(
    command: String,
    command_content: Vec<Value>,
    storage: &mut Store,
    redis_database: &mut RedisDatabase,
    rdb_file: &mut RdbFile,
) -> Value {
    match command.as_str() {
        "PING" => handle_ping().expect("Error when handle PING"),
        "ECHO" => handle_echo(command_content).expect("Error when handle ECHO"),
        "SET" => {
            handle_set(command_content, storage, redis_database).expect("Error when handle SET")
        }
        "GET" => handle_get(command_content, storage).expect("Error when handle GET"),
        "CONFIG" => {
            handle_config(command_content, redis_database).expect("Error when handle CONFIG")
        }
        "KEYS" => handle_key(command_content, rdb_file).expect("Error when handle KEY"),
        c => {
            eprintln!("Invalid command: {}", c);
            Value::NullBulkString
        }
    }
}
fn handle_ping() -> Result<Value> {
    Ok(Value::SimpleString("PONG".to_string()))
}
fn handle_echo(command_content: Vec<Value>) -> Result<Value> {
    Ok(command_content.get(0).unwrap().clone())
}
fn handle_set(
    command_content: Vec<Value>,
    storage: &mut Store,
    redis_database: &mut RedisDatabase,
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

    let result = match command_content.get(2) {
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
                    match storage.set_value_with_px(key.clone(), value, px.clone()) {
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
            storage.set_value(key.clone(), value).unwrap(),
        )),
    };
    let _ = redis_database.save_key_to_rdb(key.as_str());
    result
}
fn handle_get(command_content: Vec<Value>, storage: &mut Store) -> Result<Value> {
    let key = command_content.get(0).unwrap().clone();
    let key = unwrap_value_to_string(&key).expect(&format!(
        "Get error when unwrap Value key {:?} to string",
        key
    ));
    match storage.get_value(key) {
        Ok(value) => Ok(Value::BulkString(value)),
        Err(_) => Ok(Value::NullBulkString),
    }
}
fn handle_config(command_content: Vec<Value>, redis_database: &mut RedisDatabase) -> Result<Value> {
    match command_content.get(0) {
        Some(value) => match unwrap_value_to_string(value).unwrap().as_str() {
            "GET" => {
                if let Some(name) = command_content.get(1) {
                    if name == &Value::BulkString("dir".to_string()) {
                        Ok(Value::Array(vec![
                            Value::BulkString("dir".to_string()),
                            Value::BulkString(redis_database.get_dir().unwrap()),
                        ]))
                    } else if name == &Value::BulkString("dbfilename".to_string()) {
                        Ok(Value::Array(vec![
                            Value::BulkString("dbfilename".to_string()),
                            Value::BulkString(redis_database.get_dir_file_name().unwrap()),
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
fn handle_key(command_content: Vec<Value>, rdb_file: &mut RdbFile) -> Result<Value> {
    let pattern: Value = command_content.get(0).unwrap().clone();
    let mut pattern: String = unwrap_value_to_string(&pattern).expect(&format!(
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
