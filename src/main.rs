mod rdb;
mod resp;
mod store;
mod command_handler;

use crate::{
    command_handler::command_handler::command_handler,
    rdb::{parse_rdb::{self, RdbFile}, rdb::RedisDatabase}, 
    resp::resp::unwrap_value_to_string, 
    store::store::Store
};
use anyhow::Result;
use resp::{
    resp::{extract_command, RespHandler},
    value::Value,
};
use std::{env, path::PathBuf};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");
    let args: Vec<String> = env::args().collect();

    let (redis_database, rdb_file) = flags_handler(args.into_iter().skip(1).collect()).unwrap();

    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to bind to address: {}", e);
            std::process::exit(1);
        });

    println!("Server listening on 127.0.0.1:6379");

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("New client connected from: {}", addr);
                let redis_database_clone = redis_database.clone();
                let rdb_file_clone = rdb_file.clone();
                tokio::spawn(async move {
                    stream_handler(stream, redis_database_clone, rdb_file_clone).await;
                });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
}

async fn stream_handler(stream: TcpStream, mut redis_database: RedisDatabase, mut rdb_file: RdbFile) {
    let mut handler: RespHandler = RespHandler::new(stream);
    let mut storage: Store = Store::new();
    loop {
        let result: Value = match handler.read_value().await {
            Ok(Some(response)) => {
                if let Ok((command, command_content)) = extract_command(response) {
                    // match command.as_str() {
                    //     "PING" => Value::SimpleString("PONG".to_string()),
                    //     "ECHO" => command_content.get(0).unwrap().clone(),
                    //     "SET" => {
                    //         let key = command_content.get(0).unwrap().clone();
                    //         let value = command_content.get(1).unwrap().clone();
                    //         match command_content.get(2) {
                    //             //"PX" "Px" "px" "pX" //pattern regrex
                    //             Some(px_command) => {
                    //                 if px_command == &Value::BulkString("px".to_string())
                    //                     || px_command == &Value::BulkString("Px".to_string())
                    //                     || px_command == &Value::BulkString("pX".to_string())
                    //                     || px_command == &Value::BulkString("PX".to_string())
                    //                 {
                    //                     if let Some(px) = command_content.get(3) {
                    //                         match storage.set_value_with_px(key, value, px.clone())
                    //                         {
                    //                             Ok(()) => Value::BulkString("OK".to_string()),
                    //                             Err(_) => Value::NullBulkString,
                    //                         }
                    //                     } else {
                    //                         Value::NullBulkString
                    //                     }
                    //                 } else {
                    //                     Value::NullBulkString
                    //                 }
                    //             }
                    //             None => storage.set_value(key, value).unwrap(),
                    //         }
                    //     }
                    //     "GET" => {
                    //         let key = command_content.get(0).unwrap().clone();
                    //         match storage.get_value(key) {
                    //             Ok(value) => value,
                    //             Err(_) => Value::NullBulkString,
                    //         }
                    //     }
                    //     "CONFIG" => match command_content.get(0) {
                    //         Some(value) => match unwrap_value_to_string(value).unwrap().as_str() {
                    //             "GET" => {
                    //                 if let Some(name) = command_content.get(1) {
                    //                     if name == &Value::BulkString("dir".to_string()) {
                    //                         Value::Array(
                    //                             (vec![
                    //                                 Value::BulkString("dir".to_string()),
                    //                                 Value::BulkString(
                    //                                     redis_database.get_dir().unwrap(),
                    //                                 ),
                    //                             ]),
                    //                         )
                    //                     } else if name == &Value::BulkString("dbfilename".to_string()){
                    //                         Value::Array(
                    //                             (vec![
                    //                                 Value::BulkString("dbfilename".to_string()),
                    //                                 Value::BulkString(
                    //                                     redis_database.get_dir_file_name().unwrap(),
                    //                                 ),
                    //                             ]),
                    //                         )
                    //                     }
                    //                     else {Value::NullBulkString}
                    //                 }
                    //                 else{ Value::NullBulkString}
                    //             }
                    //             // "SET" => {}
                    //             _ => Value::NullBulkString,
                    //         },
                    //         None => Value::NullBulkString,
                    //     },
                    //     c => {
                    //         eprintln!("Invalid command: {}", c);
                    //         break;
                    //     }
                    // }
                    command_handler(command, command_content, &mut storage, &mut redis_database, &mut rdb_file)
                } else {
                    eprintln!("Failed to extract command");
                    break;
                }
            }
            Ok(None) => {
                println!("Client disconnected or no data");
                break;
            }
            Err(e) => {
                eprintln!("Error reading value: {}", e);
                break;
            }
        };
        // println!("LOG_FROM_stream_handler --- result: {:?}", result);
        handler.write_value(Value::serialize(&result)).await;
    }
}
fn flags_handler(flags: Vec<String>) -> Result<(RedisDatabase, RdbFile)> {
    let mut redis_database = RedisDatabase::new();
    let mut index = 0;
    while index < flags.len() {
        let flag_name = flags[index].clone();
        match flag_name.as_str() {
            "--dir" => match flags.get(index + 1) {
                Some(dir) => {
                    let _ = redis_database.set_dir(dir.to_owned());
                }
                None => panic!("Need a directory"),
            },
            "--dbfilename" => match flags.get(index + 1) {
                Some(dir_file_name) => {
                    let _ = redis_database.set_dir_file_name(dir_file_name.to_owned());
                }
                None => panic!("Need a file name"),
            },
            _ => panic!("Invalid flags name: {}", flag_name),
        }
        index += 2;
    }

    let path: PathBuf = PathBuf::from(redis_database.get_dir().unwrap_or("./".into()))
    .join(redis_database.get_dir_file_name().unwrap_or("dump.rdb".into()));

    let mut rdb_file = RdbFile::new();
    if path.exists(){
        (_, rdb_file) = parse_rdb::parse_rdb_file(
            std::fs::read(path).unwrap().as_slice()
        ).unwrap();
        // println!("LOG_FROM_flags_hanlder --- rdb_file: {:?}", rdb_file);
    }
    Ok((redis_database, rdb_file))
}
