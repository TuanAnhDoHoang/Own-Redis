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

    let (redis_database, rdb_file, port) = flags_handler(args.into_iter().skip(1).collect()).unwrap();

    let listener = TcpListener::bind(format!("127.0.0.1:{}",port))
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

fn flags_handler(flags: Vec<String>) -> Result<(RedisDatabase, RdbFile, usize)> {
    let mut redis_database = RedisDatabase::new();
    let mut rdb_file = RdbFile::new();
    let mut port: usize = 6379;
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
            "--port" => match flags.get(index + 1) {
                Some(port_number) => {
                    port = port_number.parse::<usize>().expect("Error when parse port number");
                }
                None => panic!("Need a file name"),
            },
            _ => panic!("Invalid flags name: {}", flag_name),
        }
        index += 2;
    }

    let path: PathBuf = PathBuf::from(redis_database.get_dir().unwrap_or("./".into()))
    .join(redis_database.get_dir_file_name().unwrap_or("dump.rdb".into()));

    if path.exists(){
        (_, rdb_file) = parse_rdb::parse_rdb_file(
            std::fs::read(path).unwrap().as_slice()
        ).unwrap();
        // println!("LOG_FROM_flags_hanlder --- rdb_file: {:?}", rdb_file);
    }
    Ok((redis_database, rdb_file, port))
}
