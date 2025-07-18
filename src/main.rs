mod command_handler;
mod rdb;
mod resp;
mod store;

use crate::command_handler::command_handler::handle_info;
use crate::resp::resp::{read_value_native, read_without_parse};
use crate::{
    command_handler::command_handler::command_handler, rdb::argument::flags_handler,
    resp::resp::unwrap_value_to_string, store::store::Store,
};
use core::panic;
use resp::resp::{read_value, write_value};
use resp::{resp::extract_command, value::Value};
use tokio::io::AsyncReadExt;
use std::env::args;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio::{
    io::{split, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() {
    let args = args().collect::<Vec<String>>();

    let (rdb_argument, rdb_file, replication) =
        flags_handler(args.into_iter().skip(1).collect()).unwrap();
    let replication = Arc::new(Mutex::new(replication));

    let (master_address, master_port) = rdb_argument.get_master_endpoint().unwrap();

    let storage = Arc::new(Mutex::new(Store::new()));

    let storage_clone = storage.clone();
    if master_port != 0 {
        //listenning new connections
        let listener = TcpListener::bind(format!("127.0.0.1:{}", rdb_argument.get_port().unwrap()))
            .await
            .unwrap_or_else(|e| {
                eprintln!("Failed to bind to address: {}", e);
                std::process::exit(1);
            });
        //slave side
        let stream_to_master = TcpStream::connect(format!("{}:{}", master_address, master_port))
            .await
            .unwrap();
        let (mut master_reader, mut master_writer) = split(stream_to_master);

        //=====================Hand sake=========================//
        let payload_step_1: String =
            Value::serialize(&Value::Array(vec![Value::BulkString("PING".to_string())]));
        let payload_step_2_once: String = Value::serialize(&Value::Array(vec![
            Value::BulkString("REPLCONF".to_string()),
            Value::BulkString("listening-port".to_string()),
            Value::BulkString(format!("{}", rdb_argument.get_port().unwrap())),
        ]));
        let payload_step_2_twice: String = Value::serialize(&Value::Array(vec![
            Value::BulkString("REPLCONF".to_string()),
            Value::BulkString("capa".to_string()),
            Value::BulkString("psync2".to_string()),
        ]));

        let payload_step_3: String = Value::serialize(&Value::Array(vec![
            Value::BulkString("PSYNC".to_string()),
            Value::BulkString("?".to_string()),
            Value::BulkString("-1".to_string()),
        ]));

        let payloads = vec![
            payload_step_1,
            payload_step_2_once,
            payload_step_2_twice,
            payload_step_3,
        ];
        for payload in payloads {
            master_writer.write_all(payload.as_bytes()).await.unwrap();
            read_without_parse(&mut master_reader).await.unwrap();
        }
        //======================End handsake====================================//

        //reciev table empty file
        read_without_parse(&mut master_reader).await.unwrap();

        tokio::spawn(async move {
            let mut buffer: [u8; 1024] = [0; 1024];
            //read master stream
            loop {
                master_reader.read(&mut buffer).await.unwrap();
                // println!("read size: {} end", String::from_utf8_lossy(&buffer[..read_size]));
                let mut asize = 0;
                let mut last = 0;

                for i in 0..buffer.len() {
                    if buffer[i] == b'\n' && buffer[i - 1] == b'\r' {
                        asize = String::from_utf8_lossy(&buffer[1..i - 1])
                            .parse()
                            .unwrap();
                        last = i + 1;
                        break;
                    }
                }

                let mut result: Vec<String> = Vec::new();

                loop {
                    let mut len_next_command = 0;
                    for i in last..buffer.len() {
                        if buffer[i] == b'\n' && buffer[i - 1] == b'\r' {
                            // println!("{:?}", buffer[..i].to_ascii_lowercase());
                            len_next_command = String::from_utf8_lossy(&buffer[last + 1..i - 1])
                                .parse()
                                .unwrap();
                            last = i + 1;
                            break;
                        }
                    }
                    result.push(
                        String::from_utf8_lossy(&buffer[last..last + len_next_command]).to_string(),
                    );
                    last = last + len_next_command + 2;
                    if result.len() == asize{
                        break;
                    }
                }
                // if result[0] == "SET"{
                    storage_clone.lock().await.set_value(result[1].clone(), result[2].clone()).unwrap();
                    // println!("setted {}:{}", result[1], result[2]);
                // }
            }
        });
        // tokio::spawn( async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    //read client stream
                    let (mut reader, mut writer) = split(stream);
                    loop {
                        sleep(Duration::from_millis(200)).await;
                        match read_value(&mut reader).await {
                            Ok(Some(response)) => {
                                let (command, command_content) = extract_command(response).unwrap();
                                let result = match command.as_str() {
                                    "INFO" => handle_info(command_content, replication.clone())
                                        .await
                                        .expect("Error when handle KEY"),
                                    "GET" => {
                                        let key =
                                            unwrap_value_to_string(command_content.get(0).unwrap())
                                                .unwrap();
                                        let storage = storage.lock().await;

                                        let mut value = Value::NullBulkString;
                                        for _ in 1..100 {
                                            if let Ok(get_value) = storage.get_value(key.clone()) {
                                                value = Value::BulkString(get_value);
                                                break;
                                            }
                                        }
                                        if value == Value::NullBulkString {
                                            panic!("Fail get value of key {}", key)
                                        } else {
                                            value
                                        }
                                    }
                                    _ => Value::NullBulkString,
                                };
                                writer
                                    .write_all(result.serialize().as_bytes())
                                    .await
                                    .unwrap();
                            }
                            Ok(None) => {
                                println!("Got nothing from client");
                                break;
                            }
                            Err(e) => println!("Got error from slave: {}", e),
                        }
                    }
                }
                Err(e) => println!("Connection error: {}", e),
            }
        }
        // });
    } else {
        //master side
        let listener = TcpListener::bind(format!("127.0.0.1:{}", rdb_argument.get_port().unwrap()))
            .await
            .unwrap_or_else(|e| {
                eprintln!("Failed to bind to address: {}", e);
                std::process::exit(1);
            });
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let (mut reader, writer) = split(stream);
                    let writer = Arc::new(Mutex::new(writer));

                    //clone for loop
                    let mut storage: Store = Store::new();
                    let mut rdb_argument = rdb_argument.clone();
                    let mut rdb_file = rdb_file.clone();
                    let replication = replication.clone();

                    tokio::spawn(async move {
                        loop {
                            match read_value(&mut reader).await {
                                Ok(Some(response)) => {
                                    let (command, command_content) =
                                        extract_command(response).unwrap();
                                    let result = command_handler(
                                        command.clone(),
                                        command_content.clone(),
                                        &mut storage,
                                        &mut rdb_argument,
                                        &mut rdb_file,
                                        replication.clone(),
                                    )
                                    .await;
                                    write_value(writer.clone(), Value::serialize(&result)).await;

                                    // handle second time
                                    if command == "PSYNC" {
                                        let empty_rdb_file = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
                                        let rdb_bytes = hex::decode(empty_rdb_file).unwrap();
                                        println!(
                                            "LOG_FROM_handle_second_time --- rdb_bytes: {:?}",
                                            rdb_bytes
                                        );
                                        let header = format!("${}\r\n", rdb_bytes.len());

                                        let mut writer_guard = writer.lock().await;
                                        writer_guard.write_all(header.as_bytes()).await.unwrap();
                                        writer_guard.write_all(&rdb_bytes).await.unwrap();
                                        writer_guard.flush().await.expect("Failed to flush stream");

                                        let mut replication = replication.lock().await;
                                        replication.add_repl_handler(writer.clone()).unwrap();
                                    } else if command == "SET" {
                                        let mut repls = replication.lock().await;
                                        let key =
                                            unwrap_value_to_string(command_content.get(0).unwrap())
                                                .unwrap();
                                        let value =
                                            unwrap_value_to_string(command_content.get(1).unwrap())
                                                .unwrap();
                                        let payload = Value::Array(vec![
                                            Value::BulkString("SET".to_string()),
                                            Value::BulkString(key),
                                            Value::BulkString(value),
                                        ]);

                                        for repl_writer in &mut repls.replication_handlers {
                                            repl_writer
                                                .lock()
                                                .await
                                                .write_all(payload.serialize().as_bytes())
                                                .await
                                                .unwrap();
                                            repl_writer.lock().await.flush().await.unwrap();
                                        }
                                    }
                                }
                                Ok(None) => {
                                    break;
                                }
                                Err(e) => {
                                    eprint!("Got error when read value : {}", e);
                                    break;
                                }
                            }
                        }
                    });
                }
                Err(e) => eprintln!("Got error when listenning... --- error: {}", e),
            }
        }
    }
}
