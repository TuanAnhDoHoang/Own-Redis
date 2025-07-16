mod command_handler;
mod rdb;
mod resp;
mod store;

use crate::{
    command_handler::command_handler::command_handler,
    rdb::argument::{flags_handler, Argument},
    resp::resp::unwrap_value_to_string,
    store::store::Store,
};
use resp::resp::{read_value, write_value};
use resp::{
    resp::{extract_command, RespHandler},
    value::Value,
};
use std::env::args;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::{
    io::{split, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn connect_to_master(rdb_argument: &mut Argument, handler: Arc<Mutex<RespHandler>>) {
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
    let mut handler = handler.lock().await;
    for payload in payloads {
        handler.write_value(payload).await;
        handler.read_without_parse().await.unwrap();
        // println!("Response from master : {}", Value::serialize(&value));
    }
    handler.read_without_parse().await.unwrap();
}
#[tokio::main]
async fn main() {
    let args = args().collect::<Vec<String>>();

    let (mut rdb_argument, mut rdb_file, replication) =
        flags_handler(args.into_iter().skip(1).collect()).unwrap();
    let replication = Arc::new(Mutex::new(replication));

    let (master_address, master_port) = rdb_argument.get_master_endpoint().unwrap();

    if master_port != 0 {
        //slave side
        let stream_to_master = TcpStream::connect(format!("{}:{}", master_address, master_port))
            .await
            .unwrap();
        let stream_to_master = Arc::new(Mutex::new(stream_to_master));
        let to_master_handler = Arc::new(Mutex::new(RespHandler::new(stream_to_master)));
        connect_to_master(&mut rdb_argument, to_master_handler.clone()).await;

        println!("Done hand sakes to master");
        let handler = to_master_handler.clone();
        let mut storage = Store::new();

        //listenning new connections
        let listener = TcpListener::bind(format!("127.0.0.1:{}", rdb_argument.get_port().unwrap()))
            .await
            .unwrap_or_else(|e| {
                eprintln!("Failed to bind to address: {}", e);
                std::process::exit(1);
            });

        //for new thread
        let rdb_argument_clone = rdb_argument.clone();
        let rdb_file_clone = rdb_file.clone();
        let replication_clone = replication.clone();
        // tokio::spawn(async move {
        //     let mut rdb_argument = rdb_argument_clone.clone();
        //     let mut rdb_file = rdb_file_clone.clone();
        //     let replication = replication_clone.clone();
        //     let mut storage = Store::new();
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let (mut reader, mut writer) = split(stream);
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
        // tokio::spawn(async move {
        //     loop {
        //         let mut handler_mutex_guard = handler.lock().await;
        //         match handler_mutex_guard.read_value().await {
        //             Ok(Some(response)) => {
        //                 let (command, command_content) = extract_command(response).unwrap();
        //                 let result = command_handler(
        //                     command.clone(),
        //                     command_content.clone(),
        //                     &mut storage,
        //                     &mut rdb_argument,
        //                     &mut rdb_file,
        //                     replication.clone(),
        //                 )
        //                 .await;
        //                 handler_mutex_guard
        //                     .write_value(Value::serialize(&result))
        //                     .await;
        //             }
        //             Ok(None) => {
        //                 println!("Got nothing from master");
        //                 break;
        //             }
        //             Err(e) => {
        //                 print!("Got error when read value : {}", e);
        //                 break;
        //             }
        //         }
        //     }
        // });
        //save master connection
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
