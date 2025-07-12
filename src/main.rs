mod command_handler;
mod rdb;
mod resp;
mod store;

use crate::{
    command_handler::command_handler::command_handler,
    rdb::{
        argument::{flags_handler, Argument},
        parse_rdb::RdbFile,
        replication::{Replication},
    },
    resp::resp::unwrap_value_to_string,
    store::store::Store,
};
use resp::{
    resp::{extract_command, RespHandler},
    value::Value,
};
use std::{env, sync::Arc};
use tokio::sync::Mutex;
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    //handle flags
    let (rdb_argument, rdb_file, replication) =
        flags_handler(args.into_iter().skip(1).collect()).unwrap();

    //create listener
    let listener = TcpListener::bind(format!("127.0.0.1:{}", rdb_argument.get_port().unwrap()))
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to bind to address: {}", e);
            std::process::exit(1);
        });

    //connect to master if it's slave
    let (master_address, master_port) = rdb_argument.get_master_endpoint().unwrap();
    if master_port != 0 {
        connect_to_master(rdb_argument.clone(), master_address, master_port).await;
    }

    //listener accept connection
    let replication = Arc::new(Mutex::new(replication));
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("New client connected from: {}", addr);
                let redis_database_clone = rdb_argument.clone();
                let rdb_file_clone = rdb_file.clone();
                let replication_clone = replication.clone();
                tokio::spawn(async move {
                    stream_handler(
                        stream,
                        redis_database_clone,
                        rdb_file_clone,
                        replication_clone,
                    )
                    .await;
                });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
}

async fn stream_handler(
    stream: TcpStream,
    mut rdb_argument: Argument,
    mut rdb_file: RdbFile,
    replication: Arc<Mutex<Replication>>
) {
    let handler= Arc::new(Mutex::new(RespHandler::new(stream)));
    let mut storage: Store = Store::new();
    loop {
        let handler_clone = handler.clone();
        let mut handler = handler_clone.lock().await;

        let result: Value = match handler.read_value().await {
            Ok(Some(response)) => {
                if let Ok((command, command_content)) = extract_command(response) {
                    let replication_clone = replication.clone();
                    command_handler(
                        command,
                        command_content,
                        handler_clone.clone(),
                        &mut storage,
                        &mut rdb_argument,
                        &mut rdb_file,
                        replication_clone,
                    ).await
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

async fn connect_to_master(rdb_argument: Argument, address: String, port: usize) {
    let stream = TcpStream::connect(format!("{}:{}", address, port))
        .await
        .unwrap();
    let mut handler = RespHandler::new(stream);

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
        // let resp_string = Value::serialize(&Value::BulkString(payload));
        handler.write_value(payload).await;
        let _ = handler.read_value().await.unwrap().unwrap();
    }
}
