mod command_handler;
mod rdb;
mod resp;
mod store;

use crate::{
    command_handler::command_handler::command_handler,
    rdb::{
        argument::{flags_handler, Argument},
        parse_rdb::RdbFile,
        replication::Replication,
    },
    resp::resp::unwrap_value_to_string,
    store::store::Store,
};
use resp::{
    resp::{extract_command, RespHandler},
    value::Value,
};
use std::env;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let (rdb_argument, rdb_file, replication) =
        flags_handler(args.into_iter().skip(1).collect()).unwrap();

    let listener = TcpListener::bind(format!("127.0.0.1:{}", rdb_argument.get_port().unwrap()))
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to bind to address: {}", e);
            std::process::exit(1);
        });

    let (m_address, m_port) = rdb_argument.get_master_endpoint().unwrap();
    if m_port != 0{
        let mut connection = TcpStream::connect(format!("{}:{}", m_address, m_port))
            .await
            .unwrap();
        let _ = connection
            .write_all("*1\r\n$4\r\nPING\r\n".as_bytes())
            .await;
    }

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
    mut replication: Replication,
) {
    let mut handler: RespHandler = RespHandler::new(stream);
    let mut storage: Store = Store::new();
    loop {
        let result: Value = match handler.read_value().await {
            Ok(Some(response)) => {
                if let Ok((command, command_content)) = extract_command(response) {
                    command_handler(
                        command,
                        command_content,
                        &mut storage,
                        &mut rdb_argument,
                        &mut rdb_file,
                        &mut replication,
                    )
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
