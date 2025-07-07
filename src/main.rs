mod store;
mod resp;

use resp::{
    resp::{extract_command, RespHandler},
    value::Value,
};
use crate::store::store::Store;
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

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
                tokio::spawn(async move {
                    stream_handler(stream).await;
                });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
}

async fn stream_handler(stream: TcpStream) {
    let mut handler: RespHandler = RespHandler::new(stream);
    let mut storage: Store = Store::new();
    loop {
        let result: Value = match handler.read_value().await {
            Ok(Some(response)) => {
                if let Ok((command, command_content)) = extract_command(response) {
                    match command.as_str() {
                        "PING" => Value::SimpleString("PONG".to_string()),
                        "ECHO" => command_content.get(0).unwrap().clone(),
                        "SET" => {
                            let key = command_content.get(0).unwrap().clone();
                            let value = command_content.get(1).unwrap().clone();
                            match command_content.get(2){ //"PX" "Px" "px" "pX" //pattern regrex
                                Some(px_command) => {
                                    if px_command == &Value::BulkString("px".to_string()) ||
                                       px_command == &Value::BulkString("Px".to_string()) ||
                                       px_command == &Value::BulkString("pX".to_string()) ||
                                       px_command == &Value::BulkString("PX".to_string()) 
                                    {
                                        if let Some(px) = command_content.get(3){
                                            match storage.set_value_with_px(key, value, px.clone()){
                                                Ok(()) => Value::BulkString("OK".to_string()),
                                                Err(_) => Value::NullBulkString 
                                            }
                                        }
                                        else{Value::NullBulkString}
                                    }
                                    else {Value::NullBulkString}
                                }
                                None => storage.set_value(key, value).unwrap()
                            }
                        },
                        "GET" =>{
                            let key = command_content.get(0).unwrap().clone();
                            match storage.get_value(key){
                                Ok(value) => value,
                                Err(_) => Value::NullBulkString 
                            }
                        }
                        c => {
                            eprintln!("Invalid command: {}", c);
                            break;
                        }
                    }
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
        handler
            .write_value(Value::serialize(&result))
            .await;
    }
}
