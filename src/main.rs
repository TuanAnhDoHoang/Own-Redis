#![allow(unused_imports)]
use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap_or_else(|e| {
        eprintln!("Failed to bind to address: {}", e);
        std::process::exit(1);
    });

    println!("Server listening on 127.0.0.1:6379");

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("Accepted new connection");
                let mut buf = [0; 1024]; // Buffer 1024 byte

                // Lặp để đọc tất cả dữ liệu từ kết nối
                loop {
                    match stream.read(&mut buf) {
                        Ok(size) if size > 0 => {
                            let received = String::from_utf8_lossy(&buf[..size]).to_string();
                            println!("Received: {}", received);

                            if received == "PING" {
                                let response = "+PONG\r\n";
                                if stream.write_all(response.as_bytes()).is_ok() {
                                    stream.flush().expect("Failed to flush stream");
                                    println!("Sent: {}", response.trim());
                                } else {
                                    println!("Failed to send response");
                                }
                            } else {
                                println!("Unknown command: {}", received);
                            }
                        }
                        Ok(_) => break, // Không còn dữ liệu, thoát vòng lặp
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break, // Không còn dữ liệu để đọc
                        Err(e) => {
                            println!("Error reading stream: {}", e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                println!("Error accepting connection: {}", e);
            }
        }
    }
}
