#![allow(unused_imports)]
use std::{io::{Read, Write}, net::TcpListener};

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

                let mut buf: [u8; 1024] = [0; 1024]; // Buffer 1024 byte
                match stream.read(&mut buf) {
                    Ok(size) if size > 0 => {

                        let received = String::from_utf8_lossy(&buf[..size]).to_string();

                        //split request base on rows
                        for word in received.split("\n"){
                            if word.trim() == "PING" {
                                print!("HI");
                                if stream.write_all("+PONG\r\n".as_bytes()).is_ok() {
                                    stream.flush().expect("Failed to flush stream");
                                    println!("Sent: +PONG");
                                } else {
                                    println!("Failed to send response");
                                }
                            }
                        }
                    }
                    Ok(_) => println!("Empty message"),
                    Err(e) => println!("Error reading stream: {}", e),
                }
            }
            Err(e) => {
                println!("Error accepting connection: {}", e);
            }
        }
    }
}