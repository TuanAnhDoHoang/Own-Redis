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

    for connection in listener.incoming(){
        println!("Connection accept");
        match connection{
            Ok(mut stream) => {
                let mut buf: [u8; 1024] = [0;1024];
                loop{
                    match stream.read(&mut buf){
                        Ok(read_size) => if read_size > 1{
                            let receiver = String::from_utf8_lossy(&buf[..read_size]).to_string();
                            println!("Get {:#?}", receiver);

                            for line in receiver.split("\n"){
                                let line = line.trim();
                                if line == "PING"{
                                    if stream.write_all("+PONG\r\n".as_bytes()).is_ok(){
                                        stream.flush().expect("Error when flushing data");
                                        println!("Sent +PONG");
                                    }
                                }
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
            },
            Err(e) => {println!("Got error when connect: {}", e)}
        }
    }
}
