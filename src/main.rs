use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    // Tạo TcpListener
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to bind to address: {}", e);
            std::process::exit(1);
        });

    println!("Server listening on 127.0.0.1:6379");

    // Chấp nhận kết nối và xử lý đồng thời
    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {
                println!("New client connected from: {}", addr);

                // Xử lý từng kết nối trong task riêng
                tokio::spawn(async move {
                    handler(stream, addr).await;
                });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
}

async fn handler(mut stream: TcpStream, addr: SocketAddr) {
    let mut buffer = [0; 1024];
    loop {
        match stream.read(&mut buffer).await {
            Ok(size) if size > 0 => {
                let received = String::from_utf8_lossy(&buffer[..size]).trim().to_string();
                println!("Received from {:?}: {}", addr, received);

                for line in received.split("\n") {
                    let line = line.trim();
                    match line.split_whitespace().next() {
                        Some("PING") => {
                            let response = "+PONG\r\n";
                            write_stream(&mut stream, addr, response).await;
                        }
                        Some("ECHO") => {
                            if let Some(payload ) = line.split_whitespace().nth(1){
                                let payload = format!("${}\r\n{}\r\n", payload.len(), payload);
                                write_stream(&mut stream, addr, payload.as_str()).await;
                            }
                        }
                        _ => println!(),
                    }

                    // if line.len() >= 4 && &line[0..4] == "ECHO" {
                    //     if line.len() == 4 {} //just only "echo"
                    //     else if &line[..=4] != "ECHO " {} //Invalid command ECHO(sahdisabud)
                    //     else{
                    //         //len > 4
                    //         let pharase = &line[4..line.len()].trim();
                    //         println!("{}", pharase);
                    //         let response = format!("${}\r\n{}\r\n", pharase.len(), pharase);

                    //         write_stream(&mut stream, addr, &response.as_str()).await;
                    //     }
                    // }
                }
            }
            Ok(_) => {
                println!("Client {:?} disconnected", addr);
                break;
            }
            Err(e) => {
                println!("Error reading from {:?}: {}", addr, e);
                break;
            }
        }
    }
}

async fn write_stream(stream: &mut TcpStream, addr: SocketAddr, payload: &str) {
    if stream.write_all(payload.as_bytes()).await.is_ok() {
        stream.flush().await.expect("Failed to flush stream");
        println!("Sent to {:?}: {}", addr, payload.trim());
    } else {
        println!("Failed to send response to {:?}", addr);
    }
}
