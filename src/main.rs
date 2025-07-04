use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
};

fn main() {
    // In log để theo dõi
    println!("Server starting...");

    // Tạo TcpListener và xử lý lỗi nếu bind thất bại
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap_or_else(|e| {
        eprintln!("Failed to bind to address: {}", e);
        std::process::exit(1);
    });

    println!("Server listening on 127.0.0.1:6379");

    // Lặp vô hạn để chấp nhận kết nối
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // Tạo một thread mới cho mỗi kết nối
                let handle = thread::spawn( move || {
                    handle_client(stream);
                });
                // (Tùy chọn) Chờ thread hoàn thành nếu cần
                handle.join().unwrap();
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
}

// Hàm xử lý từng kết nối
fn handle_client(mut stream: std::net::TcpStream) {
    println!("New client connected");

    let mut buffer = [0; 1024]; // Buffer 1024 byte

    // Lặp để đọc và xử lý dữ liệu
    loop {
        match stream.read(&mut buffer) {
            Ok(size) if size > 0 => {
                let received = String::from_utf8_lossy(&buffer[..size]).trim().to_string();
                println!("Received: {}", received);

                for line in received.split("\n"){
                    let line = line.trim();

                    if line == "PING" {
                        let response = "+PONG\r\n";
                        if stream.write_all(response.as_bytes()).is_ok() {
                            stream.flush().expect("Failed to flush stream");
                            println!("Sent: {}", response.trim());
                        } else {
                            println!("Failed to send response");
                        }
                    }
                }
                
            }
            Ok(_) => break, // Không còn dữ liệu, thoát
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => {
                println!("Error reading from stream: {}", e);
                break;
            }
        }
    }
    println!("Client disconnected");
}