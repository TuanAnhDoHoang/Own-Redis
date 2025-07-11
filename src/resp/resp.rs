use std::{usize};
use crate::resp::value::{Value};
use anyhow::{Result};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
pub struct RespHandler {
    stream: TcpStream,
    // buffer: BytesMut
    buffer: [u8;1024],
}

impl RespHandler {
    pub fn new(stream: TcpStream) -> Self {
        RespHandler {
            stream,
            // buffer: BytesMut::with_capacity(1024),
            buffer: [0;1024]
        }
    }
    pub async fn read_value(&mut self) -> Result<Option<Value>> {
        let read_size: usize = self.stream.read(&mut self.buffer).await?;

        if read_size == 0{ return Ok(None); }

        // println!("LOG_FROM_read_value -- buffer: {}", bytes_to_string(&self.buffer[..read_size]));       
        match parse_payload(&self.buffer[..read_size]){
            Ok((value, _)) => Ok(Some(value)),
            Err(e) =>  Err(anyhow::anyhow!("Got Error when parse value from stream: {}", e)) 
        }
    }
    pub async fn write_value(&mut self, payload: String){
        println!("LOG_FROM_write_value -- payload: {}", payload);
        if self.stream.write_all(payload.as_bytes()).await.is_ok() {
            self.stream.flush().await.expect("Failed to flush stream");
            // println!("Sent to {:?}: {}", addr, payload.trim_end_matches("\r\n"));
        } else {
            println!("Failed to send response ");
        }

        if payload == Value::serialize(&Value::SimpleString(format!("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"))){
            let empty_rdb_file = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
            let rdb_bytes = hex::decode(empty_rdb_file).unwrap();
            let header = format!("${}\r\n", rdb_bytes.len());
            self.stream.write_all(header.as_bytes()).await.unwrap();
            self.stream.write_all(&rdb_bytes).await.unwrap();
            self.stream.flush().await.expect("Failed to flush stream");
        }
    }
}

fn parse_payload(payload: &[u8]) -> Result<(Value, usize)> {
    match payload[0] as char {
        '+' => parse_simple_string(payload),
        '$' => parse_bulk_string(payload),
        '*' => parse_array(payload),
        _ => Err(anyhow::anyhow!("Invalid sign in string given: {}", bytes_to_string(&payload))),
    }
}

fn parse_simple_string(payload: &[u8]) -> Result<(Value, usize)> {
    if let Some((buffer, buff_size)) = read_until_crlf(&payload[1..]) {
        return Ok((
            Value::SimpleString(String::from_utf8_lossy(buffer).trim().to_string()),
            buff_size,
        ));
    }
    Err(anyhow::anyhow!(
        "Invalid when parse simple string: {:#?}",
        payload
    ))
}

fn parse_bulk_string(payload: &[u8]) -> Result<(Value, usize)> {
    //$4\r\nPING\r\n
    // println!("LOG_FROM parse_bulk_string --- payload: {}", bytes_to_string(payload));
    if let Some((payload_size, _)) = read_until_crlf(&payload[1..]) {
        //Size of bulk string
        let payload_size: usize = String::from_utf8_lossy(payload_size)
            .parse::<usize>()
            .unwrap();

        let format_prev_bulk_string: String = format!("${}\r\n", payload_size);

        if let Some((buffer, _)) = read_until_crlf(&payload[format_prev_bulk_string.len()..]) {
                return Ok((
                    Value::BulkString(String::from_utf8_lossy(buffer).trim().to_string()),
                    payload_size,
                ));
        }
    }
    Err(anyhow::anyhow!(
        "Invalid when parse simple string: {:#?}",
        payload
    ))
}

fn parse_array(payload: &[u8]) -> Result<(Value, usize)> {
    // *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
    let (array_size, last) = read_until_crlf(&payload[1..]).unwrap();
    // println!("LOG_FROM_parse_array --- array_size: {} and last: {}", bytes_to_string(array_size), last);
    let array_size: usize = String::from_utf8_lossy(array_size)
        .trim()
        .to_string()
        .parse::<usize>()
        .unwrap();
    let mut array_parsed: Vec<Value> = Vec::new();
    let mut start = last + 1;
    for _ in 0..array_size{
        match parse_payload(&payload[start..]){
            Ok((buffer, buf_size)) => {
                // println!("LOG_FROM_parse_array buffer: {}", unwrap_value_to_string(&buffer).unwrap());
                // println!("LOG_FROM_parse_array --- payload[{}..]: {:?}", start, bytes_to_string(&payload[start..]));
                //re-calculation start value
                let format_prev_bulk_string = format!("${}\r\n{}\r\n", buf_size, unwrap_value_to_string(&buffer).unwrap());
                // println!("LOG_FROM_parse_array --- format: {}", format_prev_bulk_string);
                start += format_prev_bulk_string.len();
                array_parsed.push(buffer);

            },
            Err(e) => return Err(anyhow::anyhow!("Got error when parse inside array: {}", e))
        }
    }
    if array_parsed.len() == array_size{
        return Ok((Value::Array(array_parsed), array_size));
    }

    Err(anyhow::anyhow!(
        "Mismatch array lenght: {:#?}",
        payload
    ))
}

//Read until \r\n
fn read_until_crlf(payload: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..payload.len() {
        if payload[i] == b'\n' && payload[i-1] == b'\r'{
            //return buffer content and size from buffer to \n
            return Some((&payload[0..i - 1], i+1));
        }
    }
    None
}

pub fn extract_command(value: Value) -> Result<(String, Vec<Value>)>{
    match value{
        Value::Array(value_array) => {
            Ok((
                unwrap_value_to_string(value_array.first().unwrap()).unwrap(),
                value_array.into_iter().skip(1).collect()
            ))
        },
        _ => Err(anyhow::anyhow!("Got error when extract command {:?}", value))
    }
}

pub fn unwrap_value_to_string(value: &Value) -> Result<String>{
    // println!("LOG_FROM_unwrap_bulk_string --- value: {:?}", value);
    match value{
        Value::BulkString(value) => Ok(value.to_owned()),
        Value::SimpleString(value) => Ok(value.to_owned()),
        _ => Err(anyhow::anyhow!("Got error unwrap_value_to_string type mismatch"))
    }
}

fn bytes_to_string(buff: &[u8]) -> String{
    String::from_utf8_lossy(buff).to_string()
}