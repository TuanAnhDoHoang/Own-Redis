use anyhow::{Result};
use crate::rdb::{parse_rdb::{parse_rdb_file, RdbFile}, replication::{Replication, Role}};
use std::path::PathBuf;

#[derive(Clone)]
pub struct Argument{
    dir: String,
    db_file_name:String,
    port: usize,
    master_endpoint: String,
    
}
impl Argument{
    pub fn new() -> Self{
        Argument { 
            dir: String::new(), 
            db_file_name: String::new(),
            port: 6379,
            master_endpoint: String::from("127.0.0.1:0")
        }
    }
    pub fn set_dir(&mut self, dir: String) -> Result<()>{
        //handle pattern
        self.dir = dir;
        Ok(())
    }
    pub fn set_dir_file_name(&mut self, db_file_name: String) -> Result<()>{
        self.db_file_name = db_file_name;
        Ok(())
    }
    
    pub fn get_dir(&self) -> Result<String>{
        Ok(self.dir.clone())
    }
    pub fn get_dir_file_name(&self) -> Result<String>{
        Ok(self.db_file_name.clone())
    }
    pub fn set_port(&mut self, port: usize) -> Result<()>{
        self.port = port;
        Ok(())
    }
    pub fn get_port(&self) -> Result<usize>{
        Ok(self.port)
    }
    pub fn get_master_endpoint(&self) -> Result<(String, usize)>{
        let mut master_endpoint = self.master_endpoint.split(":");
        let address = master_endpoint.next().unwrap().to_string();
        let port = master_endpoint.next().unwrap().parse::<usize>().unwrap();
        Ok((address, port))
    }
    pub fn set_master_endpoint(&mut self, address: String, port: usize) -> Result<()>{
        println!("LOG_FROM_set_master_endpoint -- address: {}, port: {}", address, port);
        self.master_endpoint = format!("{}:{}", address, port);
        Ok(())
    }
}

pub fn flags_handler<'a>(flags: Vec<String>) -> Result<(Argument, RdbFile, Replication)> {
    let mut rdb_argument = Argument::new();
    let mut rdb_file = RdbFile::new();
    let mut replication = Replication::new();

    let mut index = 0;
    while index < flags.len() {
        let flag_name = flags[index].clone();
        match flag_name.as_str() {
            "--dir" => match flags.get(index + 1) {
                Some(dir) => {
                    let _ = rdb_argument.set_dir(dir.to_owned());
                }
                None => panic!("Need a directory"),
            },
            "--dbfilename" => match flags.get(index + 1) {
                Some(dir_file_name) => {
                    let _ = rdb_argument.set_dir_file_name(dir_file_name.to_owned());
                }
                None => panic!("Need a file name"),
            },
            "--port" => match flags.get(index + 1) {
                Some(port_number) => {
                    let _ = rdb_argument.set_port(port_number.parse::<usize>().expect("Error when parse port number"));
                }
                None => panic!("Need a file name"),
            },
            "--replicaof" => match flags.get(index+1) {
                Some(master_endpoint) => {
                    replication.set_role(Role::Slave).expect("Error when set role in replication");

                    let mut m_endpoint = master_endpoint.split_whitespace();
                    let _ = rdb_argument.set_master_endpoint(
                        m_endpoint.next().unwrap().to_string(), 
                        m_endpoint.next().unwrap().parse::<usize>().unwrap()
                    );
                } 
                None => panic!("Need a file name"),
            }
            _ => panic!("Invalid flags name: {}", flag_name),
        }
        index += 2;
    }

    let path: PathBuf = PathBuf::from(rdb_argument.get_dir().unwrap_or("./".into()))
    .join(rdb_argument.get_dir_file_name().unwrap_or("dump.rdb".into()));

    if path.exists(){
        (_, rdb_file) = parse_rdb_file(
            std::fs::read(path).unwrap().as_slice()
        ).unwrap();
        // println!("LOG_FROM_flags_hanlder --- rdb_file: {:?}", rdb_file);
    }
    Ok((rdb_argument, rdb_file, replication))
}