use anyhow::Result;
#[derive(Clone)]
pub struct RedisDatabase{
    dir: String,
    dir_file_name:String,
    keys: Vec<String>
}
impl RedisDatabase{
    pub fn new() -> Self{
        RedisDatabase { dir: String::new(), dir_file_name: String::new() , keys: Vec::new()}
    }
    pub fn set_dir(&mut self, dir: String) -> Result<()>{
        //handle pattern
        self.dir = dir;
        Ok(())
    }
    pub fn set_dir_file_name(&mut self, dir_file_name: String) -> Result<()>{
        self.dir_file_name = dir_file_name;
        Ok(())
    }
    
    pub fn get_dir(&self) -> Result<String>{
        Ok(self.dir.clone())
    }
    pub fn get_dir_file_name(&mut self) -> Result<String>{
        Ok(self.dir_file_name.clone())
    }
    pub fn save_key_to_rdb(&mut self, key: &str) -> Result<()>{
        self.keys.push(key.to_string());
        Ok(())
    }
    pub fn get_all_key_from_rdb(&mut self) -> Result<Vec<String>>{
       Ok(self.keys.clone())
    }
}