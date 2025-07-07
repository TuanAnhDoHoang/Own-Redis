use anyhow::Result;
#[derive(Clone)]
pub struct RedisDatabase{
    dir: String,
    dir_file_name:String 
}
impl RedisDatabase{
    pub fn new() -> Self{
        RedisDatabase { dir: String::from(""), dir_file_name: String::from("") }
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
}