use anyhow::{Error, Result};
use std::collections::HashMap;
#[derive(Clone)]
struct StreamType {
    stream_id: String,
    collection: HashMap<String, String>,
    //splitted
    stream_time: usize,
    sequence_number: usize,
}

impl StreamType {
    pub fn new_with_stream_id(stream_id: &str) -> Self {
        let mut stream_id_parse = stream_id.split('-');
        let stream_time = stream_id_parse.next().unwrap().parse::<usize>().unwrap();
        let sequence_number = stream_id_parse.next().unwrap().parse::<usize>().unwrap();

        StreamType {
            stream_id: stream_id.to_string(),
            collection: HashMap::new(),
            stream_time,
            sequence_number,
        }
    }
    pub fn get_stream_id(&self) -> Result<String> {
        Ok(self.stream_id.clone())
    }
    // pub fn set_stream_id(&mut self, stream_id: &str) -> Result<()> {
    //     self.stream_id = stream_id.to_string();
    //     Ok(())
    // }
    pub fn add_to_collection(&mut self, key: &str, value: &str) -> Result<()> {
        self.collection.insert(key.to_string(), value.to_string());
        Ok(())
    }
    // pub fn get_stream_time(&self) -> Result<usize> {
    //     Ok(self.stream_time)
    // }
    // pub fn get_sequence_number(&self) -> Result<usize> {
    //     Ok(self.sequence_number)
    // }
    // pub fn get_collection(&mut self) -> Result<&mut HashMap<String, String>>{
    //     Ok(
    //         &mut self.collection
    //     )
    // }
}

pub fn split_stream_id(stream_id: &str) -> Result<(usize, usize)> {
    let mut stream_id_splited = stream_id.split('-');
    Ok((
        stream_id_splited
            .next()
            .expect(format!("Can not get stream_time in stream_id: {}", stream_id).as_str())
            .parse::<usize>()
            .expect("Can not parse stream time to usize"),
        stream_id_splited
            .next()
            .expect(format!("Can not get sequence number in stream_id: {}", stream_id).as_str())
            .parse::<usize>()
            .expect("Can not parse sequence number to usize"),
    ))
}

#[derive(Clone)]
pub struct Entry {
    //stream_key :: vector<stream>
    collection: HashMap<String, Vec<StreamType>>,
}
impl Entry {
    pub fn new() -> Self {
        Entry {
            collection: HashMap::new(),
        }
    }
    pub fn add_new_stream_key(&mut self, stream_key: &str) -> Result<()> {
        self.collection.insert(stream_key.to_string(), Vec::new());
        Ok(())
    }
    pub fn add_stream(&mut self, stream_key: &str, stream_id: &str) -> StreamEntryValidate {
        let (stream_time, sequence_number) = split_stream_id(stream_id).unwrap();
        match self.validate(stream_key, stream_time, sequence_number) {
            StreamEntryValidate::Successfull => {
                let new_stream = StreamType::new_with_stream_id(stream_id);
                match self.collection.get_mut(stream_key) {
                    Some(streams) => {
                        streams.push(new_stream);
                    }
                    None => {
                        self.collection
                            .insert(stream_key.to_string(), vec![new_stream])
                            .unwrap();
                    }
                }
                StreamEntryValidate::Successfull 
            }
            e => e,
        }
    }
    fn validate(&self, stream_key: &str, stream_time: usize, sequence_number: usize) -> StreamEntryValidate {
        if stream_time == 0 && sequence_number == 0{
           return StreamEntryValidate::EGreaterThan0_0; 
        }
        let stream = self.collection.get(stream_key).unwrap();
        match stream.last() {
            Some(last_stream) => {
                println!(
                    "LOG_FROM_validate --- {}:{} && {}:{}",
                    last_stream.stream_time,
                    last_stream.sequence_number,
                    stream_time,
                    sequence_number
                );
                if (last_stream.stream_time < stream_time)
                    || (last_stream.stream_time == stream_time
                        && last_stream.sequence_number < sequence_number)
                {
                    StreamEntryValidate::Successfull
                } else {
                    StreamEntryValidate::EIsSmallerOrEqual //must be greater than 
                }
            }
            None => {
                if stream_time > 0 || sequence_number > 0 {
                    StreamEntryValidate::Successfull
                } else {
                    StreamEntryValidate::EGreaterThan0_0 //must be greater than 0-0
                }
            }
        }
    }

    pub fn add_to_stream(
        &mut self,
        stream_key: &str,
        stream_id: &str,
        key: &str,
        value: &str,
    ) -> Result<()> {
        let streams = self.collection.get_mut(stream_key).unwrap();
        for stream in streams {
            if stream.get_stream_id().unwrap() == stream_id {
                stream.add_to_collection(key, value).unwrap();
            }
        }
        Ok(())
    }
    pub fn check_stream_id_exist(&self, stream_key: &str, stream_id: &str) -> bool {
        let streams = self
            .collection
            .get(stream_key)
            .expect(format!("Stream key {} is not exist", stream_key).as_str());
        for stream in streams {
            if stream.get_stream_id().unwrap() == stream_id {
                return true;
            }
        }
        false
    }
    pub fn check_stream_key_exist(&self, stream_key: &str) -> bool {
        self.collection.contains_key(stream_key)
    }
}

pub enum StreamEntryValidate{
    Successfull,
    EGreaterThan0_0,
    EIsSmallerOrEqual
}
impl StreamEntryValidate{
    pub fn as_msg(&self) -> String{
        match self{
            Self::EGreaterThan0_0 => format!("ERR The ID specified in XADD must be greater than 0-0"),
            Self::EIsSmallerOrEqual => format!("ERR The ID specified in XADD is equal or smaller than the target stream top item"),
            _ => format!("")
        }
    }
}
