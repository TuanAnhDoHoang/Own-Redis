use std::{collections::HashMap};

use chrono::{DateTime, TimeZone, Utc};
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while_m_n},
    combinator::{peek},
    multi::{many0},
    number::complete::{le_u16, le_u32, le_u64, le_u8},
    IResult, Parser,
};

#[derive(Debug, Clone)]
pub struct Entry(pub String, pub Option<DateTime<Utc>>);
impl<T: Into<String>> From<T> for Entry {
    fn from(s: T) -> Self {
        Entry(s.into(), None)
    }
}
impl Entry {
    fn get(&self) -> Option<&String> {
        if let Some(exp) = self.1 {
            if exp < Utc::now() {
                return None;
            }
        }
        Some(&self.0)
    }
    fn with_exp(mut self, exp: Option<DateTime<Utc>>) -> Self {
        self.1 = exp;
        self
    }
}
#[derive(Debug, Clone)]
pub struct RdbFile {
    pub map: HashMap<String, Entry>,
}
impl RdbFile{
    pub fn new() -> Self{
        RdbFile { map: HashMap::new() }
    }
}
pub fn parse_rdb_file(input: &[u8]) -> IResult<&[u8], RdbFile>{
    let (input, _) = (tag(&b"REDIS"[..]), take_while_m_n(4, 4, |c: u8| c.is_ascii_digit())).parse(input)?;
    let (input, _) = many0(alt((parse_metadata_section, parse_database_section))).parse(input)?;

    let (input, (key, val)) = parse_key_value(input)?;
    let mut map = HashMap::new();
    map.insert(key, val);

    Ok((input, RdbFile { map }))

}
fn parse_metadata_section(input: &[u8]) -> IResult<&[u8], ()> {
    let (input, _) = tag(&b"\xFA"[..])(input)?;
    let (input, _) = parse_string(input)?;
    let (input, _) = parse_string(input)?;

    Ok((input, ()))
}
fn parse_database_section(input: &[u8]) -> IResult<&[u8], ()> {

    let (input, _) = tag(&b"\xFE"[..])(input)?;
    let (input, _db_number) = parse_length(input)?;
    let (input, _) = tag(&b"\xFB"[..])(input)?;
    let (input, _) = parse_length(input)?;
    let (input, _) = parse_length(input)?;

    Ok((input, ()))
}
fn parse_expiry(input: &[u8]) -> IResult<&[u8], Option<DateTime<Utc>>>{
    let origin_input = input;
   let (input, first_byte) = le_u8(input)?;
   match first_byte {
    0xfc => {
        let (input, exp) = le_u64(input)?;
        Ok((input, Some(Utc.timestamp_millis_opt(exp as i64).unwrap())))
    },
    0xfd => {
        let (input, exp) = le_u64(input)?;
        Ok((input, Some(Utc.timestamp_opt(exp as i64, 0).unwrap())))
    },
    _ => {
       Ok((
        origin_input, None
       )) 
    }
   }
}
fn parse_key_value(input: &[u8]) -> IResult<&[u8], (String, Entry)> {
    let (input, expiry) = parse_expiry(input)?;
    let (input, _) = tag(&b"\x00"[..])(input)?; // expect string type
    let (input, key) = parse_string(input)?;
    let (input, value) = parse_string(input)?;
    Ok((input, (key, Entry::from(value).with_exp(expiry))))
}
fn parse_string(input: &[u8]) -> IResult<&[u8], String> {
    let (input, first_byte) = peek(le_u8).parse(input)?;
    if first_byte >> 6 == 0b11 {
        let (input, first_byte) = le_u8(input)?;
        return match first_byte & 0b111111 {
            0 => {
                let (input, value) = le_u8(input)?;
                Ok((input, format!("{}", value)))
            }
            1 => {
                let (input, value) = le_u16(input)?;
                Ok((input, format!("{}", value)))
            }
            2 => {
                let (input, value) = le_u32(input)?;
                Ok((input, format!("{}", value)))
            }
            _ => Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Fail,
            ))),
        };
    }
    let (input, len) = parse_length(input)?;
    let (input, s) = nom::bytes::complete::take(len).parse(input)?;
    Ok((input, String::from_utf8_lossy(s).to_string()))
}
fn parse_length(input: &[u8]) -> IResult<&[u8], usize> {
    let (input, first_byte) = le_u8(input)?;
    let first_byte = first_byte as usize;
    match first_byte >> 6 {
        0b00 => Ok((input, first_byte & 0b111111)),
        0b01 => {
            let (input, second_byte) = le_u8(input)?;
            Ok((input, ((first_byte & 0b111111) << 8) | second_byte as usize))
        }
        0b10 => {
            let (input, next_four_byte) = le_u32(input)?;
            Ok((input, next_four_byte as usize))
        }
        _ => Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Fail,
        ))),
    }
}
