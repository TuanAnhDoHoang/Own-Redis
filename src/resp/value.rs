#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    Array(Vec<Value>),
}
impl Value {
    pub fn serialize(&self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            Value::NullBulkString => String::from("$-1\r\n"),
            _ => panic!("Unsupport value for serialization"),
        }
    }
}
