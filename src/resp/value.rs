#[derive(Debug, Clone)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Value>),
}
impl Value {
    pub fn serialize(&self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            _ => panic!("Unsupport value for serialization"),
        }
    }
}
