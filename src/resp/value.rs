#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    BulkStringNoCRLF(String),
    Array(Vec<Value>),
    NullBulkString,
}
impl Value {
    pub fn serialize(&self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            Value::BulkStringNoCRLF(s) => format!("${}\r\n{}", s.len(), s),
            Value::NullBulkString => String::from("$-1\r\n"),
            Value::Array(a) => {
               let mut result = format!("*{}\r\n", a.len());
               for value in a{
                    let value = value.serialize();
                    result += &value;
               }
               result
            } 
            // _ => panic!("Unsupport value for serialization"),
        }
    }
}
