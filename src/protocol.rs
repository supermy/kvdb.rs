use bytes::{Bytes, BytesMut};

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Vec<RespValue>),
    Null,
    Boolean(bool),
    Double(f64),
    Map(Vec<(RespValue, RespValue)>),
    Set(Vec<RespValue>),
}

pub struct RespParser;

impl RespParser {
    /// 从字节流解析一条 RESP 消息；数据不完整时返回 None，调用方应继续读取。
    pub fn parse_one(data: &[u8]) -> Option<(RespValue, usize)> {
        if data.is_empty() {
            return None;
        }
        match data[0] {
            b'+' => parse_simple(data),
            b'-' => parse_error(data),
            b':' => parse_int(data),
            b'$' => parse_bulk(data),
            b'*' => parse_array(data),
            b'_' => parse_null(data),
            b'#' => parse_bool(data),
            b',' => parse_double(data),
            b'%' => parse_map(data),
            b'~' => parse_set(data),
            _ => None,
        }
    }

    /// 解析一条客户端命令：返回参数列表与已消费字节数。
    pub fn parse_cmd(buf: &mut BytesMut) -> Option<(Vec<Bytes>, usize)> {
        let (val, consumed) = Self::parse_one(buf)?;
        let args = match val {
            RespValue::Array(arr) => arr
                .into_iter()
                .map(|v| match v {
                    RespValue::BulkString(Some(b)) => b,
                    RespValue::BulkString(None) => Bytes::new(),
                    RespValue::SimpleString(s) => Bytes::from(s),
                    _ => Bytes::new(),
                })
                .collect(),
            RespValue::BulkString(Some(b)) => vec![b],
            RespValue::SimpleString(s) => vec![Bytes::from(s)],
            _ => return None,
        };
        Some((args, consumed))
    }
}

fn find_crlf(data: &[u8]) -> Option<usize> {
    data.windows(2).position(|w| w == b"\r\n")
}

fn parse_line(data: &[u8]) -> Option<(&[u8], usize)> {
    let pos = find_crlf(data)?;
    Some((&data[1..pos], pos + 2))
}

fn parse_simple(data: &[u8]) -> Option<(RespValue, usize)> {
    let (line, consumed) = parse_line(data)?;
    Some((
        RespValue::SimpleString(String::from_utf8_lossy(line).into_owned()),
        consumed,
    ))
}

fn parse_error(data: &[u8]) -> Option<(RespValue, usize)> {
    let (line, consumed) = parse_line(data)?;
    Some((
        RespValue::Error(String::from_utf8_lossy(line).into_owned()),
        consumed,
    ))
}

fn parse_int(data: &[u8]) -> Option<(RespValue, usize)> {
    let (line, consumed) = parse_line(data)?;
    let n = std::str::from_utf8(line).ok()?.parse::<i64>().ok()?;
    Some((RespValue::Integer(n), consumed))
}

fn parse_bulk(data: &[u8]) -> Option<(RespValue, usize)> {
    let (line, header_len) = parse_line(data)?;
    let len = std::str::from_utf8(line).ok()?.parse::<i64>().ok()?;
    if len < 0 {
        return Some((RespValue::BulkString(None), header_len));
    }
    let len = len as usize;
    let body_end = header_len + len + 2;
    if data.len() < body_end {
        return None;
    }
    let body = Bytes::copy_from_slice(&data[header_len..header_len + len]);
    Some((RespValue::BulkString(Some(body)), body_end))
}

fn parse_array(data: &[u8]) -> Option<(RespValue, usize)> {
    let (line, mut consumed) = parse_line(data)?;
    let count = std::str::from_utf8(line).ok()?.parse::<i64>().ok()?;
    if count < 0 {
        return Some((RespValue::Null, consumed));
    }
    let mut items = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (val, used) = RespParser::parse_one(&data[consumed..])?;
        items.push(val);
        consumed += used;
    }
    Some((RespValue::Array(items), consumed))
}

fn parse_null(data: &[u8]) -> Option<(RespValue, usize)> {
    let (_, consumed) = parse_line(data)?;
    Some((RespValue::Null, consumed))
}

fn parse_bool(data: &[u8]) -> Option<(RespValue, usize)> {
    let (line, consumed) = parse_line(data)?;
    let v = match line {
        b"t" => true,
        b"f" => false,
        _ => return None,
    };
    Some((RespValue::Boolean(v), consumed))
}

fn parse_double(data: &[u8]) -> Option<(RespValue, usize)> {
    let (line, consumed) = parse_line(data)?;
    let f = std::str::from_utf8(line).ok()?.parse::<f64>().ok()?;
    Some((RespValue::Double(f), consumed))
}

fn parse_map(data: &[u8]) -> Option<(RespValue, usize)> {
    let (line, mut consumed) = parse_line(data)?;
    let count = std::str::from_utf8(line).ok()?.parse::<i64>().ok()?;
    let mut map = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (k, used) = RespParser::parse_one(&data[consumed..])?;
        consumed += used;
        let (v, used) = RespParser::parse_one(&data[consumed..])?;
        consumed += used;
        map.push((k, v));
    }
    Some((RespValue::Map(map), consumed))
}

fn parse_set(data: &[u8]) -> Option<(RespValue, usize)> {
    let (line, mut consumed) = parse_line(data)?;
    let count = std::str::from_utf8(line).ok()?.parse::<i64>().ok()?;
    let mut items = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (val, used) = RespParser::parse_one(&data[consumed..])?;
        items.push(val);
        consumed += used;
    }
    Some((RespValue::Set(items), consumed))
}

pub struct RespSerializer;

impl RespSerializer {
    pub fn serialize(value: &RespValue) -> Bytes {
        let mut buf = Vec::new();
        Self::serialize_into(value, &mut buf);
        Bytes::from(buf)
    }

    fn serialize_into(value: &RespValue, buf: &mut Vec<u8>) {
        match value {
            RespValue::SimpleString(s) => {
                buf.push(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Error(s) => {
                buf.push(b'-');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(n) => {
                buf.push(b':');
                buf.extend_from_slice(n.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(None) => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            RespValue::BulkString(Some(b)) => {
                buf.push(b'$');
                buf.extend_from_slice(b.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(b);
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Array(items) => {
                buf.push(b'*');
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    Self::serialize_into(item, buf);
                }
            }
            RespValue::Null => {
                buf.extend_from_slice(b"_\r\n");
            }
            RespValue::Boolean(v) => {
                buf.push(b'#');
                buf.push(if *v { b't' } else { b'f' });
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Double(f) => {
                buf.push(b',');
                buf.extend_from_slice(f.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RespValue::Map(map) => {
                buf.push(b'%');
                buf.extend_from_slice(map.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for (k, v) in map {
                    Self::serialize_into(k, buf);
                    Self::serialize_into(v, buf);
                }
            }
            RespValue::Set(items) => {
                buf.push(b'~');
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    Self::serialize_into(item, buf);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_set_command() {
        let mut buf = BytesMut::from("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let (args, consumed) = RespParser::parse_cmd(&mut buf).unwrap();
        assert_eq!(consumed, 31);
        assert_eq!(args, vec!["SET", "foo", "bar"]);
    }

    #[test]
    fn serialize_error() {
        let v = RespValue::Error("ERR unknown command".to_string());
        assert_eq!(
            RespSerializer::serialize(&v),
            "-ERR unknown command\r\n".as_bytes()
        );
    }
}
