use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvdbError {
    #[error("storage error: {0}")]
    Storage(#[from] rocksdb::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("lua error: {0}")]
    Lua(#[from] mlua::Error),
    #[error("config error: {0}")]
    Config(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("{0}")]
    Command(String),
    #[error("wrong number of arguments for '{0}' command")]
    WrongArgCount(&'static str),
    #[error("value is not an integer")]
    NotInteger,
    #[error("value is out of range")]
    OutOfRange,
    #[error("OOM command not allowed when used memory > maxmemory")]
    Oom,
    #[error("unknown command '{0}'")]
    UnknownCommand(String),
}

pub type KvdbResult<T> = Result<T, KvdbError>;
