use crate::app::AppRequest;
use bytes::Bytes;
use prost::Message;
use raft::prelude::*;

pub trait TtoU8 {
    fn t_to_u8(&self) -> Vec<u8>;
}
pub trait U8toT {
    fn u8_to_t(data: Vec<u8>) -> Self;
}
impl TtoU8 for HardState {
    fn t_to_u8(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode(&mut buf).unwrap();
        buf
    }
}
impl U8toT for HardState {
    fn u8_to_t(data: Vec<u8>) -> Self {
        let buf = Bytes::from(data);
        prost::Message::decode(buf).unwrap()
    }
}
impl TtoU8 for ConfState {
    fn t_to_u8(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode(&mut buf).unwrap();
        buf
    }
}
impl U8toT for ConfState {
    fn u8_to_t(data: Vec<u8>) -> Self {
        let buf = Bytes::from(data);
        prost::Message::decode(buf).unwrap()
    }
}
impl TtoU8 for Entry {
    fn t_to_u8(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode(&mut buf).unwrap();
        buf
    }
}
impl U8toT for Entry {
    fn u8_to_t(data: Vec<u8>) -> Self {
        let buf = Bytes::from(data);
        prost::Message::decode(buf).unwrap()
    }
}
impl TtoU8 for SnapshotMetadata {
    fn t_to_u8(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode(&mut buf).unwrap();
        buf
    }
}
impl U8toT for SnapshotMetadata {
    fn u8_to_t(data: Vec<u8>) -> Self {
        let buf = Bytes::from(data);
        prost::Message::decode(buf).unwrap()
    }
}
pub fn convert_req_to_propose_data(req: AppRequest) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    req.encode(&mut buf).unwrap();
    buf
}
pub fn convert_propose_data_to_req(data: Vec<u8>) -> AppRequest {
    let buf = Bytes::from(data);
    prost::Message::decode(buf).unwrap()
}
#[cfg(test)]
mod tests {
    use crate::app;

    use super::*;
    #[test]
    fn test_convert() {
        let mut req = AppRequest::default();
        req.id = 15;
        req.set_cmd(app::AppCmd::Remove);
        req.key = "test_convert".to_string();
        req.value = "convert_test".to_string();
        let data = convert_req_to_propose_data(req.clone());
        assert_eq!(req, convert_propose_data_to_req(data));
    }
}
