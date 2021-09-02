use crate::app::AppRequest;
use bytes::Bytes;
use prost::Message;

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
        req.set_cmd(app::AppCmd::Delete);
        req.key = "test_convert".to_string();
        req.value = "convert_test".to_string();
        let data = convert_req_to_propose_data(req.clone());
        assert_eq!(req, convert_propose_data_to_req(data));
    }
}
