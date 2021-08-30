pub mod network;
pub mod app {
    tonic::include_proto!("app");
}
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
