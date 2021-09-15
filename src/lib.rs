pub mod consensus;
pub mod network;
pub mod state_machine;
pub mod storage;
mod types;
mod utils;
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
