#[derive(Debug)]
pub enum DkvrrError {
    Tonic(tonic::transport::Error),
}
impl std::error::Error for DkvrrError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}
impl std::fmt::Display for DkvrrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &*self {
            DkvrrError::Tonic(e) => write!(f, "{}", e),
        }
    }
}
impl From<tonic::transport::Error> for DkvrrError {
    fn from(e: tonic::transport::Error) -> Self {
        DkvrrError::Tonic(e)
    }
}
pub type Result<T> = std::result::Result<T, DkvrrError>;
