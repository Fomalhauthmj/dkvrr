use std::thread;
use structopt::StructOpt;
#[derive(StructOpt, Debug)]
#[structopt(name = "dkvrr-server")]
struct Opt {
    #[structopt(short, long)]
    id: u64,
}
#[tokio::main]
async fn main() {
    let opt = Opt::from_args();
    let id = opt.id;
    let logger = init_logging();
    let server = dkvrr::DkvrrServer::new(id, logger);
    server.run();
    thread::park();
}
fn init_logging() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog::Drain::fuse(slog_term::FullFormat::new(decorator).build());
    let drain = slog::Drain::fuse(slog_async::Async::new(drain).build());

    slog::Logger::root(drain, slog::o!())
}
