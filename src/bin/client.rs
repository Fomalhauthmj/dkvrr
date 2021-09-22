use dkvrr::app::*;
use std::io;
use structopt::StructOpt;
#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    #[structopt(short, long)]
    connect_addr: String,
}
#[tokio::main]
async fn main() {
    let mut id = 1;
    let opt = Opt::from_args();
    loop {
        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer).expect("read line error");
        buffer.remove(buffer.len() - 1);
        let args: Vec<&str> = buffer.split(' ').collect();
        println!("{:?}", args);
        let mut req = AppRequest::default();
        req.id = id;
        match args[0] {
            "put" => req.set_cmd(AppCmd::Put),
            "get" => req.set_cmd(AppCmd::Get),
            "delete" => req.set_cmd(AppCmd::Delete),
            _ => {
                println!("unsupported cmd");
                continue;
            }
        };
        req.key = args[1].to_string();
        if args.len() >= 3 {
            req.value = args[2].to_string();
        }
        let mut client = app_message_client::AppMessageClient::connect(opt.connect_addr.clone())
            .await
            .expect("connect error");
        let resp = client.app(req).await;
        println!("{:?}", resp);
        id += 1;
    }
}
