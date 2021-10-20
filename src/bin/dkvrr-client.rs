use clap::AppSettings;
use dkvrr::{get_rpc_endpoint, DkvrrClient, Result};
use std::process::exit;
use structopt::StructOpt;

const DEFAULT_NODE_ID: &str = "1";

#[derive(StructOpt, Debug)]
#[structopt(
    name = "dkvrr-client",
    global_settings = &[AppSettings::DisableHelpSubcommand,AppSettings::VersionlessSubcommands],
)]
struct Opt {
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt, Debug)]
enum Command {
    #[structopt(name = "get", about = "Get the string value of a given string key")]
    Get {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
        #[structopt(
            long,
            help = "Sets the server address",
            default_value = DEFAULT_NODE_ID,
            parse(try_from_str)
        )]
        addr: u64,
    },
    #[structopt(name = "set", about = "Set the value of a string key to a string")]
    Set {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
        #[structopt(name = "VALUE", help = "The string value of the key")]
        value: String,
        #[structopt(
            long,
            help = "Sets the server address",
            default_value = DEFAULT_NODE_ID,
            parse(try_from_str)
        )]
        addr: u64,
    },
    #[structopt(name = "rm", about = "Remove a given string key")]
    Remove {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,
        #[structopt(
            long,
            help = "Sets the server address",
            default_value = DEFAULT_NODE_ID,
            parse(try_from_str)
        )]
        addr: u64,
    },
    #[structopt(name = "addnode", about = "Add node")]
    AddNode {
        #[structopt(name = "ID", help = "Node ID")]
        id: u64,
        #[structopt(
            long,
            help = "Sets the server address",
            default_value = DEFAULT_NODE_ID,
            parse(try_from_str)
        )]
        addr: u64,
    },
    #[structopt(name = "rmnode", about = "Remove node")]
    RemoveNode {
        #[structopt(name = "ID", help = "Node ID")]
        id: u64,
        #[structopt(
            long,
            help = "Sets the server address",
            default_value = DEFAULT_NODE_ID,
            parse(try_from_str)
        )]
        addr: u64,
    },
}
#[tokio::main]
async fn main() {
    let opt = Opt::from_args();
    if let Err(e) = run(opt).await {
        eprintln!("{}", e);
        exit(1);
    }
}

async fn run(opt: Opt) -> Result<()> {
    match opt.command {
        Command::Get { key, addr } => {
            let mut client = DkvrrClient::connect(get_rpc_endpoint(addr)).await?;
            client.get(key).await;
        }
        Command::Set { key, value, addr } => {
            let mut client = DkvrrClient::connect(get_rpc_endpoint(addr)).await?;
            client.set(key, value).await;
        }
        Command::Remove { key, addr } => {
            let mut client = DkvrrClient::connect(get_rpc_endpoint(addr)).await?;
            client.remove(key).await;
        }
        Command::AddNode { id, addr } => {
            let mut client = DkvrrClient::connect(get_rpc_endpoint(addr)).await?;
            client.add_node(id).await;
        }
        Command::RemoveNode { id, addr } => {
            let mut client = DkvrrClient::connect(get_rpc_endpoint(addr)).await?;
            client.remove_node(id).await;
        }
    }
    Ok(())
}
