use rand::Rng;
use std::sync::{Arc,Mutex};
use std::{time::{Instant,Duration}, collections::HashMap};
use itertools::Itertools;
use std::net::Ipv4Addr;
use tokio::{sync::{mpsc}, time::sleep};
use std::net::IpAddr;
use terminal_cli;
use std::io::Write;
use std::convert::TryFrom;

use shellfish::{Command as ShellCommand, Shell};
use std::convert::TryInto;
use std::error::Error;
use std::fmt;
use std::ops::AddAssign;
use std::pin::Pin;

mod table;
mod config;
mod agent;
mod control;
mod datapath;
mod cli;

use table::table::{Table, KeyValue};
use config::config::{Config, Vmi, Acl, AclKey,AclValue};
use control::control::Control;
use cli::cli::Cli;
use agent::agent::{Agent,Action,Add,FlowKey};
use datapath::datapath::Datapath;
use clap::{arg, Parser, Command, SubCommand};



#[macro_use]
extern crate shellfish;


#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {

    #[clap(short, long, value_parser, default_value_t = 0)]
    agents: u32,

    #[clap(short, long, value_parser, default_value_t = 0)]
    vmis: u32,

    #[clap(short, long, value_parser, default_value_t = 1)]
    threads: u32,

    #[clap(short, long, value_parser, default_value_t = 0)]
    packets: u32,

    #[clap(short, long, value_parser, default_value_t = false)]
    stats: bool,

    #[clap(short, long, value_parser, default_value_t = false)]
    flows: bool,
}

#[tokio::main]
async fn main() {

    let args = Args::parse();

    let config = Config::new();
    let control = Control::new();
    let (route_sender, route_receiver) = mpsc::unbounded_channel();
    control.run(route_receiver);

    let cli = Cli::new(route_sender, control, config);
    cli.run().await;
}
