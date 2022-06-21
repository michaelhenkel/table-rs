use rand::Rng;
use std::sync::{Arc,Mutex};
use std::{time::{Instant,Duration}, collections::HashMap};
use itertools::Itertools;
use std::net::Ipv4Addr;
use tokio::{sync::{mpsc}, time::sleep};
use std::net::IpAddr;

mod table;
mod config;
mod agent;
mod control;
mod datapath;

use table::table::{Table, KeyValue};
use config::config::{Config, Vmi};
use control::control::Control;
use agent::agent::{Agent,Action,Add,FlowKey};
use datapath::datapath::Datapath;
use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {

    #[clap(short, long, value_parser, default_value_t = 1)]
    agents: u32,

    #[clap(short, long, value_parser, default_value_t = 1)]
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

    let num_agents = args.agents;
    let num_vmis = args.vmis;
    let num_partitions = args.threads;
    let num_packets = args.packets;

    let config = Config::new();
    let control = Control::new();
    let (route_sender, route_receiver) = mpsc::unbounded_channel();

    let agents = Arc::new(Mutex::new(HashMap::new()));
    let mut agent_handlers = Vec::new();
    for i in 0..num_agents{
        let agent_name = format!("agent{}", i);
        let (agent_sender, agent_receiver) = mpsc::unbounded_channel();
        let agent = Agent::new(agent_name.clone(), num_partitions, agent_sender.clone());
        let mut agent_res = agent.run(agent_receiver, route_sender.clone());
        agent_handlers.append(&mut agent_res);
    
        control.add_agent(agent_name.clone(), agent_sender.clone());
        config.add_agent(agent_name.clone(), agent_sender);
        let mut agents = agents.lock().unwrap();
        agents.insert(agent_name, agent);
    }

    let control_res = control.run(route_receiver);

    let mut agent_ips: HashMap<String, Vec<IpAddr>> = HashMap::new();
    let mut all_ips = Vec::new();

    println!("creating config");
    for agent in 0..num_agents{
        let agent_name = format!("agent{}", agent);
        let mut vmi_list = Vec::new();
        for vmi in 0..num_vmis {
            let agent_ip = format!{"{}.1.1.0/32", agent+1};
            let vmi_ip: ipnet::Ipv4Net = agent_ip.parse().unwrap();
            let octets = vmi_ip.addr().octets();
            let mut ip_bin = as_u32_be(&octets);
            ip_bin = ip_bin +vmi;
            let new_octets = as_br(ip_bin);
            let new_ip = IpAddr::from(new_octets);
            vmi_list.push(new_ip);
            all_ips.push(new_ip);
            let ip = format!{"{}/32", new_ip};
                config.clone().add_vmi(Vmi { 
                    name: "vmi".to_string(),
                    ip: ip.parse().unwrap(),
                    agent: agent_name.clone(),
            });
        }
        agent_ips.insert(agent_name, vmi_list);
    }

    sleep(Duration::from_secs(3)).await;

    let agents = agents.lock().unwrap();
    let mut handlers = Vec::new();
    for (name, agent) in agents.clone(){
        
        let res = tokio::spawn(async move{
            let routes = agent.get_routes().await;
            println!("{} routes {:?}", name, routes.len());

            let flows = agent.get_flows().await;
            println!("{} flows {:?}", name, flows.len());
        });
        handlers.push(res);
    }
    futures::future::join_all(handlers).await;
    /* 
    for (name, agent) in agents.clone(){
        let routes = agent.get_routes().await;
        println!("{} routes {:?}", name, routes);
        let flows = agent.get_flows().await;
        println!("{} flows {:?}", name, flows);
    }
    */
    


    if args.packets > 0{
        println!("preparing datapath");
        let mut handlers = Vec::new();
        for (_, agent) in agents.clone() {
            let res = tokio::spawn(async move{
                agent.create_datapath(num_packets).await;
            });
            handlers.push(res);
        }
        futures::future::join_all(handlers).await;
        println!("running datapath");
        for (_, agent) in agents.clone() {
            let now = Instant::now();
            let jh = agent.run_datapath();
            jh.await;
            println!("millisecs {}",now.elapsed().as_millis());
        }
    }

    futures::future::join_all(agent_handlers).await;
    futures::future::join_all(control_res).await;

}


fn as_br(x: u32) -> [u8; 4]{
    x.to_be_bytes()
}

fn as_u32_be(array: &[u8;4]) -> u32 {
    ((array[0] as u32) << 24) +
    ((array[1] as u32) << 16) +
    ((array[2] as u32) << 8) +
    ((array[3] as u32) << 0)
}

