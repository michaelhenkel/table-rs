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

    //let num_partitions = 8;

    let config = Config::new();
    let control = Control::new();
    let (route_sender, route_receiver) = mpsc::unbounded_channel();

    let mut agents = Arc::new(Mutex::new(HashMap::new()));
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

    /* 
    for (name, agent) in agents.clone(){
        let routes = agent.get_routes().await;
        println!("{} routes {:?}", name, routes);
        let flows = agent.get_flows().await;
        println!("{} flows {:?}", name, flows);
    }
    */


    if args.packets > 0{
        let agents = agents.lock().unwrap();
        //let mut jh_list = Vec::new();
       
        for (_, agent) in agents.clone() {
            let now = Instant::now();
            let jh = agent.create_datapath(num_packets);
            jh.await;
            println!("millisecs {}",now.elapsed().as_millis());
        }
    }


    futures::future::join_all(agent_handlers).await;
    futures::future::join_all(control_res).await;

/* 
    let num_partitions = 8;
    let num_values = 10000;
    let num_runs: u32 = 1000000;
    let num_chunks = num_runs/num_partitions;

    let mut flow_table: Table<u32, String> = Table::new(num_partitions);

    let mut flow_table2: Table<FlowKey, String> = Table::new(num_partitions);

    let res = flow_table.run();

    let mut key_list = Vec::new();

    for n in 0..num_values {
        key_list.push(n);
    }

    for n in key_list {
        let key_value = KeyValue { key: n, value: format!("bla{}",n) };
        flow_table.set(key_value).await;
    }

    let mut sample_list = Vec::new();
    for _ in 0..num_runs {
        let mut rng = rand::thread_rng();
        let random_src_idx: usize = rng.gen_range(0..num_values.try_into().unwrap());
        sample_list.push(random_src_idx);
    }

    let mut chunk_list = Vec::new();
    for chunk in &sample_list.clone().into_iter().chunks(num_chunks.try_into().unwrap()) {
        chunk_list.push(chunk.collect::<Vec<_>>());
    }
    let now = Instant::now();

    let mut send_handlers = Vec::new();
    let flow_table = flow_table.clone();
    for chunk in chunk_list.clone() {
        println!("chunks {} size {}", chunk_list.len(), chunk.len());
        let flow_table = flow_table.clone();
        let res = tokio::spawn(async move {           
            for sample in chunk{               
                let bla = flow_table.get(sample.try_into().unwrap()).await.unwrap();        
            }
        });
        send_handlers.push(res);
    }
    futures::future::join_all(send_handlers).await;
    println!("millisecs {}",now.elapsed().as_millis());
    futures::future::join_all(res).await;
*/
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

