use rand::Rng;
use std::time::{Instant,Duration};
use itertools::Itertools;
use std::net::Ipv4Addr;
use tokio::{sync::{mpsc}, time::sleep};

mod table;
mod config;
mod agent;
mod control;

use table::table::{Table, KeyValue};
use config::config::{Config, Vmi};
use control::control::Control;
use agent::agent::{Agent,Action,Add};

#[tokio::main]
async fn main() {

    let config = Config::new();
    let control = Control::new();
    let (route_sender, route_receiver) = mpsc::unbounded_channel();
    
    let (agent_sender, agent_receiver) = mpsc::unbounded_channel();
    let agent = Agent::new("agent1".to_string());
    let agent_res = agent.run(agent_receiver, route_sender);
    
    control.add_agent("agent1".into(), agent_sender.clone());
    config.add_agent("agent1".into(), agent_sender);

    let control_res = control.run(route_receiver);

    let vmi_1 = Vmi{
        name: "vmi".into(),
        ip: "1.1.1.1/32".parse().unwrap(),
        agent: "agent1".into(),
    };
    config.add_vmi(vmi_1);

    let vmi_2 = Vmi{
        name: "vmi".into(),
        ip: "1.1.1.2/32".parse().unwrap(),
        agent: "agent1".into(),
    };
    config.add_vmi(vmi_2);

    futures::future::join_all(agent_res).await;
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

#[derive(PartialEq,Hash,Eq,Clone,Debug)]
pub struct FlowKey{
    pub src_prefix: Ipv4Addr,
    pub dst_prefix: Ipv4Addr,
}