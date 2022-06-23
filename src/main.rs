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

mod table;
mod config;
mod agent;
mod control;
mod datapath;

use table::table::{Table, KeyValue};
use config::config::{Config, Vmi, Acl, AclKey,AclValue};
use control::control::Control;
use agent::agent::{Agent,Action,Add,FlowKey};
use datapath::datapath::Datapath;
use clap::{arg, Parser, Command,Arg};

fn prompt(name:&str) -> Vec<String> {
    let mut line = String::new();
    print!("{}", name);
    std::io::stdout().flush().unwrap();
    std::io::stdin().read_line(&mut line).expect("Error: Could not read a line");
    let res: Vec<&str> = line.split(" ").collect();
    let mut res_vec = Vec::new();
    for s in res {
        res_vec.push(s.clone().trim().to_string());
    }
    return res_vec.clone()
}
 

fn cli() -> Command<'static> {
    Command::new("agent")
        .subcommand(
            Command::new("add")
                .about("A fictional versioning CLI")
                .subcommand_required(false)
                .allow_missing_positional(true)
                .arg_required_else_help(false)
                .allow_external_subcommands(true)
                .allow_invalid_utf8_for_external_subcommands(true)
                .ignore_errors(true)
                 
                .subcommand(
                    Command::new("vmi")
                        .arg_required_else_help(false)
                        .allow_missing_positional(true)
                        .about("Clones repos")
                        .arg(arg!(-n --name <NAME> "The remote to clone")).allow_missing_positional(true)
                        .arg(arg!(-i --ip <IP> "The remote to clone")).allow_missing_positional(true)
                        .arg(arg!(-a --agent <AGENT> "The remote to clone")).allow_missing_positional(true)
                )
                
                .subcommand(
                    Command::new("vmis")
                        .arg_required_else_help(false)
                        .allow_missing_positional(true)
                        .about("Clones repos")
                        .arg(arg!(-s --start <START> "The remote to clone")).allow_missing_positional(true)
                        .arg(arg!(-c --count <COUNT> "The remote to clone")).allow_missing_positional(true)
                        .arg(arg!(-a --agent <AGENT> "The remote to clone")).allow_missing_positional(true)
                )
                .subcommand(
                    Command::new("agents")
                        .arg_required_else_help(false)
                        .allow_missing_positional(true)
                        .about("Clones repos")
                        .arg(arg!(-c --count <COUNT> "The remote to clone")).allow_missing_positional(true)
                )
                .subcommand(
                    Command::new("acl")
                        .arg_required_else_help(false)
                        .allow_missing_positional(true)
                        .about("Clones repos")
                        .arg(arg!(-s --srcnet <SRCNET> "The remote to clone")).allow_missing_positional(true)
                        .arg(arg!(-d --dstnet <DSTNET> "The remote to clone")).allow_missing_positional(true)
                        .arg(arg!(-a --agent <AGENT> "The remote to clone")).allow_missing_positional(true)
                        .subcommand(
                            Command::new("policy")
                            .arg(arg!(-s --srcport <SRCPORT> "The remote to clone")).allow_missing_positional(true)
                            .arg(arg!(-d --dstport <DSTPORT> "The remote to clone")).allow_missing_positional(true)
                        )
                )
            )
        .subcommand(
            Command::new("list")
                .subcommand(
                    Command::new("agents")
                )
                .subcommand(
                    Command::new("vmis")
                )
                .subcommand(
                    Command::new("routes")
                )
                .subcommand(
                    Command::new("flows")
                )
        )

}

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

    let num_agents = args.agents;
    let num_vmis = args.vmis;
    let num_partitions = args.threads;
    let num_packets = args.packets;

    let config = Config::new();
    let mut control = Control::new();
    let (route_sender, route_receiver) = mpsc::unbounded_channel();

    let agents = Arc::new(Mutex::new(HashMap::new()));
    let mut agent_handlers = Vec::new();

    let control_res = control.run(route_receiver);

    
    loop {
        
        let input=prompt("> ");
        let matches = cli().get_matches_from(input);
        match matches.subcommand() {
            Some(("add", sub_matches)) => {
                match sub_matches.subcommand() {
                    Some(("acl", sub_matches)) => {
                        let src_net: String;
                        let dst_net: String;
                        let agent_name: String;
                        let mut src_port: u16 = 0;
                        let mut dst_port: u16 = 0;
                        let src_net_res = sub_matches.get_one::<String>("srcnet");
                        match src_net_res {
                            Some(src_net_res) => { 
                                src_net = src_net_res.clone();
                             },
                            None => {
                                println!("src net is missing");
                                continue; 
                            },
                        }
                        let dst_net_res = sub_matches.get_one::<String>("dstnet");
                        match dst_net_res {
                            Some(dst_net_res) => { 
                                dst_net = dst_net_res.clone();
                             },
                            None => {
                                println!("dst net is missing");
                                continue; 
                            },
                        }
                        let agent_res = sub_matches.get_one::<String>("agent");
                        match agent_res {
                            Some(agent_res) => { agent_name = agent_res.clone(); },
                            None => {
                                println!("agent is missing");
                                continue; 
                            },
                        }
                        match sub_matches.subcommand() {
                            Some(("policy", sub_matches)) => {
                                let src_port_res = sub_matches.get_many::<String>("srcport");
                                match src_port_res {
                                    Some(src_port_res) => { 
                                        for sp in src_port_res {
                                            src_port = sp.clone().parse().unwrap();
                                        }
                                    },
                                    None => {
                                        println!("src port is missing");
                                        continue; 
                                    },
                                }
                                let dst_port_res = sub_matches.get_many::<String>("dstport");
                                match dst_port_res {
                                    Some(dst_port_res) => { 
                                        for dp in dst_port_res {
                                            dst_port = dp.clone().parse().unwrap();
                                        }
                                    },
                                    None => {
                                        println!("dst port is missing");
                                        continue; 
                                    },
                                }
                                let acl = Acl{
                                    key: AclKey { 
                                        src_net: src_net.parse().unwrap(),
                                        dst_net: dst_net.parse().unwrap(),
                                    },
                                    value: AclValue { src_port, dst_port },
                                    agent: agent_name,
                                };
                                config.add_acl(acl);
                            },
                            _ => {},
                        }
                    },
                    Some(("agents", sub_matches)) => {
                        let count: u32;
                        let count_res = sub_matches.get_one::<String>("count");
                        match count_res {
                            Some(count_res) => {
                                count = count_res.clone().parse().unwrap(); 
                            },
                            None => {
                                println!("count is missing");
                                continue; 
                            },
                        }
                        let mut agent_count = agents.lock().unwrap().len();

                        for i in 0..count{
                            agent_count = agent_count + usize::try_from(i).unwrap();
                            let agent_name = format!("agent{}", agent_count);
                            let (agent_sender, agent_receiver) = mpsc::unbounded_channel();
                            let agent = Agent::new(agent_name.clone(), num_partitions, agent_sender.clone());
                            let mut agent_res = agent.run(agent_receiver, route_sender.clone());
                            agent_handlers.append(&mut agent_res);
                            control.add_agent(agent_name.clone(), agent_sender.clone());
                            config.add_agent(agent_name.clone(), agent_sender);
                            let mut agents = agents.lock().unwrap();
                            agents.insert(agent_name.clone(), agent);
                            println!("added agent {}",agent_name);
                        }
                    },
                    Some(("vmis", sub_matches)) => {
                        let count: u32;
                        let start_ip: ipnet::Ipv4Net;
                        let agent_name;
                        let count_res = sub_matches.get_one::<String>("count");
                        match count_res {
                            Some(count_res) => {
                                count = count_res.clone().parse().unwrap(); 
                            },
                            None => {
                                println!("count is missing");
                                continue; 
                            },
                        }
                        let start_res = sub_matches.get_one::<String>("start");
                        match start_res {
                            Some(start_res) => { start_ip = start_res.parse().unwrap(); },
                            None => {
                                println!("ip is missing");
                                continue; 
                            },
                        }
                        let agent_res = sub_matches.get_one::<String>("agent");
                        match agent_res {
                            Some(agent_res) => { agent_name = agent_res.clone(); },
                            None => {
                                println!("agent is missing");
                                continue; 
                            },
                        }
                        let agents = agents.lock().unwrap();
                        if agents.get(&agent_name).is_none() {
                            println!("agent not found");
                            continue;
                        }
                        let mut vmi_list = Vec::new();
                        for vmi in 0..count {
                            let vmi_ip: ipnet::Ipv4Net = start_ip;
                            let octets = vmi_ip.addr().octets();
                            let mut ip_bin = as_u32_be(&octets);
                            ip_bin = ip_bin +vmi;
                            let new_octets = as_br(ip_bin);
                            let new_ip = IpAddr::from(new_octets);
                            vmi_list.push(new_ip);
                            //all_ips.push(new_ip);
                            let ip = format!{"{}/32", new_ip};
                                config.clone().add_vmi(Vmi { 
                                    name: "vmi".to_string(),
                                    ip: ip.parse().unwrap(),
                                    agent: agent_name.clone(),
                            });
                        }
                        //agent_ips.insert(agent_name, vmi_list);
                    },
                    Some(("vmi", sub_matches)) => {

                        let mut vmi = Vmi{
                            name: "".into(),
                            ip: "0.0.0.0/0".parse().unwrap(),
                            agent: "".into(),
                        };
                        let name_res = sub_matches.get_one::<String>("name");
                        match name_res {
                            Some(name_res) => { vmi.name = name_res.clone(); },
                            None => {
                                println!("name is missing");
                                continue; 
                            },
                        }
                        let ip_res = sub_matches.get_one::<String>("ip");
                        match ip_res {
                            Some(ip_res) => { vmi.ip = ip_res.parse().unwrap(); },
                            None => {
                                println!("ip is missing");
                                continue; 
                            },
                        }
                        let agent_res = sub_matches.get_one::<String>("agent");
                        match agent_res {
                            Some(agent_res) => { vmi.agent = agent_res.clone(); },
                            None => {
                                println!("agent is missing");
                                continue; 
                            },
                        }
                        config.clone().add_vmi(vmi);
                    },
                    _ => { println!("not a valid command")},
                }
            },
            Some(("list", sub_matches)) => {
                match sub_matches.subcommand() {
                    Some(("agents", _)) => {
                        let agents = agents.lock().unwrap();
                        for (agent,_) in agents.clone() {
                            println!("{}", agent);
                        }
                    },
                    Some(("vmis", _)) => {
                        let agents = agents.lock().unwrap();
                        for (agent_name,agent) in agents.clone() {
                            let agent_vmi_list = agent.list_vmis().await;
                            println!("{}:", agent_name);
                            for vmi in agent_vmi_list {
                                println!("{:?}",vmi);
                            }
                        }

                    },
                    Some(("flows", _)) => {
                        let agents = agents.lock().unwrap();
                        for (agent_name,agent) in agents.clone() {
                            let agent_flow_list = agent.get_flows().await;
                            println!("{}:", agent_name);
                            for (flow_key, nh) in agent_flow_list {
                                println!("{:?} => {}",flow_key, nh);
                            }
                        }

                    },
                    Some(("routes", _)) => {
                        let agents = agents.lock().unwrap();
                        for (agent_name,agent) in agents.clone() {
                            println!("{}:", agent_name);
                            println!("  local routes:");
                            let agent_local_list = agent.get_local_routes().await;
                            for route in agent_local_list {
                                println!("{:?}",route);
                            }
                            println!("  remote routes:");
                            let agent_remote_list = agent.get_remote_routes().await;
                            for route in agent_remote_list {
                                println!("{:?}",route);
                            }
                        }

                    },
                    _ => {},
                }
            },
            _ => {},
        }

 
        
    /*
        match m.get_matches().subcommand() {
            Some(("acl", sub_matches)) => {
                let src_net_res = sub_matches.get_one::<String>("srcnet");
                match src_net_res {
                    Some(src_net) => { println!("src_net {}", src_net.clone()); },
                    None => {
                        println!("src net is missing");
                        continue; 
                    },
                }
                let dst_net_res = sub_matches.get_one::<String>("dstnet");
                match dst_net_res {
                    Some(dst_net) => { println!("dst_net {}", dst_net.clone()); },
                    None => {
                        println!("dst net is missing");
                        continue; 
                    },
                }
                println!("{:?}", sub_matches);
                match sub_matches.subcommand() {
                    Some(("policy", sub_matches)) => {
                        let src_port_res = sub_matches.get_many::<String>("srcport");
                        match src_port_res {
                            Some(src_port) => { 
                                for src_port in src_port {
                                    println!("src_port {}", src_port);
                                }
                            },
                            None => {
                                println!("src port is missing");
                                continue; 
                            },
                        }
                        let dst_port_res = sub_matches.get_many::<String>("dstport");
                        match dst_port_res {
                            Some(dst_port) => { 
                                for dst_port in dst_port {
                                    println!("dst_port {}", dst_port);
                                }
                            },
                            None => {
                                println!("dst port is missing");
                                continue; 
                            },
                        }
                    },
                    _ => {},
                }
            },
            Some(("vmi", sub_matches)) => {
                
                let mut vmi = Vmi{
                    name: "".into(),
                    ip: "0.0.0.0/0".parse().unwrap(),
                    agent: "".into(),
                };
                let name_res = sub_matches.get_one::<String>("name");
                match name_res {
                    Some(name_res) => { vmi.name = name_res.clone(); },
                    None => {
                        println!("name is missing");
                        continue; 
                    },
                }
                let ip_res = sub_matches.get_one::<String>("ip");
                match ip_res {
                    Some(ip_res) => { vmi.ip = ip_res.parse().unwrap(); },
                    None => {
                        println!("ip is missing");
                        continue; 
                    },
                }
                let agent_res = sub_matches.get_one::<String>("agent");
                match agent_res {
                    Some(agent_res) => { vmi.agent = agent_res.clone(); },
                    None => {
                        println!("agent is missing");
                        continue; 
                    },
                }
                config.clone().add_vmi(vmi);

            },
            _ => { println!("not a valid command")},
        }
        */
    }


    let mut agent_ips: HashMap<String, Vec<IpAddr>> = HashMap::new();
    let mut all_ips = Vec::new();

    println!("creating config");

    let acl_3 = Acl{
        agent: "agent1".into(),
        key: AclKey { 
            src_net: "1.1.1.1/32".parse().unwrap(),
            dst_net: "2.1.1.0/32".parse().unwrap(),
        },
        value: AclValue { 
            src_port: 22,
            dst_port: 83,
        },
    };
    config.clone().add_acl(acl_3);

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
    println!("flows before acl update");
    for (name, agent) in agents.clone(){
        let res = tokio::spawn(async move{
            let flows = agent.get_flows().await;
            println!("{} flows {:?}", name, flows.len());
        });
        handlers.push(res);
    }
    futures::future::join_all(handlers).await;
 
    let acl_1 = Acl{
        agent: "agent0".into(),
        key: AclKey { 
            src_net: "1.1.1.0/24".parse().unwrap(),
            dst_net: "2.1.1.0/24".parse().unwrap(),
        },
        value: AclValue { 
            src_port: 0,
            dst_port: 80,
        },
    };
    config.clone().add_acl(acl_1);

    let mut acl_2 = Acl{
        agent: "agent1".into(),
        key: AclKey { 
            src_net: "1.1.1.0/24".parse().unwrap(),
            dst_net: "2.1.1.0/24".parse().unwrap(),
        },
        value: AclValue { 
            src_port: 0,
            dst_port: 80,
        },
    };
    config.clone().add_acl(acl_2.clone());

    acl_2.value.dst_port = 81;
    config.clone().add_acl(acl_2);



    sleep(Duration::from_secs(2)).await;

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
     
    for (name, agent) in agents.clone(){
        let routes = agent.get_routes().await;
        println!("{} routes {:?}", name, routes);
        let flows = agent.get_flows().await;
        for (flow_key, nh) in flows{
            println!("{} {:?} {}", name, flow_key, nh);
        }
        
    }
    
    
    
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

