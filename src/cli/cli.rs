use ipnet::Ipv4Net;
use shellfish::{Command as ShellCommand, Shell};
use std::convert::TryInto;
use std::error::Error;
use clap::{arg, Command};
use std::collections::HashMap;
use std::net::IpAddr;
use tokio::{sync::mpsc};
use tokio::task::JoinHandle;
use std::time::{Instant};
use std::net::Ipv4Addr;

use crate::agent::agent::{Agent,Action};
use crate::control::control::{Control,Route};
use crate::config::config::{Config,Vmi, Acl, AclKey, AclValue, AclAction};



pub struct Cli {
    route_sender: mpsc::UnboundedSender<Action>,
    //agent_list: Arc<Mutex<HashMap<String, Agent>>>,
    agent_list: HashMap<String, (u32, Agent)>,
    agent_handlers: Vec<JoinHandle<()>>,
    control: Control,
    config: Config,
}

impl Cli {
    pub fn new(route_sender: mpsc::UnboundedSender<Action>, control: Control, config: Config) -> Self {
        Self {  
            route_sender,
            agent_handlers: Vec::new(),
            agent_list: HashMap::new(),
            control,
            config,
        }
    }

    async fn packet(&mut self, args: Vec<String>){
        let matches = packet_cli().get_matches_from(args);
        match matches.subcommand() {
            Some(("create", sub_matches)) => {
                let count: u32;
                let mut agent_list = Vec::new();
                let res = sub_matches.get_one::<String>("count");
                match res {
                    Some(res) => {
                        count = res.parse().unwrap();
                    },
                    None => {
                        count = 100;
                    }
                }
                let res = sub_matches.get_many::<String>("agent");
                match res {
                    Some(res) => {
                        for agent in res {
                            agent_list.push(agent.clone());
                        }
                    },
                    None => {
                        for (agent, _) in self.agent_list.clone() {
                            agent_list.push(agent);
                        }
                    }
                }

                let mut agent_map = self.agent_list.clone();
                    
                for agent_name in agent_list {                  
                    let agent = agent_map.get_mut(&agent_name.clone()).unwrap();
                    agent.1.create_datapath(count).await;  
                }

            },
            Some(("send", sub_matches)) => {
                let mut agent_list = Vec::new();
                let res = sub_matches.get_many::<String>("agent");
                match res {
                    Some(res) => {
                        for agent in res {
                            agent_list.push(agent.clone());
                        }
                    },
                    None => {
                        for (agent, _) in self.agent_list.clone() {
                            agent_list.push(agent);
                        }
                    }
                }
                for agent_name in agent_list {
                    let agent = self.agent_list.get(&agent_name).unwrap();
                    let mut agent = agent.clone();
                    agent.1.run_datapath().await;

                }
            }
            _ => {},
        }
    }

    async fn delete(&mut self, args: Vec<String>){
        let matches = delete_cli().get_matches_from(args);
        match matches.subcommand() {
            Some(("route", sub_matches)) => {
                println!("deleting route");
                let route: ipnet::Ipv4Net;
                let nh: String;
                let res = sub_matches.get_one::<String>("route");
                match res {
                    Some(res) => {
                        route = res.parse().unwrap();
                    },
                    None => {
                        println!("route is missing");
                        return
                    },
                }
                let res = sub_matches.get_one::<String>("nexthop");
                match res {
                    Some(res) => {
                        nh = res.parse().unwrap();
                    },
                    None => {
                        println!("nh is missing");
                        return
                    },
                }
                let route = Route{
                    dst: route,
                    nh,
                };
                self.control.del_route(route);
                
                
            },
            Some(("vmi", sub_matches)) => {
                println!("deleting vmi");
                let agent: String;
                let name: String;
                let res = sub_matches.get_one::<String>("agent");
                match res {
                    Some(res) => {
                        agent = res.clone();
                    },
                    None => {
                        println!("agent is missing");
                        return
                    },
                }
                let res = sub_matches.get_one::<String>("name");
                match res {
                    Some(res) => {
                        name = res.clone();
                    },
                    None => {
                        println!("name is missing");
                        return
                    },
                }
                let agent_clone = agent.clone();
                let name_clone = name.clone();
                for (agent_name, (_,a)) in self.agent_list.clone() {
                    let name_clone = name_clone.clone();
                    let agent_clone = agent_clone.clone();
                    if agent_name == agent_clone.clone() {
                        let vmi_list = a.list_vmis().await;
                        for vmi in vmi_list {
                            let name_clone = name_clone.clone();
                            if vmi.agent == name_clone.clone() {
                                self.config.del_vmi(vmi, agent_name.clone());
                            }
                        }
                    }
                }

            },
            Some(("acl", sub_matches)) => {
                println!("deleting acl");
                let src_net: ipnet::Ipv4Net;
                let dst_net: ipnet::Ipv4Net;
                let src_port: u16;
                let dst_port: u16;
                let mut agents: Vec<String> = Vec::new();
                let res = sub_matches.get_one::<String>("src");
                match res {
                    Some(res) => {
                        let ip_port: Vec<&str> = res.split(":").collect();
                        src_net = ip_port[0].parse().unwrap();
                        src_port = ip_port[1].parse().unwrap();
                    },
                    None => {
                        println!("source_ip:port is missing");
                        return
                    },
                }
                let res = sub_matches.get_one::<String>("dst");
                match res {
                    Some(res) => {
                        let ip_port: Vec<&str> = res.split(":").collect();
                        dst_net = ip_port[0].parse().unwrap();
                        dst_port = ip_port[1].parse().unwrap();
                    },
                    None => {
                        println!("source_ip:port is missing");
                        return
                    },
                }
                let agent_res = sub_matches.get_many::<String>("agent");
                match agent_res {
                    Some(agent_res) => { 
                        for agent in agent_res{
                            agents.push(agent.clone());
                        }
                    },
                    None => {
                        for (agent,_) in self.agent_list.clone() {
                            agents.push(agent);
                        }
                    },
                }
                for agent in agents.clone() {
                    if self.agent_list.get(&agent).is_none() {
                        println!("agent not found");
                        return
                    }
                }

                for agent_name in agents.clone() {
                    let acl = Acl{
                        key: AclKey { 
                            src_net,
                            src_port,
                            dst_net,
                            dst_port,
                        },
                        value: AclValue { 
                           action: AclAction::default() 
                        },
                        agent: agent_name,
                    };
                    self.config.del_acl(acl);
                }
            },
            _ => {},
        }
    }

    async fn list(&mut self, args: Vec<String>){
        let matches = list_cli().get_matches_from(args);
        match matches.subcommand() {
            Some(("agents", sub_matches)) => {
                for (agent, _) in self.agent_list.clone() {
                    println!("{}", agent);
                }
            },
            Some(("vmis", sub_matches)) => {
                for (agent_name, agent) in self.agent_list.clone() {
                    println!("{}:", agent_name);
                    let vmi_list = agent.1.list_vmis().await;
                    for vmi in vmi_list {
                        println!("{:?}", vmi);
                    }
                }
            },
            Some(("routes", sub_matches)) => {
                for (agent_name, agent) in self.agent_list.clone() {
                    let local_route_list = agent.1.get_local_routes().await;
                    let remote_route_list = agent.1.get_remote_routes().await;
                    println!("{} local routes", agent_name);
                    for route in local_route_list {
                        println!("{:?}", route);
                    }
                    println!("{} remote routes", agent_name);
                    for route in remote_route_list {
                        println!("{:?}", route);
                    }
                }
            },
            Some(("flows", sub_matches)) => {
                for (agent_name, agent) in self.agent_list.clone() {
                    println!("{}:", agent_name);
                    let flows_list = agent.1.get_flows().await;
                    for (flow_key, nh) in flows_list.clone() {
                        let octets = flow_key.src_prefix.to_be_bytes();
                        let src_ip = Ipv4Addr::new(octets[0],octets[1],octets[2],octets[3]);
                        let octets = flow_key.dst_prefix.to_be_bytes();
                        let dst_ip = Ipv4Addr::new(octets[0],octets[1],octets[2],octets[3]); 
                        println!("src {}:{} dst {}:{} => {:?}", src_ip, flow_key.src_port, dst_ip, flow_key.dst_port, nh);
                    }
                    println!("{}: {} flows", agent_name, flows_list.len());

                    let wc_flows_list = agent.1.get_wc_flows().await;
                    for (flow_key, nh) in wc_flows_list.clone() {
                        let max_mask: u32 = 4294967295;
                        let octets = flow_key.src_net.to_be_bytes();
                        let src_ip = Ipv4Addr::new(octets[0],octets[1],octets[2],octets[3]);
                        let src_mask: u32;
                        if flow_key.src_mask == 0 {
                            src_mask = 0;
                        } else {
                            src_mask = 32 - ((max_mask - flow_key.src_mask + 1) as f32).log2() as u32;
                        }
                        let octets = flow_key.dst_net.to_be_bytes();
                        let dst_ip = Ipv4Addr::new(octets[0],octets[1],octets[2],octets[3]); 
                        let dst_mask: u32;
                        if flow_key.dst_mask == 0 {
                            dst_mask = 0;
                        } else {
                            dst_mask = 32 - ((max_mask - flow_key.dst_mask + 1) as f32).log2() as u32;
                        }
                        println!("src {}/{}:{} {}/{}:{} => {:?}", src_ip, src_mask, flow_key.src_port, dst_ip, dst_mask, flow_key.dst_port, nh);
                    }
                    println!("{}: {} wc flows", agent_name, wc_flows_list.len());
                }
            },
            _ => {},
        }
    }

    fn add(&mut self, args: Vec<String>){
        let matches = add_cli().get_matches_from(args);
        match matches.subcommand() {
            Some(("route", sub_matches)) => {
                println!("adding route");
                let route: ipnet::Ipv4Net;
                let nh: String;
                let res = sub_matches.get_one::<String>("route");
                match res {
                    Some(res) => {
                        route = res.parse().unwrap();
                    },
                    None => {
                        println!("route is missing");
                        return
                    },
                }
                let res = sub_matches.get_one::<String>("nexthop");
                match res {
                    Some(res) => {
                        nh = res.parse().unwrap();
                    },
                    None => {
                        println!("nh is missing");
                        return
                    },
                }
                let route = Route{
                    dst: route,
                    nh,
                };
                self.control.add_route(route);
                
                
            },
            Some(("acl", sub_matches)) => {
                println!("adding acl");
                let mut src_net: Ipv4Net = Ipv4Net::default();
                let mut dst_net: Ipv4Net = Ipv4Net::default();
                let mut src_port: u16 = 0;
                let mut dst_port: u16 = 0;
                let mut action: AclAction = AclAction::default();
                let mut default = false;
                let mut agents: Vec<String> = Vec::new();
                let res = sub_matches.get_one::<String>("default");
                match res {
                    Some(_) => {

                            src_net = "0.0.0.0/0".to_string().parse().unwrap();
                            dst_net = "0.0.0.0/0".to_string().parse().unwrap();
                            src_port = 0;
                            dst_port = 0;
                            default = true;
                            action = AclAction::Allow;

                        
                    },
                    None => {},
                }
                if !default{
                    let res = sub_matches.get_one::<String>("src");
                    match res {
                        Some(res) => {
                            let ip_port: Vec<&str> = res.split(":").collect();
                            src_net = ip_port[0].parse().unwrap();
                            src_port = ip_port[1].parse().unwrap();
                        },
                        None => {
                            println!("source_ip:port is missing");
                            return
                        },
                    }
                    let res = sub_matches.get_one::<String>("dst");
                    match res {
                        Some(res) => {
                            let ip_port: Vec<&str> = res.split(":").collect();
                            dst_net = ip_port[0].parse().unwrap();
                            dst_port = ip_port[1].parse().unwrap();
                        },
                        None => {
                            println!("source_ip:port is missing");
                            return
                        },
                    }
                
                    let res = sub_matches.get_one::<String>("action");
                    match res {
                        Some(res) => {
                            match res.as_str() {
                                "allow" => {
                                    action = AclAction::Allow;
                                },
                                "deny" => {
                                    action = AclAction::Deny;
                                },
                                _ => {
                                    println!("invalid action. allow or deny");
                                    return
                                },
                            }
                        },
                        None => {
                            action = AclAction::Deny;
                        },
                    }
                }
                let agent_res = sub_matches.get_many::<String>("agent");
                match agent_res {
                    Some(agent_res) => { 
                        for agent in agent_res{
                            agents.push(agent.clone());
                        }
                    },
                    None => {
                        for (agent,_) in self.agent_list.clone() {
                            agents.push(agent);
                        }
                    },
                }
                for agent in agents.clone() {
                    if self.agent_list.get(&agent).is_none() {
                        println!("agent not found");
                        return
                    }
                }

                for agent_name in agents.clone() {
                    let acl = Acl{
                        key: AclKey { 
                            src_net,
                            src_port,
                            dst_net,
                            dst_port,
                        },
                        value: AclValue { 
                            action: action.clone()
                        },
                        agent: agent_name,
                    };
                    self.config.add_acl(acl);
                }
                
            },
            Some(("vmis", sub_matches)) => {
                let count: u32;
                let start_ip: ipnet::Ipv4Net;
                let mut agent_list = Vec::new();
                let agent_name: String;
                let count_res = sub_matches.get_one::<String>("count");
                match count_res {
                    Some(count_res) => {
                        count = count_res.clone().parse().unwrap(); 
                    },
                    None => {
                        count = 1;
                    },
                }
                let start_res = sub_matches.get_one::<String>("start");
                match start_res {
                    Some(start_res) => { start_ip = start_res.parse().unwrap(); },
                    None => {
                        
                    },
                }
                let agent_res = sub_matches.get_many::<String>("agent");
                match agent_res {
                    Some(agent_res) => { 
                        for agent_name in agent_res {
                            agent_list.push(agent_name.clone());
                        }
                    },
                    None => {
                        for (agent_name,_) in self.agent_list.clone(){
                            agent_list.push(agent_name);
                        }
                    },
                }

                for agent_name in agent_list {
                    let (idx, _) = self.agent_list.get(&agent_name).unwrap();
                    for vmi in 0..count {
                        let vmi_ip: ipnet::Ipv4Net = format!("{}.1.1.1/32", idx.clone()).parse().unwrap();
                        let octets = vmi_ip.addr().octets();
                        let mut ip_bin = as_u32_be(&octets);
                        ip_bin = ip_bin +vmi;
                        let new_octets = as_br(ip_bin);
                        let new_ip = IpAddr::from(new_octets);
                        let ip = format!{"{}/32", new_ip};
                            self.config.clone().add_vmi(Vmi { 
                                name: "vmi".to_string(),
                                ip: ip.parse().unwrap(),
                                agent: agent_name.clone(),
                        });
                    }
                }
                
            },
            Some(("agents", sub_matches)) => {
                let count: u32;
                let partitions: u32;
                let count_res = sub_matches.get_one::<String>("count");
                match count_res {
                    Some(count_res) => {
                        count = count_res.clone().parse().unwrap(); 
                    },
                    None => {
                        count = 1;
                    },
                }
                let partition_res = sub_matches.get_one::<String>("partitions");
                match partition_res {
                    Some(partition_res) => {
                        partitions = partition_res.clone().parse().unwrap(); 
                    },
                    None => {
                        partitions = 1;
                    },
                }
                let mut agent_count = self.agent_list.len();
    
                for i in 0..count{
                    agent_count = agent_count + 1;
                    let agent_name = format!("agent{}", agent_count);
                    let (agent_sender, agent_receiver) = mpsc::unbounded_channel();
                    let agent = Agent::new(agent_name.clone(), partitions, agent_sender.clone());
                    let mut agent_res = agent.run(agent_receiver, self.route_sender.clone());
                    self.agent_handlers.append(&mut agent_res);
                    self.control.add_agent(agent_name.clone(), agent_sender.clone());
                    self.config.add_agent(agent_name.clone(), agent_sender);
                    self.agent_list.insert(agent_name.clone(), (agent_count.try_into().unwrap(), agent));
                    println!("added agent {}",agent_name);
                }
            },
            _ => {},
        }
       
    
    }

    pub async fn run(self) {
        let mut shell = Shell::new_async(self, "<[agent shell]>-$ ");

        shell.commands.insert("add", ShellCommand::new(
            "descr".to_string(),
            add,
        ));

        shell.commands.insert("list", ShellCommand::new_async(
            "descr".to_string(),
            async_fn!(Cli, list)
        ));

        shell.commands.insert("packet", ShellCommand::new_async(
            "descr".to_string(),
            async_fn!(Cli, packet)
        ));
        shell.commands.insert("del", ShellCommand::new_async(
            "descr".to_string(),
            async_fn!(Cli, delete)
        ));

        shell.run_async().await;
    }
}


fn add(state: &mut Cli, args: Vec<String>) -> Result<(), Box<dyn Error>> {
    state.add(args);
    Ok(())
}

async fn list(state: &mut Cli, args: Vec<String>) -> Result<(), Box<dyn Error>> {
    state.list(args).await;
    Ok(())
}

async fn delete(state: &mut Cli, args: Vec<String>) -> Result<(), Box<dyn Error>> {
    state.delete(args).await;
    Ok(())
}

async fn packet(state: &mut Cli, args: Vec<String>) -> Result<(), Box<dyn Error>> {
    state.packet(args).await;
    Ok(())
}

fn add_cli() -> Command<'static> {
    Command::new("add")
    .ignore_errors(true)
    .subcommand(
        Command::new("agents")
        .arg_required_else_help(false)
        .allow_missing_positional(true)
        .about("Clones repos")
        .arg(arg!(-c --count <COUNT> "The remote to clone")).allow_missing_positional(true)
        .arg(arg!(-p --partitions <PARTITIONS> "The remote to clone")).arg_required_else_help(false)
    )
    .subcommand(
        Command::new("route")
        .arg_required_else_help(false)
        .allow_missing_positional(true)
        .about("Clones repos")
        .arg(arg!(-r --route <ROUTE> "The remote to clone")).allow_missing_positional(true)
        .arg(arg!(-n --nexthop <NEXTHOP> "The remote to clone")).arg_required_else_help(false)
    )
    .subcommand(
        Command::new("vmis")
        .arg_required_else_help(false)
        .allow_missing_positional(true)
        .about("Clones repos")
        .arg(arg!(-c --count <COUNT> "The remote to clone")).allow_missing_positional(true)
        .arg(arg!(-a --agent <AGENT> "The remote to clone")).arg_required_else_help(false)
        .arg(arg!(-s --start <START> "The remote to clone")).arg_required_else_help(false)
    )
    .subcommand(
        Command::new("acl")
        .arg_required_else_help(false)
        .allow_missing_positional(true)
        .about("Clones repos")
        .arg(arg!(-s --src <SRC> "The remote to clone")).allow_missing_positional(true)
        .arg(arg!(-d --dst <DST> "The remote to clone")).allow_missing_positional(true)
        .arg(arg!(-a --agent <AGENT> "The remote to clone")).arg_required_else_help(false)
        .arg(arg!(-x --action <ACTION> "The remote to clone")).arg_required_else_help(false)
        .arg(arg!(-y --default <DEFAULT> "The remote to clone")).arg_required_else_help(false)
    )
}

fn delete_cli() -> Command<'static> {
    Command::new("del")
    .ignore_errors(true)
    .subcommand(
        Command::new("acl")
        .arg_required_else_help(false)
        .allow_missing_positional(true)
        .about("Clones repos")
        .arg(arg!(-s --src <SRC> "The remote to clone")).allow_missing_positional(true)
        .arg(arg!(-d --dst <DST> "The remote to clone")).allow_missing_positional(true)
        .arg(arg!(-a --agent <AGENT> "The remote to clone")).arg_required_else_help(false)
    )
    .subcommand(
        Command::new("vmi")
        .arg_required_else_help(false)
        .allow_missing_positional(true)
        .about("Clones repos")
        .arg(arg!(-a --agent <AGENT> "The remote to clone")).arg_required_else_help(false)
        .arg(arg!(-n --name <NAME> "The remote to clone")).arg_required_else_help(false)
    )
    .subcommand(
        Command::new("route")
        .arg_required_else_help(false)
        .allow_missing_positional(true)
        .about("Clones repos")
        .arg(arg!(-r --route <ROUTE> "The remote to clone")).allow_missing_positional(true)
        .arg(arg!(-n --nexthop <NEXTHOP> "The remote to clone")).arg_required_else_help(false)
    )
}

fn list_cli() -> Command<'static> {
    Command::new("list")
    .ignore_errors(true)
    .subcommand(
        Command::new("agents")
        .arg_required_else_help(false)
        .allow_missing_positional(true)
        .about("Clones repos")
    )
    .subcommand(
        Command::new("vmis")
        .arg_required_else_help(false)
        .allow_missing_positional(true)
        .about("Clones repos")
    )
    .subcommand(
        Command::new("routes")
        .arg_required_else_help(false)
        .allow_missing_positional(true)
        .about("Clones repos")
    )
    .subcommand(
        Command::new("flows")
        .arg_required_else_help(false)
        .allow_missing_positional(true)
        .about("Clones repos")
    )
}

fn packet_cli() -> Command<'static> {
    Command::new("packet")
    .ignore_errors(true)
    .subcommand(
        Command::new("create")
        .arg_required_else_help(false)
        .allow_missing_positional(true)
        .about("Clones repos")
        .arg(arg!(-c --count <COUNT> "The remote to clone")).allow_missing_positional(true)
        .arg(arg!(-a --agent <COUNT> "The remote to clone")).allow_missing_positional(true)
    )
    .subcommand(
        Command::new("send")
        .arg_required_else_help(false)
        .allow_missing_positional(true)
        .about("Clones repos")
        .arg(arg!(-a --agent <COUNT> "The remote to clone")).allow_missing_positional(true)
    )
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
