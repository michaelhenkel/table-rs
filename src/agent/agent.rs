use crate::config::config::{Vmi,Acl,AclKey,AclValue};
use ipnet::Ipv4Net;
use crate::table::table::{Table, KeyValue,Command, calculate_hash, n_mod_m};
use crate::control::control::Route;
use crate::datapath::datapath::{Datapath,Partition};
use tokio::sync::{mpsc, oneshot};
use std::net::Ipv4Addr;
use std::collections::HashMap;
use std::sync::{Arc,Mutex, MutexGuard};

#[derive(Debug)]
pub enum Add{
    Vmi(Vmi),
    Acl(Acl),
    Route(Route),
}

#[derive(Debug)]
pub enum Get{
    Flow(FlowKey),
}


#[derive(Debug)]
pub enum Action{
    Add(Add),
    GetFlow(FlowKey, oneshot::Sender<(FlowKey,String)>),
    RouteList(oneshot::Sender<Vec<Route>>),
    LocalRouteList(oneshot::Sender<Vec<Route>>),
    RemoteRouteList(oneshot::Sender<Vec<Route>>),
    FlowList(oneshot::Sender<HashMap<FlowKey,String>>),
    GetFlowTableSenders(oneshot::Sender<HashMap<u32, tokio::sync::mpsc::UnboundedSender<Command<FlowKey, String>>>>),
}

#[derive(Clone)]
pub struct Agent{
    pub name: String,
    agent_sender: mpsc::UnboundedSender<Action>,
    flow_table_partitions: u32,
    datapath_partitions: Arc<Mutex<Vec<Partition>>>,
}

#[derive(PartialEq,Hash,Eq,Clone, Debug)]
pub struct FlowKey{
    pub src_prefix: Ipv4Addr,
    pub dst_prefix: Ipv4Addr,
    pub src_port: u16,
    pub dst_port: u16,
}

fn vmi_setter(key_value: KeyValue<Ipv4Net, String>, mut map: MutexGuard<HashMap<Ipv4Net,String>>) -> Option<String>{
    map.insert(key_value.key, key_value.value)
}

fn acl_setter(key_value: KeyValue<AclKey, AclValue>, mut map: MutexGuard<HashMap<AclKey,AclValue>>) -> Option<AclValue> {
    map.insert(key_value.key, key_value.value)
}

fn flow_setter(key_value: KeyValue<FlowKey, String>, mut map: MutexGuard<HashMap<FlowKey,String>>) -> Option<String> {
    map.insert(key_value.key, key_value.value)
}

fn vmi_finder(key: Ipv4Net, map: MutexGuard<HashMap<Ipv4Net,String>>) -> String {
    map.get(&key).unwrap().clone()
}

fn acl_finder(key: AclKey, map: MutexGuard<HashMap<AclKey,AclValue>>) -> AclValue {
    map.get(&key).unwrap().clone()
}

fn flow_finder(key: FlowKey, map: MutexGuard<HashMap<FlowKey,String>>) -> String {
    let mut mod_key = key.clone();
    mod_key.src_port = 0;
    mod_key.dst_port = 0;
    let res = map.get(&mod_key);
    match res {
        Some(nh) => { return nh.clone() },
        None => {
            mod_key.dst_port = key.dst_port;
            let res = map.get(&mod_key);
            match res {
                Some(nh) => { return nh.clone() },
                None => {
                    mod_key.src_port = key.src_port;
                    let res = map.get(&mod_key);
                    match res {
                        Some(nh) => { return nh.clone() },
                        None => {
                            for (k,v) in map.clone() {
                                println!("{:?} -> {}",k, v);
                            }
                            mod_key.src_port = key.src_port;
                            mod_key.dst_port = key.dst_port;
                            println!("{:?}",key);
                            let res = map.get(&mod_key).unwrap();
                            return res.clone();
                        },
                    }
                },
            }
        },
    }
    "".into()
}
//AclKey, AclValue

impl Agent {
    pub fn new(name: String, flow_table_partitions: u32, agent_sender: mpsc::UnboundedSender<Action>) -> Self {
        Self { 
            name,
            agent_sender,
            flow_table_partitions,
            datapath_partitions: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn run_datapath(&self) {
        let flow_partitions = self.flow_table_partitions.clone();
        let mut join_handlers = Vec::new();
        let dp_list = self.datapath_partitions.lock().unwrap();
        
        let dp_list = dp_list.clone();
    
        for partition in dp_list {
            let flow_key_senders = self.get_flow_key_senders().await;
            let name = self.name.clone();
            let res = tokio::spawn(async move {
                for packet in partition.packet_list {
                    let flow_key = FlowKey{
                        src_prefix: packet.src_ip,
                        dst_prefix: packet.dst_ip,
                        src_port: packet.src_port,
                        dst_port: packet.dst_port,
                    };
                    let part_flow_key = FlowKey{
                        src_prefix: packet.src_ip,
                        dst_prefix: packet.dst_ip,
                        src_port: 0,
                        dst_port: 0,
                    };
                    let key_hash = calculate_hash(&part_flow_key);
                    let part = n_mod_m(key_hash, flow_partitions.try_into().unwrap());
                    let sender = flow_key_senders.get(&part.try_into().unwrap()).unwrap();
                    let (responder_sender, responder_receiver) = oneshot::channel();
                    sender.send(Command::Get { key: flow_key, responder: responder_sender }).unwrap();
                    let res = responder_receiver.await.unwrap();
                    //println!("{} src {} dst {} via {}", name.clone(), packet.src_ip, packet.dst_ip, res);
                }
            });
            join_handlers.push(res);
        }

        futures::future::join_all(join_handlers).await;
    }
    

    pub async fn create_datapath(&self, num_of_packets: u32) {
        
        let mut local_routes = Vec::new();
        let mut all_routes = Vec::new();
        
        let routes = self.get_routes().await;
        for route in routes {
            all_routes.push(route.dst.addr())
        }

        let routes = self.get_local_routes().await;
        for route in routes {
            local_routes.push(route.dst.addr())
        }
        
        let mut dp = Datapath::new();
        dp.add_partitions(all_routes.clone(), local_routes.clone(), num_of_packets, self.flow_table_partitions);
        let mut dp_list = self.datapath_partitions.lock().unwrap();
        for partition in dp.partitions { 
            dp_list.push(partition);
        }
        
    }

    pub async fn get_routes(&self) -> Vec<Route>{
        let (sender, receiver) = oneshot::channel();
        self.agent_sender.send(Action::RouteList(sender)).unwrap();
        let routes = receiver.await.unwrap();
        routes
    }

    pub async fn get_local_routes(&self) -> Vec<Route>{
        let (sender, receiver) = oneshot::channel();
        self.agent_sender.send(Action::LocalRouteList(sender)).unwrap();
        let routes = receiver.await.unwrap();
        routes
    }

    pub async fn get_remote_routes(&self) -> Vec<Route>{
        let (sender, receiver) = oneshot::channel();
        self.agent_sender.send(Action::RemoteRouteList(sender)).unwrap();
        let routes = receiver.await.unwrap();
        routes
    }

    pub async fn get_flow_key_senders(&self) -> HashMap<u32, tokio::sync::mpsc::UnboundedSender<Command<FlowKey, String>>>{
        let (sender, receiver) = oneshot::channel();
        self.agent_sender.send(Action::GetFlowTableSenders(sender)).unwrap();
        let flow_table_senders = receiver.await.unwrap();
        flow_table_senders.clone()
    }

    pub async fn match_flow(&self, flow_key: FlowKey) -> String {
        let (sender, receiver) = oneshot::channel();
        self.agent_sender.send(Action::GetFlow(flow_key, sender)).unwrap();
        let (flow_key, nh) = receiver.await.unwrap();
        nh
    }

    pub async fn get_flows(&self) -> HashMap<FlowKey,String>{
        let (sender, receiver) = oneshot::channel();
        self.agent_sender.send(Action::FlowList(sender)).unwrap();
        let flows = receiver.await.unwrap();
        flows
    }

    pub fn run(&self, mut receiver: mpsc::UnboundedReceiver<Action>, route_sender: mpsc::UnboundedSender<Action>) -> Vec<tokio::task::JoinHandle<()>> {
        let mut join_handlers: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        
        let mut vmi_table: Table<Ipv4Net, String> = Table::new(1);
        let mut vmi_table_handlers = vmi_table.run(vmi_finder, vmi_setter);
        join_handlers.append(&mut vmi_table_handlers);

        let mut local_route_table: Table<Ipv4Net, String> = Table::new(1);
        let mut local_route_table_handlers = local_route_table.run(vmi_finder, vmi_setter);
        join_handlers.append(&mut local_route_table_handlers);

        let mut remote_route_table: Table<Ipv4Net, String> = Table::new(1);
        let mut remote_route_table_handlers = remote_route_table.run(vmi_finder, vmi_setter);
        join_handlers.append(&mut remote_route_table_handlers);

        let mut acl_table: Table<AclKey, AclValue> = Table::new(1);
        let mut acl_table_handlers = acl_table.run(acl_finder, acl_setter);
        join_handlers.append(&mut acl_table_handlers);

        let mut flow_table: Table<FlowKey, String> = Table::new(self.flow_table_partitions);
        let mut flow_table_handlers = flow_table.run(flow_finder, flow_setter);
        join_handlers.append(&mut flow_table_handlers);


        let name = self.name.clone();
        let handle = tokio::spawn(async move{
            while let Some(cmd) = receiver.recv().await {
                match cmd {
                    Action::Add(Add::Vmi(vmi)) => {
                        let num_of_vmis = vmi_table.len().await;
                        let vmi_name = format!("{}{}", vmi.clone().name, num_of_vmis);
                        vmi_table.set(KeyValue{
                            key: vmi.clone().ip,
                            value: vmi_name,
                        }).await.unwrap();
                        route_sender.send(Action::Add(Add::Route(Route{
                            dst: vmi.clone().ip,
                            nh: name.clone(),
                        }))).unwrap();
                    },
                    Action::Add(Add::Acl(acl)) => {
                        let local_routes = local_route_table.list().await;
                        let remote_routes = remote_route_table.list().await;

                        let res = acl_table.set(KeyValue{
                            key: acl.key.clone(),
                            value: acl.value.clone(),
                        }).await;
                        match res {
                            Ok(res) => {
                                println!("updating acl");
                                match res {
                                    Some(acl_value) => {
                                        let acl = Acl{
                                            key: acl.key.clone(),
                                            value: acl_value,
                                            agent: acl.agent.clone(),
                                        };
                                        let (add_flows, _) = get_flows_from_acl(acl, local_routes.clone(), remote_routes.clone());
                                        for (flow_key,_) in add_flows{
                                            flow_table.delete(flow_key).await;
                                        }
                                    },
                                    None => {},
                                };
                            },
                            Err(_) => {},
                        };
                        let (add_flows, delete_flows) = get_flows_from_acl(acl, local_routes, remote_routes);
                        for flow_key in delete_flows{
                            flow_table.delete(flow_key).await;
                        }
                        for (flow_key,nh) in add_flows{
                            flow_table.set(KeyValue{
                                key: flow_key,
                                value: nh,
                            }).await.unwrap();
                        }

                    },                      
                    Action::Add(Add::Route(route)) => {
                        if route.clone().nh == name.clone(){
                            let nh = vmi_table.get(route.dst).await.unwrap();
                            local_route_table.set(KeyValue{
                                key: route.dst,
                                value: nh.clone(),
                            }).await.unwrap();
                            
                            let local_routes = local_route_table.list().await;
                            let remote_routes = remote_route_table.list().await;
                            let acls = acl_table.list().await;

                            for local_route in local_routes {
                                let acls = acls.clone();
                                if local_route.key != route.dst {
                                    let mut src_port = 0;
                                    let mut dst_port = 0;
                                    for acl in acls {
                                        if acl.key.src_net.contains(&local_route.key.addr()){
                                            if acl.key.dst_net.contains(&route.dst.addr()){
                                                src_port = acl.value.src_port;
                                                dst_port = acl.value.dst_port;
                                            }
                                        }
                                    }

                                    let ingress_flow = FlowKey{
                                        src_prefix: local_route.key.addr(),
                                        dst_prefix: route.dst.addr(),
                                        src_port: src_port,
                                        dst_port: dst_port,
                                    };
                                    flow_table.set(KeyValue{
                                        key: ingress_flow,
                                        value: nh.clone(),
                                    }).await.unwrap();

                                    if src_port == 0 && dst_port == 0 {
                                        let egress_flow = FlowKey{
                                            src_prefix: route.dst.addr(),
                                            dst_prefix: local_route.key.addr(),
                                            src_port: 0,
                                            dst_port: 0,
                                        };
                                        flow_table.set(KeyValue{
                                            key: egress_flow,
                                            value: local_route.value,
                                        }).await.unwrap();
                                    }
                                }
                            }
                            
                            for remote_route in remote_routes {
                                let acls = acls.clone();
                                let mut src_port = 0;
                                let mut dst_port = 0;
                                for acl in acls {
                                    if acl.key.src_net.contains(&remote_route.key.addr()){
                                        if acl.key.dst_net.contains(&route.dst.addr()){
                                            src_port = acl.value.src_port;
                                            dst_port = acl.value.dst_port;
                                        }
                                    }
                                }
                                let ingress_flow = FlowKey{
                                    src_prefix: remote_route.key.addr(),
                                    dst_prefix: route.dst.addr(),
                                    src_port: src_port,
                                    dst_port: dst_port,
                                };
                                flow_table.set(KeyValue{
                                    key: ingress_flow,
                                    value: nh.clone(),
                                }).await.unwrap();

                                if dst_port == 0 && src_port == 0 {
                                    let egress_flow = FlowKey{
                                        src_prefix: route.dst.addr(),
                                        dst_prefix: remote_route.key.addr(),
                                        src_port: 0,
                                        dst_port: 0,
                                    };
                                    flow_table.set(KeyValue{
                                        key: egress_flow,
                                        value: remote_route.value,
                                    }).await.unwrap();
                                }
                            }
                        } else {
                            remote_route_table.set(KeyValue{
                                key: route.dst,
                                value: route.clone().nh,
                            }).await.unwrap();

                            let local_routes = local_route_table.list().await;
                            for local_route in local_routes {
                                let acls = acl_table.list().await;
                                let mut src_port = 0;
                                let mut dst_port = 0;
                                for acl in acls {
                                    if acl.key.src_net.contains(&local_route.key.addr()){
                                        if acl.key.dst_net.contains(&route.dst.addr()){
                                            src_port = acl.value.src_port;
                                            dst_port = acl.value.dst_port;
                                        }
                                    }
                                }
                                let ingress_flow = FlowKey{
                                    src_prefix: route.dst.addr(),
                                    dst_prefix: local_route.key.addr(),
                                    src_port: src_port,
                                    dst_port: dst_port,
                                };
                                flow_table.set(KeyValue{
                                    key: ingress_flow,
                                    value: local_route.value,
                                }).await.unwrap();

                                if dst_port == 0 && src_port == 0 {
                                    let egress_flow = FlowKey{
                                        src_prefix: local_route.key.addr(),
                                        dst_prefix: route.dst.addr(),
                                        src_port: 0,
                                        dst_port: 0,
                                    };
                                    flow_table.set(KeyValue{
                                        key: egress_flow,
                                        value: route.nh.clone(),
                                    }).await.unwrap();
                                }
                            }
                        }
                    },
                    Action::RouteList(sender) => {
                        let mut route_list = Vec::new();
                        let local_route_table_list = local_route_table.list().await;
                        for r in local_route_table_list {
                            route_list.push(Route{
                                dst: r.key,
                                nh: r.value,
                            });
                        }
                        let remote_route_table_list = remote_route_table.list().await;
                        for r in remote_route_table_list {
                            route_list.push(Route{
                                dst: r.key,
                                nh: r.value,
                            });
                        }
                        sender.send(route_list).unwrap();
                    },
                    Action::LocalRouteList(sender) => {
                        let mut route_list = Vec::new();
                        let local_route_table_list = local_route_table.list().await;
                        for r in local_route_table_list {
                            route_list.push(Route{
                                dst: r.key,
                                nh: r.value,
                            });
                        }
                        sender.send(route_list).unwrap();
                    },
                    Action::RemoteRouteList(sender) => {
                        let mut route_list = Vec::new();
                        let remote_route_table_list = remote_route_table.list().await;
                        for r in remote_route_table_list {
                            route_list.push(Route{
                                dst: r.key,
                                nh: r.value,
                            });
                        }
                        sender.send(route_list).unwrap();
                    },
                    Action::FlowList(sender) => {
                        let mut flow_list = HashMap::new();
                        let flows = flow_table.list().await;
                        for flow in flows {
                            flow_list.insert(flow.key, flow.value);
                        }
                        sender.send(flow_list).unwrap();
                    },
                    Action::GetFlow(flow_key, sender) => {
                        let nh = flow_table.get(flow_key.clone()).await.unwrap();
                        sender.send((flow_key, nh)).unwrap(); 
                    },
                    Action::GetFlowTableSenders(sender) => {
                        let flow_table_senders = flow_table.get_senders();
                        sender.send(flow_table_senders.clone()).unwrap(); 
                    },
                    _ => {},
                }
            }
            
        });
        join_handlers.push(handle);
        join_handlers
    }
}

struct port_nh{
    port: u16,
    nh: String,
}

fn get_flows_from_acl(acl: Acl, local_routes: Vec<KeyValue<Ipv4Net, String>>, remote_routes: Vec<KeyValue<Ipv4Net, String>>) -> (Vec<(FlowKey,String)>, Vec<FlowKey>) {
    let mut src_ip_map = HashMap::new();
    let mut dst_ip_map = HashMap::new();
    for local_route in local_routes {
        if acl.key.src_net.contains(&local_route.key.addr()) {
            src_ip_map.insert(local_route.key.addr(), acl.value.src_port);
        }
        if acl.key.dst_net.contains(&local_route.key.addr()) {
            dst_ip_map.insert(local_route.key.addr(), (acl.value.dst_port, local_route.value));
        }
    }
    for remote_route in remote_routes {
        if acl.key.src_net.contains(&remote_route.key.addr()) {
            src_ip_map.insert(remote_route.key.addr(), acl.value.src_port);
        }
        if acl.key.dst_net.contains(&remote_route.key.addr()) {
            dst_ip_map.insert(remote_route.key.addr(), (acl.value.dst_port, remote_route.value));
        }
    }
    let mut flow_add_list: Vec<(FlowKey,String)> = Vec::new();
    let mut flow_delete_list = Vec::new();
    for (src_ip, src_port) in src_ip_map{
        let dst_ip_map = dst_ip_map.clone();
        for (dst_ip, port_nh) in dst_ip_map {
            let delete_forward_flow = FlowKey{
                src_prefix: src_ip,
                dst_prefix: dst_ip,
                src_port: 0,
                dst_port: 0,
            };
            flow_delete_list.push(delete_forward_flow);
            let delete_reverse_flow = FlowKey{
                src_prefix: dst_ip,
                dst_prefix: src_ip,
                src_port: 0,
                dst_port: 0,
            };
            flow_delete_list.push(delete_reverse_flow);
            let flow_key = FlowKey{
                src_prefix: src_ip,
                dst_prefix: dst_ip,
                src_port: src_port,
                dst_port: port_nh.0,
            };
            flow_add_list.push((flow_key, port_nh.1));
        }
    }
    (flow_add_list, flow_delete_list)
}