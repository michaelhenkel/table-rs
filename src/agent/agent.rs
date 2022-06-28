use crate::config::config::{Vmi,Acl,AclKey,AclValue};
use ipnet::Ipv4Net;
use crate::table::table::{Table, KeyValue,Command, calculate_hash, n_mod_m};
use crate::control::control::Route;
use crate::datapath::datapath::{Datapath,Partition};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use std::net::Ipv4Addr;
use std::collections::HashMap;
use std::sync::{Arc, MutexGuard, Mutex as StdMutex};
use std::time::{Instant, Duration};
use futures::lock::Mutex;

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
#[derive(PartialEq)]
enum Origination {
    Local,
    Remote,
}

#[derive(Debug)]
pub enum Delete{
    Vmi(Vmi),
    Acl(Acl),
    Route(Route),
}


#[derive(Debug)]
pub enum Action{
    Add(Add),
    Delete(Delete),
    GetFlow(FlowKey, oneshot::Sender<(FlowKey,String)>),
    RouteList(oneshot::Sender<Vec<Route>>),
    VmiList(oneshot::Sender<Vec<Vmi>>),
    LocalRouteList(oneshot::Sender<Vec<Route>>),
    RemoteRouteList(oneshot::Sender<Vec<Route>>),
    FlowList(oneshot::Sender<HashMap<FlowKey,String>>),
    WcFlowList(oneshot::Sender<HashMap<FlowNetKey,String>>),
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

#[derive(PartialEq,Hash,Eq,Clone, Debug)]
pub struct FlowNetKey{
    pub src_net: Ipv4Net,
    pub dst_net: Ipv4Net,
    pub src_port: u16,
    pub dst_port: u16,
}

impl Agent {
    pub fn new(name: String, flow_table_partitions: u32, agent_sender: mpsc::UnboundedSender<Action>) -> Self {
        Self { 
            name,
            agent_sender,
            flow_table_partitions,
            datapath_partitions: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn run_datapath(&mut self){
        let flow_partitions = self.flow_table_partitions.clone();
        let flow_key_senders = self.get_flow_key_senders().await;

        let mut join_handlers: Vec<JoinHandle<()>> = Vec::new();
        let dp_list_clone = Arc::clone(&self.datapath_partitions);
        let dp_list = dp_list_clone.lock().await;
       
        let cn = Arc::new(StdMutex::new(HashMap::new()));
        
        
        let dpl = dp_list.clone();
        let mut part = 0;
        let now = Instant::now();
        for partition in dpl{
            let cn = Arc::clone(&cn);
            let flow_key_senders = flow_key_senders.clone();
            let res = tokio::spawn(async move {
                let mut counter: i32 = 0;
                let mut mis_counter: i32 = 0;
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
                    let res = responder_receiver.await;
                    if res.is_ok() {
                        res.unwrap().map_or_else(|| { mis_counter = mis_counter + 1;}, |_| { counter = counter + 1; });
                    }
                }
                let mut cn = cn.lock().unwrap();
                cn.insert(part, (counter, mis_counter));
            });
            join_handlers.push(res); 
            part = part + 1;
        }
        futures::future::join_all(join_handlers).await;
        let elapsed = now.elapsed();
        let mut total_packets = 0;
        let mut total_mis_packets = 0;
        let cn = cn.lock().unwrap();
        for (_, counter) in cn.clone() {
            total_packets = total_packets + counter.0;
            total_mis_packets = total_mis_packets + counter.1;
        }
        println!("{}: matched {}, missed {} packets in {:?}",self.name, total_packets, total_mis_packets, elapsed);
    }
    

    pub async fn create_datapath(&mut self, num_of_packets: u32) {
        
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
        let mut dp_list = self.datapath_partitions.lock().await;
        let mut part_counter = 0;
        for partition in dp.partitions { 
            println!("{}\tcreated {}\t packets in data path partition {}",self.name, partition.packet_list.len(), part_counter);
            dp_list.push(partition);
            part_counter = part_counter + 1;
        }
    }

    pub async fn get_routes(&self) -> Vec<Route>{
        let (sender, receiver) = oneshot::channel();
        self.agent_sender.send(Action::RouteList(sender)).unwrap();
        let routes = receiver.await.unwrap();
        routes
    }

    pub async fn list_vmis(&self) -> Vec<Vmi>{
        let (sender, receiver) = oneshot::channel();
        self.agent_sender.send(Action::VmiList(sender)).unwrap();
        let vmis = receiver.await.unwrap();
        vmis
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

    pub async fn get_wc_flows(&self) -> HashMap<FlowNetKey,String>{
        let (sender, receiver) = oneshot::channel();
        self.agent_sender.send(Action::WcFlowList(sender)).unwrap();
        let flows = receiver.await.unwrap();
        flows
    }

    pub fn run(&self, mut receiver: mpsc::UnboundedReceiver<Action>, route_sender: mpsc::UnboundedSender<Action>) -> Vec<tokio::task::JoinHandle<()>> {
        
        let mut join_handlers: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        let vmi_setter = |key_value: KeyValue<Ipv4Net, String>, mut map: MutexGuard<HashMap<Ipv4Net,String>>| {
            map.insert(key_value.key, key_value.value)
        };

        let vmi_finder = |key: Ipv4Net, map: MutexGuard<HashMap<Ipv4Net,String>>| {
            map.get(&key).cloned()
        };

        let acl_setter = |key_value: KeyValue<AclKey, AclValue>, mut map: MutexGuard<HashMap<AclKey,AclValue>>| {
            map.insert(key_value.key, key_value.value)
        };

        let acl_finder = |key: AclKey, map: MutexGuard<HashMap<AclKey,AclValue>>| {
            map.get(&key).cloned()
        };

        let flow_setter = |key_value: KeyValue<FlowKey, String>, mut map: MutexGuard<HashMap<FlowKey,String>>| {
            map.insert(key_value.key, key_value.value)
        };

        let flow_finder = |key: FlowKey, map: MutexGuard<HashMap<FlowKey,String>>| {
            let mut mod_key = key.clone();
            mod_key.src_port = 0;
            mod_key.dst_port = 0;

            map.get(&mod_key)
                .map_or_else(|| { mod_key.dst_port = key.dst_port; map.get(&mod_key)
                    .map_or_else(|| { mod_key.src_port = key.src_port; map.get(&mod_key)
                        .map_or_else(|| { mod_key.src_port = key.src_port; mod_key.dst_port = key.dst_port; map.get(&mod_key)}
                        ,|nh| Some(&nh))} 
                    ,|nh| Some(&nh))}
                ,|nh| Some(&nh)).cloned()
        };

        let wc_flow_setter = |key_value: KeyValue<FlowNetKey, String>, mut map: MutexGuard<HashMap<FlowNetKey,String>>| {
            map.insert(key_value.key, key_value.value)
        };

        let wc_flow_finder = |key: FlowNetKey, map: MutexGuard<HashMap<FlowNetKey,String>>| {
            let mut mod_key = key.clone();
            mod_key.src_port = 0;
            mod_key.dst_port = 0;

            map.get(&mod_key)
                .map_or_else(|| { mod_key.dst_port = key.dst_port; map.get(&mod_key)
                    .map_or_else(|| { mod_key.src_port = key.src_port; map.get(&mod_key)
                        .map_or_else(|| { mod_key.src_port = key.src_port; mod_key.dst_port = key.dst_port; map.get(&mod_key)}
                        ,|nh| Some(&nh))} 
                    ,|nh| Some(&nh))}
                ,|nh| Some(&nh)).cloned()
        };
        
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

        let mut wc_flow_table: Table<FlowNetKey, String> = Table::new(1);
        let mut wc_flow_table_handlers = wc_flow_table.run(wc_flow_finder, wc_flow_setter);
        join_handlers.append(&mut wc_flow_table_handlers);


        let name = self.name.clone();
        let handle = tokio::spawn(async move{
            while let Some(cmd) = receiver.recv().await {
                match cmd {
                    Action::Delete(Delete::Vmi(vmi)) => {
                        vmi_table.delete(vmi.ip).await.unwrap();
                        route_sender.send(Action::Delete(Delete::Route(Route{
                            dst: vmi.clone().ip,
                            nh: name.clone(),
                        }))).unwrap();
                    },
                    Action::Delete(Delete::Route(route)) => {
                        if route.clone().nh == name.clone(){
                            let nh = local_route_table.get(route.clone().dst.clone()).await.unwrap();

                            let acls = acl_table.list().await;
                            let local_routes = local_route_table.list().await;
                            let remote_routes = remote_route_table.list().await;

                            let local_flow_list = get_flows_from_route(name.clone(), local_routes, acls.clone(), route.clone(), nh.clone(), Origination::Local, Origination::Local);
                            for flow in local_flow_list {
                                if flow.0.dst_net.prefix_len() == 32 && flow.0.src_net.prefix_len() == 32 {
                                    wc_flow_table.delete(flow.0).await.unwrap();
                                } else {
                                    let specific_flow_key = FlowKey{
                                        src_prefix: flow.0.src_net.addr(),
                                        dst_prefix: flow.0.dst_net.addr(),
                                        src_port: flow.0.src_port,
                                        dst_port: flow.0.dst_port,
                                    };
                                    flow_table.delete(specific_flow_key).await.unwrap();
                                }
                            }
                            let remote_flow_list = get_flows_from_route(name.clone(), remote_routes, acls, route.clone(), nh, Origination::Remote, Origination::Local);  
                            for flow in remote_flow_list {
                                if flow.0.dst_net.prefix_len() == 32 && flow.0.src_net.prefix_len() == 32 {
                                    wc_flow_table.delete(flow.0).await.unwrap();
                                } else {
                                    let specific_flow_key = FlowKey{
                                        src_prefix: flow.0.src_net.addr(),
                                        dst_prefix: flow.0.dst_net.addr(),
                                        src_port: flow.0.src_port,
                                        dst_port: flow.0.dst_port,
                                    };
                                    flow_table.delete(specific_flow_key).await.unwrap();
                                }
                            }

                            let nh = local_route_table.delete(route.dst.clone()).await.unwrap().unwrap();
                        } else {
                            remote_route_table.delete(route.dst).await.unwrap();
                            let local_routes = local_route_table.list().await;
                            let acls = acl_table.list().await;
                            let flow_list = get_flows_from_route(name.clone(), local_routes, acls, route, None, Origination::Local, Origination::Remote);  
                            for flow in flow_list {
                                if flow.0.dst_net.prefix_len() == 32 && flow.0.src_net.prefix_len() == 32 {
                                    wc_flow_table.delete(flow.0).await.unwrap();
                                } else {
                                    let specific_flow_key = FlowKey{
                                        src_prefix: flow.0.src_net.addr(),
                                        dst_prefix: flow.0.dst_net.addr(),
                                        src_port: flow.0.src_port,
                                        dst_port: flow.0.dst_port,
                                    };
                                    flow_table.delete(specific_flow_key).await.unwrap();
                                }
                            }
                        }
                    },
                    Action::Delete(Delete::Acl(acl)) => {
                        let local_routes = local_route_table.list().await;
                        let remote_routes = remote_route_table.list().await;
                        let res = acl_table.delete(acl.key.clone()).await;
                        match res {
                            Ok(res) => {
                                match res {
                                    Some(acl_value) => {
                                        let acl = Acl{
                                            key: acl.key.clone(),
                                            value: acl_value,
                                            agent: acl.agent.clone(),
                                        };
                                        let (add_flows, _) = get_wc_flows_from_acl(acl, local_routes.clone(), remote_routes.clone());
                                        let mut vmi_map = HashMap::new();
                                        let mut remote_vmi_map = HashMap::new();
                                        for (flow_key,_) in add_flows{
                                            let res = vmi_table.get(flow_key.src_net).await;
                                            if res.is_ok(){
                                                res.unwrap().map_or_else(|| { 
                                                    for remote_route in remote_routes.clone() {
                                                        if remote_route.key.contains(&flow_key.src_net) {
                                                            remote_vmi_map.insert(remote_route.key, remote_route.value);
                                                        }
                                                    }
                                                }, |nh| { 
                                                    vmi_map.insert(flow_key.dst_net, nh);
                                                });
                                            }
                                            if flow_key.dst_net.prefix_len() == 32 && flow_key.src_net.prefix_len() == 32 {
                                                let specific_flow_key = FlowKey{
                                                    src_prefix: flow_key.src_net.addr(),
                                                    dst_prefix: flow_key.dst_net.addr(),
                                                    src_port: flow_key.src_port,
                                                    dst_port: flow_key.dst_port,
                                                };
                                                flow_table.delete(specific_flow_key).await.unwrap();
                                            } else {
                                                wc_flow_table.delete(flow_key).await.unwrap();
                                            }
                                        }
                                        for (vmi, nh) in vmi_map.clone() {
                                            let acls = acl_table.list().await;
                                            let route = Route{
                                                dst: vmi,
                                                nh: name.clone(),
                                            };
                                            let local_flow_list = get_flows_from_route(name.clone(), local_routes.clone(), acls.clone(), route.clone(), Some(nh.clone()), Origination::Local, Origination::Local);
                                            let (flow_list, flow_net_list) = flow_getter(local_route_table.clone(), local_flow_list).await;
                                            for flow in flow_list{
                                                flow_table.set(KeyValue{
                                                    key: flow.0, 
                                                    value: flow.1,
                                                }).await.unwrap();
                                            }
                                            for flow in flow_net_list{
                                                wc_flow_table.set(KeyValue{
                                                    key: flow.0, 
                                                    value: flow.1,
                                                }).await.unwrap();
                                            }
                                            let local_flow_list = get_flows_from_route(name.clone(), remote_routes.clone(), acls.clone(), route.clone(), Some(nh.clone()), Origination::Remote, Origination::Local);
                                            let (flow_list, flow_net_list) = flow_getter(local_route_table.clone(), local_flow_list).await;
                                            for flow in flow_list{
                                                flow_table.set(KeyValue{
                                                    key: flow.0, 
                                                    value: flow.1,
                                                }).await.unwrap();
                                            }
                                            for flow in flow_net_list{
                                                wc_flow_table.set(KeyValue{
                                                    key: flow.0, 
                                                    value: flow.1,
                                                }).await.unwrap();
                                            }
                                        }
                                        for (vmi, nh) in remote_vmi_map.clone() {
                                            let acls = acl_table.list().await;
                                            let route = Route{
                                                dst: vmi,
                                                nh: nh,
                                            };
                                            let local_flow_list = get_flows_from_route(name.clone(), local_routes.clone(), acls.clone(), route.clone(), None, Origination::Local, Origination::Remote);
                                            let (flow_list, flow_net_list) = flow_getter(local_route_table.clone(), local_flow_list).await;
                                            for flow in flow_list{
                                                flow_table.set(KeyValue{
                                                    key: flow.0, 
                                                    value: flow.1,
                                                }).await.unwrap();
                                            }
                                            for flow in flow_net_list{
                                                wc_flow_table.set(KeyValue{
                                                    key: flow.0, 
                                                    value: flow.1,
                                                }).await.unwrap();
                                            }
                                        }
                                    },
                                    None => {},
                                }
                            },
                            Err(e) => {},
                        } 

                    },
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
                                match res {
                                    Some(acl_value) => {
                                        let acl = Acl{
                                            key: acl.key.clone(),
                                            value: acl_value,
                                            agent: acl.agent.clone(),
                                        };
                                        let (add_flows, _) = get_wc_flows_from_acl(acl, local_routes.clone(), remote_routes.clone());
                                        for (flow_key,_) in add_flows{
                                            if flow_key.dst_net.prefix_len() == 32 && flow_key.src_net.prefix_len() == 32 {
                                                let specific_flow_key = FlowKey{
                                                    src_prefix: flow_key.src_net.addr(),
                                                    dst_prefix: flow_key.dst_net.addr(),
                                                    src_port: flow_key.src_port,
                                                    dst_port: flow_key.dst_port,
                                                };
                                                flow_table.delete(specific_flow_key).await.unwrap();
                                            } else {
                                                wc_flow_table.delete(flow_key).await.unwrap();
                                            }
                                        }
                                    },
                                    None => {},
                                };
                            },
                            Err(_) => {},
                        };
                        let (add_flows, delete_flows) = get_wc_flows_from_acl(acl.clone(), local_routes.clone(), remote_routes.clone());
                        for flow_key in delete_flows{
                            if flow_key.dst_net.prefix_len() == 32 && flow_key.src_net.prefix_len() == 32 {
                                let specific_flow_key = FlowKey{
                                    src_prefix: flow_key.src_net.addr(),
                                    dst_prefix: flow_key.dst_net.addr(),
                                    src_port: flow_key.src_port,
                                    dst_port: flow_key.dst_port,
                                };
                                flow_table.delete(specific_flow_key).await.unwrap();
                            } else {
                                wc_flow_table.delete(flow_key).await.unwrap();
                            }
                        }
                        let (flow_list, flow_net_list) = flow_getter(local_route_table.clone(), add_flows).await;
                        for flow in flow_list{
                            flow_table.set(KeyValue{
                                key: flow.0, 
                                value: flow.1,
                            }).await.unwrap();
                        }
                        for flow in flow_net_list{
                            wc_flow_table.set(KeyValue{
                                key: flow.0, 
                                value: flow.1,
                            }).await.unwrap();
                        }
                    },                      
                    Action::Add(Add::Route(route)) => {
                        if route.dst.prefix_len() != 32 {
                            remote_route_table.set(KeyValue{
                                key: route.clone().dst,
                                value: route.clone().nh,
                            }).await.unwrap();

                            let local_routes = local_route_table.list().await;
                            let acls = acl_table.list().await;
                            let nh = route.nh.clone();
                            let flow_list = get_net_flows_from_route(name.clone(),local_routes, acls, route, Some(nh), Origination::Local, Origination::Remote);  
                            for flow in flow_list {
                                wc_flow_table.set(KeyValue{
                                    key: flow.0,
                                    value: flow.1,
                                }).await.unwrap();
                            }
                        } else {
                            if route.clone().nh == name.clone(){
                                let nh = vmi_table.get(route.dst).await.unwrap();

                                if nh.is_some() {
                                    local_route_table.set(KeyValue{
                                        key: route.dst,
                                        value: nh.clone().unwrap(),
                                    }).await.unwrap();
                                }
                                let acls = acl_table.list().await;

                                let local_routes = local_route_table.list().await;
                                let local_flow_list = get_flows_from_route(name.clone(), local_routes, acls.clone(), route.clone(), nh.clone(), Origination::Local, Origination::Local);
                                let (flow_list, flow_net_list) = flow_getter(local_route_table.clone(), local_flow_list).await;
                                for flow in flow_list{
                                    flow_table.set(KeyValue{
                                        key: flow.0, 
                                        value: flow.1,
                                    }).await.unwrap();
                                }
                                for flow in flow_net_list{
                                    wc_flow_table.set(KeyValue{
                                        key: flow.0, 
                                        value: flow.1,
                                    }).await.unwrap();
                                }

                                let remote_routes = remote_route_table.list().await;
                                let remote_flow_list = get_flows_from_route(name.clone(), remote_routes, acls, route, nh, Origination::Remote, Origination::Local);  
                                let (flow_list, flow_net_list) = flow_getter(local_route_table.clone(), remote_flow_list).await;
                                for flow in flow_list{
                                    flow_table.set(KeyValue{
                                        key: flow.0, 
                                        value: flow.1,
                                    }).await.unwrap();
                                }
                                for flow in flow_net_list{
                                    wc_flow_table.set(KeyValue{
                                        key: flow.0, 
                                        value: flow.1,
                                    }).await.unwrap();
                                }
                            } else {
                                remote_route_table.set(KeyValue{
                                    key: route.dst,
                                    value: route.clone().nh,
                                }).await.unwrap();

                                let local_routes = local_route_table.list().await;
                                let acls = acl_table.list().await;

                                let flow_list = get_flows_from_route(name.clone(),local_routes, acls, route, None, Origination::Local, Origination::Remote);  
                                let (flow_list, flow_net_list) = flow_getter(local_route_table.clone(), flow_list).await;
                                for flow in flow_list{
                                    flow_table.set(KeyValue{
                                        key: flow.0, 
                                        value: flow.1,
                                    }).await.unwrap();
                                }
                                for flow in flow_net_list{
                                    wc_flow_table.set(KeyValue{
                                        key: flow.0, 
                                        value: flow.1,
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
                    Action::VmiList(sender) => {
                        let mut vmi_list = Vec::new();
                        let local_vmi_list = vmi_table.list().await;
                        for vmi in local_vmi_list {
                            vmi_list.push(Vmi{
                                name: "vmi".to_string(),
                                ip: vmi.key,
                                agent: vmi.value,
                            });
                        }
                        sender.send(vmi_list).unwrap();
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
                    Action::WcFlowList(sender) => {
                        let mut flow_list = HashMap::new();
                        let flows = wc_flow_table.list().await;
                        for flow in flows {
                            flow_list.insert(flow.key, flow.value);
                        }
                        sender.send(flow_list).unwrap();
                    },
                    Action::GetFlow(flow_key, sender) => {
                        let nh = flow_table.get(flow_key.clone()).await.unwrap();
                        if nh.is_some(){
                            sender.send((flow_key, nh.unwrap())).unwrap();
                        }
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

fn get_net_flows_from_route(agent: String, route_table: Vec<KeyValue<Ipv4Net, String>>, acls: Vec<KeyValue<AclKey, AclValue>>, route: Route, nh: Option<String>, route_table_orignation: Origination, route_origination: Origination) -> Vec<(FlowNetKey,String)>{
    let mut flow_list: Vec<(FlowNetKey,String)> = Vec::new();
    for route_entry in route_table {
        let nh = nh.clone();
        let acls = acls.clone();
        let mut src_port = 0;
        let mut dst_port = 0;
        for acl in acls.clone() {
            if acl.key.src_net.contains(&route_entry.key.addr()){
                if acl.key.dst_net.contains(&route.dst.addr()){
                    src_port = acl.value.src_port;
                    dst_port = acl.value.dst_port;
                }
            }
        }
        let ingress_flow = FlowNetKey{
            src_net: route_entry.key,
            dst_net: route.dst,
            src_port: src_port,
            dst_port: dst_port,
        };
        if nh.is_some(){
            flow_list.push((ingress_flow, nh.unwrap()));
            if src_port == 0 && dst_port == 0 {
                let egress_flow = FlowNetKey{
                    src_net: route.dst,
                    dst_net: route_entry.key,
                    src_port: 0,
                    dst_port: 0,
                };
                flow_list.push((egress_flow, route_entry.value));
            }
        }
    }
    flow_list
}


fn get_flows_from_route(agent: String, route_table: Vec<KeyValue<Ipv4Net, String>>, acls: Vec<KeyValue<AclKey, AclValue>>, route: Route, nh: Option<String>, route_table_orignation: Origination, route_origination: Origination) -> Vec<(FlowNetKey,String)>{
    let mut flow_list: Vec<(FlowNetKey,String)> = Vec::new();
    for route_entry in route_table {
        let nh = nh.clone();
        if route_entry.key != route.dst{
            let acls = acls.clone();
            let mut src_port = 0;
            let mut dst_port = 0;
            if route_table_orignation == Origination::Local && route_origination == Origination::Local{
                let route_entry = route_entry.clone();
                for acl in acls.clone() {
                    if acl.key.src_net.contains(&route_entry.key.addr()){
                        if acl.key.dst_net.contains(&route.dst.addr()){
                            src_port = acl.value.src_port;
                            dst_port = acl.value.dst_port;
                        }
                    }
                }
                let ingress_flow = FlowNetKey{
                    src_net: route_entry.key,
                    dst_net: route.dst,
                    src_port: src_port,
                    dst_port: dst_port,
                };
                if nh.is_some(){
                    flow_list.push((ingress_flow, nh.clone().unwrap()));
                    if src_port == 0 && dst_port == 0 {
                        let egress_flow = FlowNetKey{
                            src_net: route.dst,
                            dst_net: route_entry.key,
                            src_port: 0,
                            dst_port: 0,
                        };
                        flow_list.push((egress_flow, route_entry.value));
                    }
                }
            }
            if route_table_orignation == Origination::Remote && route_origination == Origination::Local{
                let route_entry = route_entry.clone();
                for acl in acls.clone() {
                    if acl.key.src_net.contains(&route.dst.addr()){
                        if acl.key.dst_net.contains(&route_entry.key.addr()){
                            src_port = acl.value.src_port;
                            dst_port = acl.value.dst_port;
                        }
                    }
                }
                let ingress_flow = FlowNetKey{
                    src_net: route.dst,
                    dst_net: route_entry.key,
                    src_port: src_port,
                    dst_port: dst_port,
                };
                flow_list.push((ingress_flow, route_entry.value));
                if nh.is_some(){
                    if dst_port == 0 && src_port == 0 {
                        let egress_flow = FlowNetKey{
                            src_net: route_entry.key,
                            dst_net: route.dst,
                            src_port: 0,
                            dst_port: 0,
                        };
                        flow_list.push((egress_flow, nh.unwrap()));
                    }
                }
            }
            if route_table_orignation == Origination::Local && route_origination == Origination::Remote{
                for acl in acls {
                    if acl.key.src_net.contains(&route.dst.addr()){
                        if acl.key.dst_net.contains(&route_entry.key.addr()){
                            src_port = acl.value.src_port;
                            dst_port = acl.value.dst_port;
                        }
                    }
                }
                let ingress_flow = FlowNetKey{
                    src_net: route.dst,
                    dst_net: route_entry.key,
                    src_port: src_port,
                    dst_port: dst_port,
                };
                flow_list.push((ingress_flow, route_entry.value));
                if dst_port == 0 && src_port == 0 {
                    let egress_flow = FlowNetKey{
                        src_net: route_entry.key,
                        dst_net: route.dst,
                        src_port: 0,
                        dst_port: 0,
                    };
                    flow_list.push((egress_flow, route.nh.clone()));
                }
            } 
        }   
    }
    flow_list
}

fn get_wc_flows_from_acl(acl: Acl, local_routes: Vec<KeyValue<Ipv4Net, String>>, remote_routes: Vec<KeyValue<Ipv4Net, String>>) -> (Vec<(FlowNetKey,String)>, Vec<FlowNetKey>) {
    let mut add_flows: Vec<(FlowNetKey,String)> = Vec::new();
    let mut del_flows: Vec<FlowNetKey> = Vec::new();
    let mut src_ip_map = HashMap::new();
    let mut dst_ip_map = HashMap::new();
    for local_route in local_routes {
        if acl.key.src_net.contains(&local_route.key.addr()) {
            src_ip_map.insert(local_route.key, acl.value.src_port);
        }
        if acl.key.dst_net.contains(&local_route.key.addr()) {
            dst_ip_map.insert(local_route.key, (acl.value.dst_port, local_route.value));
        }
    }
    for remote_route in remote_routes {
        if acl.key.src_net.contains(&remote_route.key.addr()) {
            src_ip_map.insert(remote_route.key, acl.value.src_port);
        }
        if acl.key.dst_net.contains(&remote_route.key.addr()) {
            dst_ip_map.insert(remote_route.key, (acl.value.dst_port, remote_route.value));
        }
    }
    for (src_ip, src_port) in src_ip_map{
        let dst_ip_map = dst_ip_map.clone();
        for (dst_ip, port_nh) in dst_ip_map {
            let delete_forward_flow = FlowNetKey{
                src_net: src_ip,
                dst_net: dst_ip,
                src_port: 0,
                dst_port: 0,
            };
            del_flows.push(delete_forward_flow);
            let delete_reverse_flow = FlowNetKey{
                src_net: dst_ip,
                dst_net: src_ip,
                src_port: 0,
                dst_port: 0,
            };
            del_flows.push(delete_reverse_flow);
            let flow_key = FlowNetKey{
                src_net: src_ip,
                dst_net: dst_ip,
                src_port: src_port,
                dst_port: port_nh.0,
            };
            add_flows.push((flow_key, port_nh.1));
        }
    }
    (add_flows, del_flows)
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
        if remote_route.key.prefix_len() == 32 {
            if acl.key.src_net.contains(&remote_route.key.addr()) {
                src_ip_map.insert(remote_route.key.addr(), acl.value.src_port);
            }
            if acl.key.dst_net.contains(&remote_route.key.addr()) {
                dst_ip_map.insert(remote_route.key.addr(), (acl.value.dst_port, remote_route.value));
            }
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

fn get_flows_from_acl_2(acl: Acl, local_routes: Vec<KeyValue<Ipv4Net, String>>, remote_routes: Vec<KeyValue<Ipv4Net, String>>) -> (Vec<(FlowKey,String)>, Vec<FlowKey>, Vec<(FlowNetKey,String)>, Vec<FlowNetKey>) {
    let mut src_ip_map = HashMap::new();
    let mut dst_ip_map = HashMap::new();
    let mut src_net_map = HashMap::new();
    let mut dst_net_map = HashMap::new();
    let mut flow_add_list: Vec<(FlowKey,String)> = Vec::new();
    let mut flow_delete_list = Vec::new();
    let mut flow_net_add_list: Vec<(FlowNetKey,String)> = Vec::new();
    let mut flow_net_delete_list: Vec<FlowNetKey> = Vec::new();

    for local_route in local_routes {
        if acl.key.src_net.contains(&local_route.key.addr()) {
            src_ip_map.insert(local_route.key.addr(), acl.value.src_port);
            src_net_map.insert(local_route.key, acl.value.src_port);
        }
        if acl.key.dst_net.contains(&local_route.key.addr()) {
            dst_ip_map.insert(local_route.key.addr(), (acl.value.dst_port, local_route.value.clone()));
            dst_net_map.insert(local_route.key, (acl.value.dst_port, local_route.value));
        }
    }
    for remote_route in remote_routes {
        if remote_route.key.prefix_len() == 32 {
            if acl.key.src_net.contains(&remote_route.key.addr()) {
                src_ip_map.insert(remote_route.key.addr(), acl.value.src_port);
                src_net_map.insert(remote_route.key, acl.value.src_port);
            }
            if acl.key.dst_net.contains(&remote_route.key.addr()) {
                dst_ip_map.insert(remote_route.key.addr(), (acl.value.dst_port, remote_route.value.clone()));
                dst_net_map.insert(remote_route.key, (acl.value.dst_port, remote_route.value));
            }
        }
    }

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
    (flow_add_list, flow_delete_list, flow_net_add_list, flow_net_delete_list)
}

pub async fn flow_getter(route_table: Table<Ipv4Net, String>, flow_list: Vec<(FlowNetKey, String)>) -> (Vec<(FlowKey, String)>, Vec<(FlowNetKey, String)>){
    let mut flow_key_list = Vec::new();
    let mut flow_net_key_list = Vec::new();
    for flow in flow_list {
        let src_res = route_table.get(flow.0.src_net).await;
        let dst_res = route_table.get(flow.0.dst_net).await;
        if src_res.is_ok() || dst_res.is_ok() {
            if flow.0.dst_net.prefix_len() == 32 && flow.0.src_net.prefix_len() == 32 {
                let specific_flow_key = FlowKey{
                    src_prefix: flow.0.src_net.addr(),
                    dst_prefix: flow.0.dst_net.addr(),
                    src_port: flow.0.src_port,
                    dst_port: flow.0.dst_port,
                };
                flow_key_list.push((specific_flow_key, flow.1));
            } else {
                flow_net_key_list.push((flow.0, flow.1));
            }
        }
    }
    (flow_key_list, flow_net_key_list)
}