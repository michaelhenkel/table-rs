use crate::config::config::{Vmi,Acl,AclKey,AclValue, AclAction};
use ipnet::Ipv4Net;
use crate::table::table::{Table, KeyValue,Command, calculate_hash, n_mod_m, GenericMap, defaults, flow_map_funcs, FlowMap};
use crate::control::control::Route;
use crate::datapath::datapath::{Datapath,Partition};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use std::hash::Hash;
use std::net::Ipv4Addr;
use std::collections::{HashMap,BTreeMap};
use std::sync::{Arc, MutexGuard, Mutex as StdMutex};
use std::time::{Instant, Duration};
use futures::lock::Mutex;

const MAX_MASK: u32 = 4294967295;

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
    GetFlow(FlowKey, oneshot::Sender<(FlowKey,FlowAction)>),
    RouteList(oneshot::Sender<Vec<Route>>),
    VmiList(oneshot::Sender<Vec<Vmi>>),
    LocalRouteList(oneshot::Sender<Vec<Route>>),
    RemoteRouteList(oneshot::Sender<Vec<Route>>),
    FlowList(oneshot::Sender<HashMap<FlowKey,FlowAction>>),
    WcFlowList(oneshot::Sender<HashMap<FlowNetKey,FlowAction>>),
    GetFlowTableSenders(oneshot::Sender<HashMap<u32, tokio::sync::mpsc::UnboundedSender<Command<FlowKey, FlowAction>>>>),
    GetFallbackFlowTableSenders(oneshot::Sender<HashMap<u32, tokio::sync::mpsc::UnboundedSender<Command<FlowNetKey, FlowAction>>>>),

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
    pub src_prefix: u32,
    pub dst_prefix: u32,
    pub src_port: u16,
    pub dst_port: u16,
}

#[derive(PartialEq,Hash,Eq,Clone, Debug)]
pub enum FlowAction{
    Allow(String),
    Deny
}

impl FlowAction{
    pub fn set_allow(&mut self, nh: String){
        self::FlowAction::Allow(nh);
    }
}

impl Default for FlowAction{
    fn default() -> FlowAction {
        FlowAction::Deny
    }
}

impl Default for FlowKey{
    fn default() -> Self { 
        Self { 
            src_prefix: 0,
            dst_prefix: 0,
            src_port: 0,
            dst_port: 0,
        }
    }
}

enum MatchType{
    WC,
    SP
}

#[derive(PartialEq,Hash,Eq,Clone, Debug, Ord, PartialOrd, Default)]
pub struct FlowNetKey{
    pub src_net: u32,
    pub src_mask: u32,
    pub src_port: u16,
    pub dst_net: u32,
    pub dst_mask: u32,
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
        let net_flow_key_senders = self.get_fallback_flow_sender().await;
        

        let mut join_handlers: Vec<JoinHandle<()>> = Vec::new();
        let dp_list_clone = Arc::clone(&self.datapath_partitions);
        let dp_list = dp_list_clone.lock().await;
       
        let cn = Arc::new(StdMutex::new(HashMap::new()));
        let wc_cn = Arc::new(StdMutex::new(HashMap::new()));
        
        
        let dpl = dp_list.clone();
        let mut part = 0;
        let now_specific = Arc::new(StdMutex::new(Duration::new(0, 0)));
        let now_wc = Arc::new(StdMutex::new(Duration::new(0, 0)));

        for partition in dpl{
            let cn = Arc::clone(&cn);
            let wc_cn = Arc::clone(&wc_cn);
            let flow_key_senders = flow_key_senders.clone();
            let net_flow_key_senders = net_flow_key_senders.clone();

            let now_specific = Arc::clone(&now_specific);
            let now_wc =  Arc::clone(&now_wc);
            let res = tokio::spawn(async move {
                let mut counter: i32 = 0;
                let mut wc_counter: i32 = 0;
                let mut mis_counter: i32 = 0;
                let mut wc_mis_counter: i32 = 0;
                let mut match_type: Option<MatchType> = None;
                let started = Instant::now();
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
                    let mut found = false;
                    if res.is_ok() {
                        match res.unwrap() {
                            Some(_) => { 
                                counter = counter + 1;
                                found = true;
                                match_type = Some(MatchType::SP);
                            },
                            None => {mis_counter = mis_counter + 1},
                        }
                    }

                    if !found{
                        let net_flow_key = FlowNetKey{
                            src_net: packet.src_ip,
                            src_mask: MAX_MASK,
                            src_port: packet.src_port,
                            dst_net: packet.dst_ip,
                            dst_mask: MAX_MASK,
                            dst_port: packet.dst_port,
                        };
                        let net_flow_key_sender = net_flow_key_senders.get(&0).unwrap();
                        let (responder_sender, responder_receiver) = oneshot::channel();
                        net_flow_key_sender.send(Command::Get { key: net_flow_key, responder: responder_sender }).unwrap();
                        let res = responder_receiver.await;
                        if res.is_ok() {
                            match res.unwrap() {
                                Some(_) => {
                                    wc_counter = wc_counter + 1;
                                    found = true;
                                    match_type = Some(MatchType::WC);
                                },
                                None => {},
                            }
                        }

                    }
                    if !found {
                        wc_mis_counter = wc_mis_counter + 1;
                    }
                    match match_type{
                        Some(MatchType::SP) => {
                            let specific_ended = started.elapsed();
                            {
                                let mut now_specific = now_specific.lock().unwrap();
                                *now_specific = specific_ended + *now_specific;
                            }
                        },
                        Some(MatchType::WC) => {
                            let wc_ended = started.elapsed();
                            {
                                let mut now_wc = now_wc.lock().unwrap();
                                *now_wc = wc_ended + *now_wc;
                            }
                        },
                        None => {},
                    }
                }
                let mut cn = cn.lock().unwrap();
                cn.insert(part, (counter, mis_counter));
                let mut wc_cn = wc_cn.lock().unwrap();
                wc_cn.insert(part, (wc_counter, wc_mis_counter));
            });
            join_handlers.push(res); 
            part = part + 1;
        }
        futures::future::join_all(join_handlers).await;

        let mut specific_total_packets = 0;
        let mut specific_total_mis_packets = 0;
        let cn = cn.lock().unwrap();
        for (_, (hit,mis)) in cn.clone() {
            specific_total_packets = specific_total_packets + hit;
            specific_total_mis_packets = specific_total_mis_packets + mis;
        }
        let miss_match = specific_total_packets + specific_total_mis_packets;
        let t = now_specific.lock().unwrap();
        let specific_timer = t.clone() / miss_match as u32;
        println!("{}:",self.name);
        println!("\n  specific \n\tmatched {}\n\tmissed {}\n\tpackets in {:?}",specific_total_packets, specific_total_mis_packets, specific_timer);

        let mut wc_total_packets = 0;
        let mut wc_total_mis_packets = 0;
        let wc_cn = wc_cn.lock().unwrap();
        for (_, (hit,mis)) in wc_cn.clone() {
            wc_total_packets = wc_total_packets + hit;
            wc_total_mis_packets = wc_total_mis_packets + mis;
        }
        let miss_match = wc_total_packets + wc_total_mis_packets;
        let t = now_wc.lock().unwrap();
        let wc_timer = t.clone() / miss_match as u32;
        println!("\n  wildcard \n\tmatched {}\n\tmissed {}\n\tpackets in {:?}",wc_total_packets, wc_total_mis_packets, wc_timer);

        println!("\n  total \n\tmatched {}\n\tmissed {}\n\tpackets in {:?}\n",wc_total_packets + specific_total_packets , wc_total_mis_packets + specific_total_mis_packets, wc_timer + specific_timer);


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

    pub async fn get_flow_key_senders(&self) -> HashMap<u32, tokio::sync::mpsc::UnboundedSender<Command<FlowKey, FlowAction>>>{
        let (sender, receiver) = oneshot::channel();
        self.agent_sender.send(Action::GetFlowTableSenders(sender)).unwrap();
        let flow_table_senders = receiver.await.unwrap();
        flow_table_senders.clone()
    }

    pub async fn get_fallback_flow_sender(&self) -> HashMap<u32, tokio::sync::mpsc::UnboundedSender<Command<FlowNetKey, FlowAction>>>{
        let (sender, receiver) = oneshot::channel();
        self.agent_sender.send(Action::GetFallbackFlowTableSenders(sender)).unwrap();
        let flow_table_senders = receiver.await.unwrap();
        flow_table_senders.clone()
    }


    pub async fn get_flows(&self) -> HashMap<FlowKey,FlowAction>{
        let (sender, receiver) = oneshot::channel();
        self.agent_sender.send(Action::FlowList(sender)).unwrap();
        let flows = receiver.await.unwrap();
        flows
    }

    pub async fn get_wc_flows(&self) -> HashMap<FlowNetKey,FlowAction>{
        let (sender, receiver) = oneshot::channel();
        self.agent_sender.send(Action::WcFlowList(sender)).unwrap();
        let flows = receiver.await.unwrap();
        flows
    }

    pub fn run(&self, mut receiver: mpsc::UnboundedReceiver<Action>, route_sender: mpsc::UnboundedSender<Action>) -> Vec<tokio::task::JoinHandle<()>> {
        
        let mut join_handlers: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        let vmi_partition = HashMap::new();
        let mut vmi_table: Table<Ipv4Net, String> = Table::new(1);
        let mut vmi_table_handlers = vmi_table.run(vmi_partition, defaults::<Ipv4Net, String, HashMap<Ipv4Net,String>>());
        join_handlers.append(&mut vmi_table_handlers);

        let local_route_table_partition = HashMap::new();
        let mut local_route_table: Table<Ipv4Net, String> = Table::new(1);
        let mut local_route_table_handlers = local_route_table.run(local_route_table_partition, defaults::<Ipv4Net, String, HashMap<Ipv4Net,String>>());
        join_handlers.append(&mut local_route_table_handlers);

        let remote_route_table_partition = HashMap::new();
        let mut remote_route_table: Table<Ipv4Net, String> = Table::new(1);
        let mut remote_route_table_handlers = remote_route_table.run(remote_route_table_partition, defaults::<Ipv4Net, String, HashMap<Ipv4Net,String>>());
        join_handlers.append(&mut remote_route_table_handlers);

        let acl_partition = HashMap::new();
        let mut acl_table: Table<AclKey, AclValue> = Table::new(1);
        let mut acl_table_handlers = acl_table.run(acl_partition,defaults::<AclKey, AclValue, HashMap<AclKey,AclValue>>());
        join_handlers.append(&mut acl_table_handlers);

        
        let flow_partition = HashMap::new();
        let mut flow_table: Table<FlowKey, FlowAction> = Table::new(self.flow_table_partitions);
        let mut flow_table_handlers = flow_table.run(flow_partition, custom_flow());
        join_handlers.append(&mut flow_table_handlers);


        let wc_flow_partition = FlowMap::new();
        let mut wc_flow_table: Table<FlowNetKey, FlowAction> = Table::new(1);
        let mut wc_flow_table_handlers = wc_flow_table.run(wc_flow_partition,flow_map_funcs());
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

                            let local_flow_list = get_flows_from_route(name.clone(), local_routes, acls.clone(), route.clone(), nh.clone());
                            for (flow, nh) in local_flow_list {
                                if flow.dst_mask == MAX_MASK && flow.src_mask == MAX_MASK {
                                    let specific_flow_key = FlowKey{
                                        src_prefix: flow.src_net,
                                        dst_prefix: flow.dst_net,
                                        src_port: flow.src_port,
                                        dst_port: flow.dst_port,
                                    };
                                    flow_table.delete(specific_flow_key).await.unwrap();
                                } else {
                                    wc_flow_table.delete(flow).await.unwrap();
                                }
                            }
                            let remote_flow_list = get_flows_from_route(name.clone(), remote_routes, acls, route.clone(), nh);  
                            for (flow, nh) in remote_flow_list {
                                if flow.dst_mask == MAX_MASK && flow.src_mask == MAX_MASK {
                                    let specific_flow_key = FlowKey{
                                        src_prefix: flow.src_net,
                                        dst_prefix: flow.dst_net,
                                        src_port: flow.src_port,
                                        dst_port: flow.dst_port,
                                    };
                                    flow_table.delete(specific_flow_key).await.unwrap();                                  
                                } else {
                                    wc_flow_table.delete(flow).await.unwrap();
                                }
                            }

                            let nh = local_route_table.delete(route.dst.clone()).await.unwrap().unwrap();
                        } else {
                            remote_route_table.delete(route.dst).await.unwrap();
                            let local_routes = local_route_table.list().await;
                            let acls = acl_table.list().await;
                            let flow_list = get_flows_from_route(name.clone(), local_routes, acls, route, None);  
                            for (flow, nh) in flow_list {
                                if flow.dst_mask == MAX_MASK && flow.src_mask == MAX_MASK {
                                    let specific_flow_key = FlowKey{
                                        src_prefix: flow.src_net,
                                        dst_prefix: flow.dst_net,
                                        src_port: flow.src_port,
                                        dst_port: flow.dst_port,
                                    };
                                    flow_table.delete(specific_flow_key).await.unwrap();
                                } else {
                                    wc_flow_table.delete(flow).await.unwrap();
                                }
                            }
                        }
                    },
                    Action::Delete(Delete::Acl(acl)) => {
                        let local_routes = local_route_table.list().await;
                        let remote_routes = remote_route_table.list().await;
                        let (flows, _) = get_wc_flows_from_acl(acl.clone(), local_routes.clone(), remote_routes.clone());
                        for (flow_key, _) in flows {
                            if flow_key.dst_mask == MAX_MASK && flow_key.src_mask == MAX_MASK {
                                let specific_flow_key = FlowKey{
                                    src_prefix: flow_key.src_net,
                                    dst_prefix: flow_key.dst_net,
                                    src_port: flow_key.src_port,
                                    dst_port: flow_key.dst_port,
                                };
                                flow_table.delete(specific_flow_key).await.unwrap();
                            } else {
                                wc_flow_table.delete(flow_key).await.unwrap();
                            }
                        }
                        _ = acl_table.delete(acl.key.clone()).await;
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

                        _ = acl_table.set(KeyValue{
                            key: acl.key.clone(),
                            value: acl.value.clone(),
                        }).await;
                        let (add_flows, _) = get_wc_flows_from_acl(acl.clone(), local_routes.clone(), remote_routes.clone());
                        let (flow_list, flow_net_list) = flow_filter(local_route_table.clone(), add_flows).await;
                        for (flow, nh) in flow_list{
                            flow_table.set(KeyValue{
                                key: flow, 
                                value: nh,
                            }).await.unwrap();
                        }
                        for (flow, nh) in flow_net_list{
                            wc_flow_table.set(KeyValue{
                                key: flow, 
                                value: nh,
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
                            let flow_list = get_flows_from_route(name.clone(),local_routes, acls, route, Some(nh));  
                            for (flow, nh) in flow_list {
                                wc_flow_table.set(KeyValue{
                                    key: flow,
                                    value: nh,
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
                                let local_flow_list = get_flows_from_route(name.clone(), local_routes, acls.clone(), route.clone(), nh.clone());
                                let (flow_list, flow_net_list) = flow_filter(local_route_table.clone(), local_flow_list).await;
                                for (flow, nh) in flow_list{
                                    flow_table.set(KeyValue{
                                        key: flow, 
                                        value: nh,
                                    }).await.unwrap();
                                }
                                for (flow, nh) in flow_net_list{
                                    wc_flow_table.set(KeyValue{
                                        key: flow, 
                                        value: nh,
                                    }).await.unwrap();
                                }

                                let remote_routes = remote_route_table.list().await;
                                let remote_flow_list = get_flows_from_route(name.clone(), remote_routes, acls, route, nh);  
                                let (flow_list, flow_net_list) = flow_filter(local_route_table.clone(), remote_flow_list).await;
                                for (flow, nh) in flow_list{
                                    flow_table.set(KeyValue{
                                        key: flow, 
                                        value: nh,
                                    }).await.unwrap();
                                }
                                for (flow, nh) in flow_net_list{
                                    wc_flow_table.set(KeyValue{
                                        key: flow, 
                                        value: nh,
                                    }).await.unwrap();
                                }
                            } else {
                                remote_route_table.set(KeyValue{
                                    key: route.dst,
                                    value: route.clone().nh,
                                }).await.unwrap();

                                let local_routes = local_route_table.list().await;
                                let acls = acl_table.list().await;

                                let flow_list = get_flows_from_route(name.clone(),local_routes, acls, route, None);  
                                let (flow_list, flow_net_list) = flow_filter(local_route_table.clone(), flow_list).await;
                                for (flow, nh) in flow_list{
                                    flow_table.set(KeyValue{
                                        key: flow, 
                                        value: nh,
                                    }).await.unwrap();
                                }
                                for (flow, nh) in flow_net_list{
                                    wc_flow_table.set(KeyValue{
                                        key: flow, 
                                        value: nh,
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
                    Action::GetFallbackFlowTableSenders(sender) => {
                        let flow_table_senders = wc_flow_table.get_senders();
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


fn get_flows_from_route(agent: String, route_table: Vec<KeyValue<Ipv4Net, String>>, acls: Vec<KeyValue<AclKey, AclValue>>, route: Route, nh: Option<String>) -> Vec<(FlowNetKey,FlowAction)>{
    let mut flow_list: Vec<(FlowNetKey,FlowAction)> = Vec::new();
    let mut flow_calc = |acl: KeyValue<AclKey, AclValue>, route_entry: KeyValue<Ipv4Net, String> |{
        let mut flow_action: FlowAction;
        if acl.key.src_net.contains(&route.dst.addr()){
            if acl.key.dst_net.contains(&route_entry.key.addr()){
                match acl.value.action {
                    AclAction::Allow => {
                        flow_action = FlowAction::Allow(route_entry.value.clone());
                    },
                    AclAction::Deny => {
                        flow_action = FlowAction::Deny;
                    },
                }
                let ingress_flow = FlowNetKey{
                    src_net: as_u32_be(&route.dst.addr().octets()),
                    src_mask: as_u32_be(&route.dst.netmask().octets()),
                    dst_net: as_u32_be(&route_entry.key.addr().octets()),
                    dst_mask: as_u32_be(&route_entry.key.netmask().octets()),
                    src_port: acl.key.src_port,
                    dst_port: acl.key.dst_port,
                };
                flow_list.push((ingress_flow, flow_action));
            }
        }
        if acl.key.src_net.contains(&route_entry.key.addr()){
            if acl.key.dst_net.contains(&route.dst.addr()){
                match acl.value.action {
                    AclAction::Allow => {
                        flow_action = FlowAction::Allow(route.nh.clone());
                    },
                    AclAction::Deny => {
                        flow_action = FlowAction::Deny;
                    },
                }
                let ingress_flow = FlowNetKey{
                    src_net: as_u32_be(&&route_entry.key.addr().octets()),
                    src_mask: as_u32_be(&route_entry.key.netmask().octets()),
                    dst_net: as_u32_be(&route.dst.addr().octets()),
                    dst_mask: as_u32_be(&route.dst.netmask().octets()),
                    src_port: acl.key.src_port,
                    dst_port: acl.key.dst_port,
                };
                flow_list.push((ingress_flow, flow_action));
            }
        }
    };
    for route_entry in route_table {
        if route_entry.key != route.dst{
            let acls = acls.clone();  
            let route_entry = route_entry.clone();
            for acl in acls.clone() {
                flow_calc(acl, route_entry.clone());
            }
        }   
    }
    flow_list
}

fn get_wc_flows_from_acl(acl: Acl, local_routes: Vec<KeyValue<Ipv4Net, String>>, remote_routes: Vec<KeyValue<Ipv4Net, String>>) -> (Vec<(FlowNetKey,FlowAction)>, Vec<FlowNetKey>) {
    let mut add_flows: Vec<(FlowNetKey,FlowAction)> = Vec::new();
    let mut del_flows: Vec<FlowNetKey> = Vec::new();
    let mut src_ip_map = HashMap::new();
    let mut dst_ip_map = HashMap::new();
    for local_route in local_routes {
        if acl.key.src_net.contains(&local_route.key.addr()) {
            src_ip_map.insert(local_route.key, acl.key.src_port);
        }
        if acl.key.dst_net.contains(&local_route.key.addr()) {
            match acl.value.action {
                AclAction::Allow => {
                    dst_ip_map.insert(local_route.key, (acl.key.dst_port, FlowAction::Allow(local_route.value)));
                },
                AclAction::Deny => {
                    dst_ip_map.insert(local_route.key, (acl.key.dst_port, FlowAction::Deny));
                },
            }
        }
    }
    for remote_route in remote_routes {
        if acl.key.src_net.contains(&remote_route.key.addr()) {
            src_ip_map.insert(remote_route.key, acl.key.src_port);
        }
        if acl.key.dst_net.contains(&remote_route.key.addr()) {
            match acl.value.action {
                AclAction::Allow => {
                    dst_ip_map.insert(remote_route.key, (acl.key.dst_port, FlowAction::Allow(remote_route.value)));
                },
                AclAction::Deny => {
                    dst_ip_map.insert(remote_route.key, (acl.key.dst_port, FlowAction::Deny));
                },
            }
        }
    }
    for (src_ip, src_port) in src_ip_map{
        let dst_ip_map = dst_ip_map.clone();
        for (dst_ip, port_nh) in dst_ip_map {
            if dst_ip != src_ip {
                let delete_forward_flow = FlowNetKey{
                    src_net: as_u32_be(&src_ip.addr().octets()),
                    src_mask: as_u32_be(&src_ip.netmask().octets()),
                    dst_net: as_u32_be(&dst_ip.addr().octets()),
                    dst_mask: as_u32_be(&dst_ip.netmask().octets()),
                    src_port: 0,
                    dst_port: 0,
                };
                del_flows.push(delete_forward_flow);
                let delete_reverse_flow = FlowNetKey{
                    src_net: as_u32_be(&dst_ip.addr().octets()),
                    src_mask: as_u32_be(&dst_ip.netmask().octets()),
                    dst_net: as_u32_be(&src_ip.addr().octets()),
                    dst_mask: as_u32_be(&src_ip.netmask().octets()),
                    src_port: 0,
                    dst_port: 0,
                };
                del_flows.push(delete_reverse_flow);
                let flow_key = FlowNetKey{
                    src_net: as_u32_be(&src_ip.addr().octets()),
                    src_mask: as_u32_be(&src_ip.netmask().octets()),
                    dst_net: as_u32_be(&dst_ip.addr().octets()),
                    dst_mask: as_u32_be(&dst_ip.netmask().octets()),
                    src_port: src_port,
                    dst_port: port_nh.0,
                };
                add_flows.push((flow_key, port_nh.1));
            }
        }
    }
    (add_flows, del_flows)
}


pub async fn flow_filter(route_table: Table<Ipv4Net, String>, flow_list: Vec<(FlowNetKey, FlowAction)>) -> (Vec<(FlowKey, FlowAction)>, Vec<(FlowNetKey, FlowAction)>){
    let mut flow_key_list = Vec::new();
    let mut flow_net_key_list = Vec::new();
    for (flow, nh) in flow_list {
        let src_res = route_table.get(u32_ipv4net(flow.src_net, flow.src_mask)).await;
        let dst_res = route_table.get(u32_ipv4net(flow.dst_net, flow.dst_mask)).await;
        if src_res.is_ok() || dst_res.is_ok() {
            if flow.dst_mask == MAX_MASK && flow.src_mask == MAX_MASK {
                let specific_flow_key = FlowKey{
                    src_prefix: flow.src_net,
                    dst_prefix: flow.dst_net,
                    src_port: flow.src_port,
                    dst_port: flow.dst_port,
                };
                flow_key_list.push((specific_flow_key, nh));
            } else {
                flow_net_key_list.push((flow, nh));
            }
        }
    }
    (flow_key_list, flow_net_key_list)
}

pub fn custom_flow() -> 
    (
        impl FnMut(FlowKey, &mut HashMap<FlowKey, FlowAction>) -> Option<FlowAction> + Clone,
        impl FnMut(KeyValue<FlowKey,FlowAction>, &mut HashMap<FlowKey, FlowAction>) -> Option<FlowAction> + Clone,
        impl FnMut(FlowKey, &mut HashMap<FlowKey, FlowAction>) -> Option<FlowAction> + Clone,
        impl FnMut(&mut HashMap<FlowKey, FlowAction>) -> Option<Vec<KeyValue<FlowKey, FlowAction>>> + Clone,
        impl FnMut(&mut HashMap<FlowKey, FlowAction>) -> usize + Clone,
    )
{
    let deleter = |k: FlowKey, p: &mut HashMap<FlowKey, FlowAction>| {
        p.deleter(k)
    };

    let getter = |key: FlowKey, map: &mut HashMap<FlowKey, FlowAction>| {
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

    let setter = |k: KeyValue<FlowKey,FlowAction>, p: &mut HashMap<FlowKey, FlowAction>| {
        p.setter(k)
    };

    let lister = |p: &mut HashMap<FlowKey, FlowAction>| {
        p.lister()
    };

    let length = |p: &mut HashMap<FlowKey, FlowAction>| {
        p.length()
    };
    (getter, setter, deleter, lister, length)
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

fn u32_ipv4net(ip: u32, mask: u32) -> Ipv4Net{
    let src_prefix_length: u32;
    if mask == 0 {
        src_prefix_length = 0;
    } else {
        src_prefix_length = 32 - ((MAX_MASK - mask + 1) as f32).log2() as u32;
    }
    let octet = as_br(ip);
    Ipv4Net::new(Ipv4Addr::new(octet[0], octet[1], octet[2], octet[3]), src_prefix_length as u8).unwrap()
}