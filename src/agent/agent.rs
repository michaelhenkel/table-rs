use crate::config::config::{Vmi,Acl,AclKey,AclValue, AclAction};
use ipnet::Ipv4Net;
use crate::table::table::{Table, KeyValue,Command, calculate_hash, n_mod_m, GenericMap, defaults, flow_map_funcs};
use crate::control::control::Route;
use crate::datapath::datapath::{Datapath,Partition, Packet};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use std::hash::Hash;
use std::net::Ipv4Addr;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Instant, Duration};
use futures::lock::Mutex;
use futures::Stream;
use futures::StreamExt;
use tokio::sync::mpsc::UnboundedSender;

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

async fn send_n_partitions(partition_list: Arc<Vec<Partition>>, flow_partitions: u32, net_flow_key_senders: HashMap<u32, UnboundedSender<Command<FlowNetKey, FlowAction>>>) -> Vec<Vec<Result<Option<FlowAction>, tokio::sync::oneshot::error::RecvError>>> {
    let partition_list_len = partition_list.len();
    send_partition_futures(partition_list, flow_partitions, net_flow_key_senders).take(partition_list_len).buffer_unordered(partition_list_len).collect().await
}

fn send_partition_futures(partition_list: Arc<Vec<Partition>>, flow_partitions: u32, net_flow_key_senders: HashMap<u32, UnboundedSender<Command<FlowNetKey, FlowAction>>>)  -> impl Stream<Item = impl futures::Future<Output = Vec<Result<Option<FlowAction>, tokio::sync::oneshot::error::RecvError>>>> {
    futures::stream::iter(0..).map(move |i| send_partition(partition_list[i].clone(), flow_partitions, net_flow_key_senders.clone()))
}

async fn send_partition(partition: Partition, flow_partitions: u32, net_flow_key_senders: HashMap<u32, UnboundedSender<Command<FlowNetKey, FlowAction>>>) -> Vec<Result<Option<FlowAction>, tokio::sync::oneshot::error::RecvError>> {
    let mut res_list = Vec::new();
    for packet in partition.packet_list {
        let partition_key = FlowKey{
            src_prefix: packet.src_ip,
            dst_prefix: packet.dst_ip,
            src_port: 0,
            dst_port: 0,
        };
        let key_hash = calculate_hash(&partition_key);
        let part = n_mod_m(key_hash, flow_partitions.try_into().unwrap());
        let net_flow_key = FlowNetKey{
            src_net: packet.src_ip,
            src_mask: MAX_MASK,
            src_port: packet.src_port,
            dst_net: packet.dst_ip,
            dst_mask: MAX_MASK,
            dst_port: packet.dst_port,
        };
        let net_flow_key_sender = net_flow_key_senders.get(&part.try_into().unwrap()).unwrap();
        let (responder_sender, responder_receiver) = oneshot::channel();
        net_flow_key_sender.send(Command::Get { key: net_flow_key.clone(), responder: responder_sender }).unwrap();
        let res = responder_receiver.await;
        res_list.push(res)
    }
    res_list
}


async fn send_n_packets(packet_list: Vec<Packet>, flow_partitions: u32, net_flow_key_senders: HashMap<u32, UnboundedSender<Command<FlowNetKey, FlowAction>>>) -> Vec<Result<Option<FlowAction>, tokio::sync::oneshot::error::RecvError>> {
    let packet_list_len = packet_list.len();
    send_packet_futures(packet_list, flow_partitions, net_flow_key_senders).take(packet_list_len).buffer_unordered(10000).collect().await
}

fn send_packet_futures(packet_list: Vec<Packet>, flow_partitions: u32, net_flow_key_senders: HashMap<u32, UnboundedSender<Command<FlowNetKey, FlowAction>>>) -> impl Stream<Item = impl futures::Future<Output = Result<Option<FlowAction>, tokio::sync::oneshot::error::RecvError>>> {
    futures::stream::iter(0..).map(move |i| send_packet(packet_list[i].clone(), flow_partitions, net_flow_key_senders.clone()))
}

async fn send_packet(packet: Packet, flow_partitions: u32, net_flow_key_senders: HashMap<u32, UnboundedSender<Command<FlowNetKey, FlowAction>>>) -> Result<Option<FlowAction>, tokio::sync::oneshot::error::RecvError> {
    let partition_key = FlowKey{
        src_prefix: packet.src_ip,
        dst_prefix: packet.dst_ip,
        src_port: 0,
        dst_port: 0,
    };
    let key_hash = calculate_hash(&partition_key);
    let part = n_mod_m(key_hash, flow_partitions.try_into().unwrap());
    let net_flow_key = FlowNetKey{
        src_net: packet.src_ip,
        src_mask: MAX_MASK,
        src_port: packet.src_port,
        dst_net: packet.dst_ip,
        dst_mask: MAX_MASK,
        dst_port: packet.dst_port,
    };
    let net_flow_key_sender = net_flow_key_senders.get(&part.try_into().unwrap()).unwrap();
    let (responder_sender, responder_receiver) = oneshot::channel();
    net_flow_key_sender.send(Command::Get { key: net_flow_key.clone(), responder: responder_sender }).unwrap();
    let res = responder_receiver.await;
    res
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

    pub async fn get_stats(&mut self){
        let mut join_handlers = Vec::new();
        let net_flow_key_senders = self.get_fallback_flow_sender().await;
        for (_,net_flow_sender) in net_flow_key_senders {
            let (responder_sender, responder_receiver) = oneshot::channel();
            net_flow_sender.send(Command::Stats{responder: responder_sender}).unwrap();
            let res = tokio::spawn(async move {
                let res = responder_receiver.await;
                match res {
                    Ok((table_name, part_number, hits, misses)) => {
                        println!("table: {}, partition, {}, hits {}, misses {}", table_name, part_number, hits, misses);
                    },
                    _ => {},
                }
            });
            join_handlers.push(res);
        }
        futures::future::join_all(join_handlers).await;
    }


    pub async fn run_datapath(&mut self){
        let flow_partitions = self.flow_table_partitions.clone();
        let net_flow_key_senders = self.get_fallback_flow_sender().await;

        //let mut join_handlers: Vec<JoinHandle<()>> = Vec::new();
        let dp_list_clone = Arc::clone(&self.datapath_partitions);
        let dp_list = dp_list_clone.lock().await;
       
        //let wc_cn = Arc::new(StdMutex::new(HashMap::new())); 
        
        let dpl = dp_list.clone();
        //let mut part = 0;
        //let now_wc = Arc::new(StdMutex::new(Duration::new(0, 0)));
        let started = Instant::now();
        let arc_dpl = Arc::new(dpl);
        send_n_partitions(arc_dpl, flow_partitions, net_flow_key_senders).await;
        let ended = started.elapsed();
        println!("agent {}", self.name);
        println!("\n  wildcard in {:?}", ended);

        /* 
        for partition in dpl{
            let name = self.name.clone();
            let wc_cn = Arc::clone(&wc_cn);
            let net_flow_key_senders = net_flow_key_senders.clone();
            let now_wc =  Arc::clone(&now_wc);
            let res = tokio::spawn(async move {
                let mut wc_counter: i32 = 1;
                let mut wc_mis_counter: i32 = 1;
                let started = Instant::now();
                let res_list = send_n_packets(partition.packet_list, flow_partitions, net_flow_key_senders).await;
                /*
                for res in res_list {
                    if res.is_ok() {
                        match res.unwrap() {
                            Some(_) => {
                                wc_counter = wc_counter + 1;
                            },
                            None => {
                                wc_mis_counter = wc_mis_counter + 1;
                            },
                        }
                    }

                }
                */
                let wc_ended = started.elapsed();
                {
                    let mut now_wc = now_wc.lock().unwrap();
                    *now_wc = wc_ended + *now_wc;
                }
                /* 
                for packet in partition.packet_list {
                    
                    let partition_key = FlowKey{
                        src_prefix: packet.src_ip,
                        dst_prefix: packet.dst_ip,
                        src_port: 0,
                        dst_port: 0,
                    };
                    let key_hash = calculate_hash(&partition_key);
                    let part = n_mod_m(key_hash, flow_partitions.try_into().unwrap());
                    let net_flow_key = FlowNetKey{
                        src_net: packet.src_ip,
                        src_mask: MAX_MASK,
                        src_port: packet.src_port,
                        dst_net: packet.dst_ip,
                        dst_mask: MAX_MASK,
                        dst_port: packet.dst_port,
                    };
                    let net_flow_key_sender = net_flow_key_senders.get(&part.try_into().unwrap()).unwrap();
                    let (responder_sender, responder_receiver) = oneshot::channel();
                    net_flow_key_sender.send(Command::Get { key: net_flow_key.clone(), responder: responder_sender }).unwrap();
                    let res = responder_receiver.await;
                    if res.is_ok() {
                        match res.unwrap() {
                            Some(_) => {
                                wc_counter = wc_counter + 1;
                            },
                            None => {
                                wc_mis_counter = wc_mis_counter + 1;
                            },
                        }
                    }
                    let wc_ended = started.elapsed();
                    {
                        let mut now_wc = now_wc.lock().unwrap();
                        *now_wc = wc_ended + *now_wc;
                    }
                }
                */
                let mut wc_cn = wc_cn.lock().unwrap();
                wc_cn.insert(part, (wc_counter, wc_mis_counter));
            });
            join_handlers.push(res); 
            part = part + 1;
        }
        
        futures::future::join_all(join_handlers).await;


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
        println!("agent {}", self.name);
        println!("\n  wildcard \n\tmatched {}\n\tmissed {}\n\tpackets in {:?}",wc_total_packets, wc_total_mis_packets, wc_timer);
        */


        


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

        let mut vmi_table: Table<Ipv4Net, String> = Table::new("vmi_table".to_string(),1);
        let mut vmi_table_handlers = vmi_table.run(defaults::<Ipv4Net, String, HashMap<Ipv4Net,String>>());
        join_handlers.append(&mut vmi_table_handlers);

        let mut local_route_table: Table<Ipv4Net, String> = Table::new("local_route_table".to_string(),1);
        let mut local_route_table_handlers = local_route_table.run(defaults::<Ipv4Net, String, HashMap<Ipv4Net,String>>());
        join_handlers.append(&mut local_route_table_handlers);

        let mut remote_route_table: Table<Ipv4Net, String> = Table::new("remote_route_table".to_string(),1);
        let mut remote_route_table_handlers = remote_route_table.run(defaults::<Ipv4Net, String, HashMap<Ipv4Net,String>>());
        join_handlers.append(&mut remote_route_table_handlers);

        let mut acl_table: Table<AclKey, AclValue> = Table::new("acl_table".to_string(),1);
        let mut acl_table_handlers = acl_table.run(defaults::<AclKey, AclValue, HashMap<AclKey,AclValue>>());
        join_handlers.append(&mut acl_table_handlers);

        
        let mut flow_table: Table<FlowKey, FlowAction> = Table::new("flow_table".to_string(),self.flow_table_partitions);
        let mut flow_table_handlers = flow_table.run(custom_flow());
        join_handlers.append(&mut flow_table_handlers);


        let mut wc_flow_table: Table<FlowNetKey, FlowAction> = Table::new("wc_flow_table".to_string(),self.flow_table_partitions);
        let mut wc_flow_table_handlers = wc_flow_table.run(flow_map_funcs());
        join_handlers.append(&mut wc_flow_table_handlers);


        let name = self.name.clone();
        let handle = tokio::spawn(async move{
            while let Some(cmd) = receiver.recv().await {
                match cmd {
                    Action::Delete(Delete::Vmi(vmi)) => {
                        vmi_table.delete(vmi.ip, vmi.ip).await.unwrap();
                        route_sender.send(Action::Delete(Delete::Route(Route{
                            dst: vmi.clone().ip,
                            nh: name.clone(),
                        }))).unwrap();
                    },
                    Action::Delete(Delete::Route(route)) => {
                        if route.clone().nh == name.clone(){
                            let acls = acl_table.list().await;

                            let local_routes = local_route_table.list().await;
                            if local_routes.len() > 0 {
                                let flow_net_key_list = get_flows_from_route(acls.clone(), local_routes, route.clone());

                                for (partition_key, flow_key, _) in flow_net_key_list{
                                    wc_flow_table.delete(partition_key,flow_key).await.unwrap();
                                }
                            }

                            let remote_routes = remote_route_table.list().await;
                            if remote_routes.len() > 0 {
                                let flow_net_key_list = get_flows_from_route(acls.clone(), remote_routes, route.clone());
                                for (partition_key,flow_key, _) in flow_net_key_list{
                                    wc_flow_table.delete(partition_key,flow_key).await.unwrap();
                                }
                            }
                            _ = local_route_table.delete(route.dst.clone(), route.dst.clone()).await.unwrap().unwrap();
                        } else {
                            remote_route_table.delete(route.dst,route.dst).await.unwrap();
                            let local_routes = local_route_table.list().await;
                            let acls = acl_table.list().await;

                            if local_routes.len() > 0 {
                                let flow_net_key_list = get_flows_from_route(acls.clone(), local_routes, route.clone());
                                for (partition_key, flow_key, _) in flow_net_key_list{
                                    wc_flow_table.delete(partition_key,flow_key).await.unwrap();
                                }
                            }
                        }
                    },
                    Action::Delete(Delete::Acl(acl)) => {
                        let local_routes = local_route_table.list().await;
                        let remote_routes = remote_route_table.list().await;
                        let flow_net_list = get_flows_from_acl(acl.clone(), local_routes.clone(), remote_routes.clone());
                        for (partition_key, flow_key, _) in flow_net_list {
                            wc_flow_table.delete(partition_key, flow_key).await.unwrap();
                        }
                        _ = acl_table.delete(acl.key.clone(), acl.key.clone()).await;
                    },
                    Action::Add(Add::Vmi(vmi)) => {
                        let kv = KeyValue{
                            key: vmi.clone().ip,
                            value: vmi.clone().name,
                        };
                        vmi_table.set(kv.clone(), kv).await.unwrap();
                        route_sender.send(Action::Add(Add::Route(Route{
                            dst: vmi.clone().ip,
                            nh: name.clone(),
                        }))).unwrap();
                    },
                    Action::Add(Add::Acl(acl)) => {
                        let local_routes = local_route_table.list().await;
                        let remote_routes = remote_route_table.list().await;
                        let kv = KeyValue{
                            key: acl.key.clone(),
                            value: acl.value.clone(),
                        };
                        _ = acl_table.set(kv.clone(), kv).await;
                        let flow_net_list = get_flows_from_acl(acl.clone(), local_routes, remote_routes.clone());
                        for (partition_key, flow_key, nh) in flow_net_list{
                            let kv = KeyValue{
                                key: flow_key, 
                                value: nh,
                            };
                            wc_flow_table.set(partition_key, kv).await.unwrap();
                        }
                    },                      
                    Action::Add(Add::Route(mut route)) => {
                        let acls = acl_table.list().await;
                        if route.clone().nh.eq(&name.clone()){
                            // locally originated route

                            // get vmi nh
                            let nh = vmi_table.get(route.dst,route.dst).await.unwrap();

                            // add local route to route table
                            if nh.is_some() {
                                let kv = KeyValue{
                                    key: route.dst,
                                    value: nh.clone().unwrap(),
                                };
                                local_route_table.set(kv.clone(), kv).await.unwrap();
                                route.nh = nh.unwrap();
                            }
                            // setup flows for local <-> local routes

                            let local_routes = local_route_table.list().await;
                            if local_routes.len() > 0 {
                                let flow_net_key_list = get_flows_from_route(acls.clone(), local_routes, route.clone());
                                for (partition_key, flow_key, nh) in flow_net_key_list{
                                    let kv = KeyValue{
                                        key: flow_key, 
                                        value: nh,
                                    };
                                    wc_flow_table.set(partition_key, kv).await.unwrap();
                                }
                            }
                            // setup flows for local <-> remote routes
                            let remote_routes = remote_route_table.list().await;
                            if remote_routes.len() > 0 {
                                let flow_net_key_list = get_flows_from_route(acls, remote_routes, route);
                                for (partition_key, flow_key, nh) in flow_net_key_list{
                                    let kv = KeyValue{
                                        key: flow_key, 
                                        value: nh,
                                    };
                                    wc_flow_table.set(partition_key, kv).await.unwrap();
                                }
                            }
                        } else {
                            // remotely originated route
                            let kv = KeyValue{
                                key: route.dst,
                                value: route.clone().nh,
                            };
                            remote_route_table.set(kv.clone(), kv).await.unwrap();

                            // setup flows for remote <-> local routes
                            let local_routes = local_route_table.list().await;
                            if local_routes.len() > 0 {
                                let flow_net_key_list = get_flows_from_route(acls, local_routes, route);
                                for (partition_key, flow_key, nh) in flow_net_key_list{
                                    let kv = KeyValue{
                                        key: flow_key, 
                                        value: nh,
                                    };
                                    wc_flow_table.set(partition_key, kv).await.unwrap();
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
                                name: vmi.value,
                                ip: vmi.key,
                                originator: name.clone(),
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
                        let nh = flow_table.get(flow_key.clone(), flow_key.clone()).await.unwrap();
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

fn get_flows_from_acl(acl: Acl, local_routes: Vec<KeyValue<Ipv4Net, String>>, remote_routes: Vec<KeyValue<Ipv4Net, String>>) -> Vec<(FlowKey, FlowNetKey, FlowAction)> {
    let mut flow_net_keys = Vec::new();

    for route in local_routes.clone(){
        let acls = vec![KeyValue{
            key: acl.clone().key,
            value: acl.clone().value,
        }];
        let mut flow_net_key_list = get_flows_from_route(acls, local_routes.clone(), Route { dst: route.key, nh: route.value });
        flow_net_keys.append(&mut flow_net_key_list);
    }
    
    for route in local_routes.clone(){
        let acls = vec![KeyValue{
            key: acl.clone().key,
            value: acl.clone().value,
        }];
        let mut flow_net_key_list = get_flows_from_route(acls, remote_routes.clone(), Route { dst: route.key, nh: route.value });
        flow_net_keys.append(&mut flow_net_key_list);
    }

    for route in remote_routes.clone(){
        let acls = vec![KeyValue{
            key: acl.clone().key,
            value: acl.clone().value,
        }];
        let mut flow_net_key_list = get_flows_from_route(acls, local_routes.clone(), Route { dst: route.key, nh: route.value });
        flow_net_keys.append(&mut flow_net_key_list);
    }
    flow_net_keys
}

fn get_flows_from_route(acls: Vec<KeyValue<AclKey, AclValue>>, route_table: Vec<KeyValue<Ipv4Net, String>>, route: Route) -> Vec<(FlowKey, FlowNetKey, FlowAction)> {
    let mut flow_net_key_list = Vec::new();
    for route_entry in route_table {
        if route_entry.key != route.dst {
            for acl in acls.clone() {
                if acl.key.src_net.contains(&route.dst.addr()) && acl.key.dst_net.contains(&route_entry.key.addr()) {
                    let flow_action: FlowAction;
                    match acl.value.action {
                        AclAction::Allow => {
                            flow_action = FlowAction::Allow(route_entry.clone().value);
                        },
                        AclAction::Deny => {
                            flow_action = FlowAction::Deny;
                        },
                    }

                    let partition_key = FlowKey {
                        src_prefix: as_u32_be(&route.dst.addr().octets()),
                        src_port: 0,
                        dst_prefix: as_u32_be(&route_entry.key.addr().octets()),
                        dst_port: 0,
                    };

                    let flow_key = FlowNetKey {
                        src_net: as_u32_be(&acl.key.src_net.addr().octets()),
                        src_mask: as_u32_be(&acl.key.src_net.netmask().octets()),
                        src_port: acl.key.src_port,
                        dst_net: as_u32_be(&route_entry.key.addr().octets()),
                        dst_mask: as_u32_be(&route_entry.key.netmask().octets()),
                        dst_port: acl.key.dst_port,
                    };
                    flow_net_key_list.push((partition_key, flow_key, flow_action));
                }

                if acl.key.src_net.contains(&route_entry.key.addr()) && acl.key.dst_net.contains(&route.dst.addr()) {
                    let flow_action: FlowAction;
                    match acl.value.action {
                        AclAction::Allow => {
                            flow_action = FlowAction::Allow(route.clone().nh);
                        },
                        AclAction::Deny => {
                            flow_action = FlowAction::Deny;
                        },
                    }

                    let partition_key = FlowKey {
                        src_prefix: as_u32_be(&route_entry.key.addr().octets()),
                        src_port: acl.key.src_port,
                        dst_prefix: as_u32_be(&route.dst.addr().octets()),
                        dst_port: acl.key.dst_port,
                    };

                    let flow_key = FlowNetKey {
                        src_net: as_u32_be(&acl.key.src_net.addr().octets()),
                        src_mask: as_u32_be(&acl.key.src_net.netmask().octets()),
                        src_port: acl.key.src_port,
                        dst_net: as_u32_be(&route.dst.addr().octets()),
                        dst_mask: as_u32_be(&route.dst.netmask().octets()),
                        dst_port: acl.key.dst_port,
                    };
                    flow_net_key_list.push((partition_key, flow_key, flow_action));

                }
            }
        }
    }
    flow_net_key_list
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