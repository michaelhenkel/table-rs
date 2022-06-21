use crate::config::config::Vmi;
use ipnet::Ipv4Net;
use crate::table::table::{Table, KeyValue,Command, calculate_hash, n_mod_m};
use crate::control::control::Route;
use crate::datapath::datapath::{Datapath,Packet,Partition};
use tokio::sync::{mpsc, oneshot};
use std::net::Ipv4Addr;
use std::collections::HashMap;
use std::sync::{Arc,Mutex, RwLock};

#[derive(Debug)]
pub enum Add{
    Vmi(Vmi),
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
                    };
                    let key_hash = calculate_hash(&flow_key);
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
        let mut vmi_table_handlers = vmi_table.run();
        join_handlers.append(&mut vmi_table_handlers);

        let mut local_route_table: Table<Ipv4Net, String> = Table::new(1);
        let mut local_route_table_handlers = local_route_table.run();
        join_handlers.append(&mut local_route_table_handlers);

        let mut remote_route_table: Table<Ipv4Net, String> = Table::new(1);
        let mut remote_route_table_handlers = remote_route_table.run();
        join_handlers.append(&mut remote_route_table_handlers);

        let mut flow_table: Table<FlowKey, String> = Table::new(self.flow_table_partitions);
        let mut flow_table_handlers = flow_table.run();
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
                        }).await;
                        route_sender.send(Action::Add(Add::Route(Route{
                            dst: vmi.clone().ip,
                            nh: name.clone(),
                        }))).unwrap();
                    },                    
                    Action::Add(Add::Route(route)) => {
                        if route.clone().nh == name.clone(){
                            let nh = vmi_table.get(route.dst).await.unwrap();
                            local_route_table.set(KeyValue{
                                key: route.dst,
                                value: nh.clone(),
                            }).await;
                            
                            let local_routes = local_route_table.list().await;
                            for local_route in local_routes {
                                if local_route.key != route.dst {
                                    let ingress_flow = FlowKey{
                                        src_prefix: local_route.key.addr(),
                                        dst_prefix: route.dst.addr(),
                                    };
                                    flow_table.set(KeyValue{
                                        key: ingress_flow,
                                        value: nh.clone(),
                                    }).await;

                                    let egress_flow = FlowKey{
                                        src_prefix: route.dst.addr(),
                                        dst_prefix: local_route.key.addr(),
                                    };
                                    flow_table.set(KeyValue{
                                        key: egress_flow,
                                        value: local_route.value,
                                    }).await;
                                }
                            }

                            let remote_routes = remote_route_table.list().await;
                            for remote_route in remote_routes {
                                let ingress_flow = FlowKey{
                                    src_prefix: remote_route.key.addr(),
                                    dst_prefix: route.dst.addr(),
                                };
                                flow_table.set(KeyValue{
                                    key: ingress_flow,
                                    value: nh.clone(),
                                }).await;

                                let egress_flow = FlowKey{
                                    src_prefix: route.dst.addr(),
                                    dst_prefix: remote_route.key.addr(),
                                };
                                flow_table.set(KeyValue{
                                    key: egress_flow,
                                    value: remote_route.value,
                                }).await;
                            }
                        } else {
                            remote_route_table.set(KeyValue{
                                key: route.dst,
                                value: route.clone().nh,
                            }).await;
                            let local_routes = local_route_table.list().await;
                            for local_route in local_routes {
                                let ingress_flow = FlowKey{
                                    src_prefix: route.dst.addr(),
                                    dst_prefix: local_route.key.addr(),
                                };
                                flow_table.set(KeyValue{
                                    key: ingress_flow,
                                    value: local_route.value,
                                }).await;

                                let egress_flow = FlowKey{
                                    src_prefix: local_route.key.addr(),
                                    dst_prefix: route.dst.addr(),
                                };
                                flow_table.set(KeyValue{
                                    key: egress_flow,
                                    value: route.nh.clone(),
                                }).await;
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
                    /* 
                    */
                    _ => {},
                }
            }
            
        });
        join_handlers.push(handle);
        join_handlers
    }
}