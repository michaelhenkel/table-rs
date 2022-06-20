use crate::config::config::Vmi;
use ipnet::Ipv4Net;
use crate::table::table::{Table, KeyValue};
use crate::control::control::Route;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum Add{
    Vmi(Vmi),
    Route(Route),
}

#[derive(Debug)]
pub enum List{
    Route(),
}

#[derive(Debug)]
pub enum Action{
    Add(Add),
    List(List,oneshot::Sender<Vec<Route>>),
}

#[derive(Clone)]
pub struct Agent{
    pub name: String,
    agent_sender: mpsc::UnboundedSender<Action>,
}

impl Agent {
    pub fn new(name: String, agent_sender: mpsc::UnboundedSender<Action>) -> Self {
        Self { 
            name,
            agent_sender,
        }
    }

    pub async fn get_routes(&self) -> Vec<Route>{
        let (sender, receiver) = oneshot::channel();
        self.agent_sender.send(Action::List(List::Route(), sender)).unwrap();
        let routes = receiver.await.unwrap();
        routes
    }

    pub fn run(self, mut receiver: mpsc::UnboundedReceiver<Action>, route_sender: mpsc::UnboundedSender<Action>) -> Vec<tokio::task::JoinHandle<()>> {
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

        let handle = tokio::spawn(async move{    
            while let Some(cmd) = receiver.recv().await {
                match cmd {
                    Action::Add(Add::Vmi(vmi)) => {
                        println!("got vmi");
                        let num_of_vmis = vmi_table.len().await;
                        let vmi_name = format!("{}{}", vmi.clone().name, num_of_vmis);
                        vmi_table.set(KeyValue{
                            key: vmi.clone().ip,
                            value: vmi_name,
                        }).await;
                        println!("vmis {}", num_of_vmis);
                        route_sender.send(Action::Add(Add::Route(Route{
                            dst: vmi.clone().ip,
                            nh: self.name.clone(),
                        }))).unwrap();
                    },
                    Action::Add(Add::Route(route)) => {
                        if route.clone().nh == self.name.clone(){
                            let nh = vmi_table.get(route.dst).await.unwrap();
                            local_route_table.set(KeyValue{
                                key: route.dst,
                                value: nh,
                            }).await;
                            let num_of_routes = local_route_table.len().await;
                            println!("num of routes {}", num_of_routes);
                        } else {
                            remote_route_table.set(KeyValue{
                                key: route.dst,
                                value: route.nh,
                            }).await;
                        }
                    },
                    Action::List(List::Route(), sender) => {
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
                        println!("route list {:?}", route_list);
                        sender.send(route_list).unwrap();
                    },
                }
            }
        });
        join_handlers.push(handle);
        join_handlers
    }
}