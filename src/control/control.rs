use tokio::sync::mpsc;
use crate::agent::agent::{Action,Add, Delete};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use crate::table::table::{Table, KeyValue, defaults};
use ipnet::Ipv4Net;


#[derive(Clone)]
pub struct Control{
    pub agent_list: Arc<RwLock<HashMap<String,mpsc::UnboundedSender<Action>>>>,
}

impl Control{
    pub fn new() -> Self {
        Self {  
            agent_list: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add_agent(&mut self, name: String, sender: mpsc::UnboundedSender<Action>) {
        let mut agent_list = self.agent_list.write().unwrap();
        agent_list.insert(name, sender);
    }

    pub fn add_route(&mut self, route: Route) {
        let agent_list_clone = Arc::clone(&self.agent_list);
        let agent_list = agent_list_clone.read().unwrap();
        for (_, agent_sender) in agent_list.clone(){
            agent_sender.send(Action::Add(Add::Route(route.clone()))).unwrap();
        }
    }

    pub fn del_route(&mut self, route: Route) {
        let agent_list_clone = Arc::clone(&self.agent_list);
        let agent_list = agent_list_clone.read().unwrap();
        for (_, agent_sender) in agent_list.clone(){
            agent_sender.send(Action::Delete(Delete::Route(route.clone()))).unwrap();
        }
    }

    pub fn run(&self, mut receiver: mpsc::UnboundedReceiver<Action>) -> Vec<tokio::task::JoinHandle<()>> {
        let mut join_handlers: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        //let route_table_partition = HashMap::new();
        let mut route_table: Table<Ipv4Net, String> = Table::new("route_table".to_string(),1);
        let mut route_table_handlers = route_table.run(defaults::<Ipv4Net, String, HashMap<Ipv4Net,String>>());
        join_handlers.append(&mut route_table_handlers);
        let agent_list_clone = Arc::clone(&self.agent_list);
        let handle = tokio::spawn(async move{  
            while let Some(cmd) = receiver.recv().await {
                match cmd {
                    Action::Add(Add::Route(route)) => {
                        let kv = KeyValue{
                            key: route.clone().dst,
                            value: route.clone().nh,
                        };
                        route_table.set(kv.clone(), kv).await.unwrap();
                        let agent_list = agent_list_clone.read().unwrap();
                        for (_, agent_sender) in agent_list.clone(){
                            agent_sender.send(Action::Add(Add::Route(route.clone()))).unwrap();
                        }
                    },
                    Action::Delete(Delete::Route(route)) => {
                        route_table.delete(route.clone().dst, route.clone().dst).await.unwrap();
                        let agent_list = agent_list_clone.read().unwrap();
                        for (_, agent_sender) in agent_list.clone(){
                            agent_sender.send(Action::Delete(Delete::Route(route.clone()))).unwrap();
                        }
                    },
                    _ => {},
                }
            }
        });
        join_handlers.push(handle);
        join_handlers
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Route{
    pub dst: ipnet::Ipv4Net,
    pub nh: String,
}