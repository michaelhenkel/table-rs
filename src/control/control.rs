use tokio::sync::mpsc;
use crate::agent::agent::{Action,Add, Delete};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use crate::table::table::{Table, KeyValue};
use ipnet::Ipv4Net;


fn route_finder(key: Ipv4Net, map: MutexGuard<HashMap<Ipv4Net,String>>) -> String {
    map.get(&key).unwrap().clone()
}

fn route_setter(key_value: KeyValue<Ipv4Net, String>, mut map: MutexGuard<HashMap<Ipv4Net,String>>) -> Option<String>{
    map.insert(key_value.key, key_value.value)
}
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

    pub fn run(&self, mut receiver: mpsc::UnboundedReceiver<Action>) -> Vec<tokio::task::JoinHandle<()>> {
        let mut join_handlers: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let mut route_table: Table<Ipv4Net, String> = Table::new(1);
        let mut route_table_handlers = route_table.run(route_finder, route_setter);
        join_handlers.append(&mut route_table_handlers);
        let agent_list_clone = Arc::clone(&self.agent_list);
        let handle = tokio::spawn(async move{  
            while let Some(cmd) = receiver.recv().await {
                match cmd {
                    Action::Add(Add::Route(route)) => {
                        route_table.set(KeyValue{
                            key: route.clone().dst,
                            value: route.clone().nh,
                        }).await.unwrap();
                        let agent_list = agent_list_clone.read().unwrap();
                        for (_, agent_sender) in agent_list.clone(){
                            agent_sender.send(Action::Add(Add::Route(route.clone()))).unwrap();
                        }
                    },
                    Action::Delete(Delete::Route(route)) => {
                        route_table.delete(route.clone().dst).await.unwrap();
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