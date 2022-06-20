use tokio::sync::mpsc;
use crate::agent::agent::{Action,Add};
use std::sync::Arc;
use std::collections::HashMap;
use std::sync::RwLock;
use crate::table::table::{Table, KeyValue};
use ipnet::Ipv4Net;

pub struct Control{
    pub agent_list: Arc<RwLock<HashMap<String,mpsc::UnboundedSender<Action>>>>,
}

impl Control{
    pub fn new() -> Self {
        Self {  
            agent_list: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add_agent(&self, name: String, sender: mpsc::UnboundedSender<Action>) {
        let mut sender_map = self.agent_list.write().unwrap();
        sender_map.insert(name, sender);
    }

    pub fn run(self, mut receiver: mpsc::UnboundedReceiver<Action>) -> Vec<tokio::task::JoinHandle<()>> {
        let mut route_table: Table<Ipv4Net, String> = Table::new(1);
        let mut join_handlers = route_table.run();
        let handle = tokio::spawn(async move{    
            while let Some(cmd) = receiver.recv().await {
                match cmd {
                    Action::Add(Add::Route(route)) => {
                        route_table.set(KeyValue{
                            key: route.clone().dst,
                            value: route.clone().nh,
                        }).await;
                        let agent_list = self.agent_list.read().unwrap();
                        for (_, agent_sender) in agent_list.clone(){
                            agent_sender.send(Action::Add(Add::Route(route.clone())));
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

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Route{
    pub dst: ipnet::Ipv4Net,
    pub nh: String,
}