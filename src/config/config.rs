use std::sync::Arc;
use std::collections::HashMap;
use std::sync::RwLock;
use crate::agent::agent::{Add,Action};
use ipnet;
use tokio::sync::{mpsc};

#[derive(Clone)]
pub struct Config{
    pub agent_list: Arc<RwLock<HashMap<String,mpsc::UnboundedSender<Action>>>>,
}

impl Config {
    pub fn new() -> Self {
        Self { 
            agent_list:  Arc::new(RwLock::new(HashMap::new())),
        }
    }
    pub fn add_agent(&self, name: String, sender: mpsc::UnboundedSender<Action>) {
        let mut sender_map = self.agent_list.write().unwrap();
        sender_map.insert(name, sender);
    }

    pub fn add_vmi(&self,vmi: Vmi) {
        let mut sender_map = self.agent_list.write().unwrap();
        let agent_sender = sender_map.get_mut(&vmi.clone().agent);
        match agent_sender{
            Some(sender) => {
                println!("sending vmi to agent");
                sender.send(Action::Add(Add::Vmi(vmi))).unwrap();
            },
            None => {
                println!("no sender found");
            },
        }
    }
}


#[derive(PartialEq,Hash,Eq,Clone,Debug)]
pub struct Vmi {
    pub name: String,
    pub ip: ipnet::Ipv4Net,
    pub agent: String,
}