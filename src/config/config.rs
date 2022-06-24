use std::sync::Arc;
use std::collections::HashMap;
use std::sync::RwLock;
use crate::agent::agent::{Add,Action,Delete};
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
                sender.send(Action::Add(Add::Vmi(vmi))).unwrap();
            },
            None => {
                println!("no sender found");
            },
        }
    }

    pub fn add_acl(&self,acl: Acl) {
        let mut sender_map = self.agent_list.write().unwrap();
        let agent_sender = sender_map.get_mut(&acl.clone().agent);
        match agent_sender{
            Some(sender) => {
                sender.send(Action::Add(Add::Acl(acl))).unwrap();
            },
            None => {
                println!("no sender found");
            },
        }
    }

    pub fn del_acl(&self,acl: Acl) {
        let mut sender_map = self.agent_list.write().unwrap();
        let agent_sender = sender_map.get_mut(&acl.clone().agent);
        match agent_sender{
            Some(sender) => {
                sender.send(Action::Delete(Delete::Acl(acl))).unwrap();
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

#[derive(PartialEq,Hash,Eq,Clone,Debug)]
pub struct Acl {
    pub key: AclKey,
    pub value: AclValue,
    pub agent: String,
}

#[derive(PartialEq,Hash,Eq,Clone,Debug)]
pub struct AclKey {
    pub src_net: ipnet::Ipv4Net,
    pub dst_net: ipnet::Ipv4Net,
}

#[derive(PartialEq,Hash,Eq,Clone,Debug)]
pub struct AclValue {
    pub src_port: u16,
    pub dst_port: u16,
}