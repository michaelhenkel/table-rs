use crate::config::config::Vmi;
use std::sync::Arc;
use std::collections::HashMap;
use std::sync::RwLock;
use ipnet::Ipv4Net;
use crate::table::table::{Table, KeyValue};
use crate::control::control::Route;
use tokio::sync::mpsc;

pub enum Add{
    Vmi(Vmi),
    Route(Route),
}

pub enum Action{
    Add(Add),
}

#[derive(Clone)]
pub struct Agent{
    pub name: String,
}

impl Agent {
    pub fn new(name: String) -> Self {
        Self { 
            name,
        }
    }
    pub fn run(self, mut receiver: mpsc::UnboundedReceiver<Action>, route_sender: mpsc::UnboundedSender<Action>) -> Vec<tokio::task::JoinHandle<()>> {
        let mut vmi_table: Table<Ipv4Net, String> = Table::new(1);
        let mut join_handlers = vmi_table.run();
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
                        })));
                    },
                    Action::Add(Add::Route(route)) => {
                        println!("received route");
                    },
                }
            }
        });
        join_handlers.push(handle);
        join_handlers
    }
}