pub mod control;
use tokio::sync::mpsc;
use crate::agent::agent::{Action,Add};

pub struct Control{}

impl Control{
    pub fn new() -> Self {
        Self {  

        }
    }

    pub fn run(self, mut receiver: mpsc::UnboundedReceiver<Action>) {

    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Route{
    pub dst: ipnet::Ipv4Net,
    pub nh: String,
}