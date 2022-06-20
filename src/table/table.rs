use std::collections::HashMap;
use std::sync::{Arc,Mutex};
use tokio::sync::{mpsc, oneshot};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use rand::Rng;
use std::time::Instant;
use itertools::Itertools;

#[derive(Debug,Clone)]
pub struct Table {
    partitions: HashMap<u32,mpsc::UnboundedSender<Command>>,
    num_partitions: u32,
}

impl Table{
    pub fn new(num_partitions: u32) -> Table {
        let partitions = HashMap::new();
        Table { 
            partitions,
            num_partitions,
        }
    }

    pub async fn get(&self, key: u32) -> Result<String, tokio::sync::oneshot::error::RecvError>{
        let part = n_mod_m(key, self.num_partitions.try_into().unwrap());
        let partiton_sender = self.partitions.get(&part.try_into().unwrap()).unwrap();
        let (responder_sender, responder_receiver) = oneshot::channel();
        partiton_sender.clone().send(Command::Get { key: key.try_into().unwrap(), responder: responder_sender }).unwrap();
        responder_receiver.await    
    }
    
    pub async fn set(&self, key_value: KeyValue) {
        let part = n_mod_m(key_value.key, self.num_partitions);
        let partiton_sender = self.partitions.get(&part).unwrap();
        partiton_sender.clone().send(Command::Set { key_value: key_value.clone()  }).unwrap();
    }
    pub fn run(&mut self) -> Vec<tokio::task::JoinHandle<()>>{
        println!("setting up partitions");
        let mut join_handlers = Vec::new();
        for part in 0..self.num_partitions{
            println!("setting up partition {}", part);
            let p = Partition::new(part);
            let (sender, receiver) = mpsc::unbounded_channel();
            self.partitions.insert(part, sender);
            let handle = tokio::spawn(async move{
                p.recv(receiver).await.unwrap();
            });
            join_handlers.push(handle);
        }
        join_handlers
    }
}

#[derive(Clone, Debug)]
struct Partition {
    partition_table: Arc<Mutex<HashMap<u32,String>>>,
    name: u32,
}

impl Partition {
    fn new(name: u32) -> Self {
        Self{
            partition_table: Arc::new(Mutex::new(HashMap::new())),
            name,
        }
    }
    async fn recv(&self, mut receiver: mpsc::UnboundedReceiver<Command>) -> Result<(), Box<dyn std::error::Error + Send>>{
        while let Some(cmd) = receiver.recv().await {
            match cmd {
                Command::Get { key, responder} => {
                    let partition_table = self.partition_table.lock().unwrap();
                    let res = partition_table.get(&key).unwrap();
                    let res = res.clone();
                    responder.send(res).unwrap();
                },
                Command::Set { key_value } => {
                    let mut partition_table = self.partition_table.lock().unwrap();
                    partition_table.insert(key_value.key, key_value.value);
                },
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum Command{
    Get{
        key: u32,
        responder: oneshot::Sender<String>,
    },
    Set{
        key_value: KeyValue,
    },
}

#[derive(Debug,Clone)]
pub struct KeyValue{
    pub key: u32,
    pub value: String,
}

pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub fn n_mod_m <T: std::ops::Rem<Output = T> + std::ops::Add<Output = T> + Copy>
  (n: T, m: T) -> T {
    ((n % m) + m) % m
}
