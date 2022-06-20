use std::collections::HashMap;
use std::sync::{Arc,Mutex, MutexGuard};
use tokio::sync::{mpsc, oneshot};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use rand::Rng;
use std::time::Instant;
use itertools::Itertools;

#[derive(Debug,Clone)]
pub struct Table<K,V> {
    partitions: HashMap<u32,mpsc::UnboundedSender<Command<K,V>>>,
    num_partitions: u32,
}

impl <K,V>Table<K,V>
where
    K: std::ops::Rem<Output = K>,
    K: std::ops::Add<Output = K>,
    K: std::marker::Copy,
    K: std::convert::From<u32>,
    K: std::fmt::Debug,
    K: std::hash::Hash,
    K: std::cmp::Eq,
    K: std::marker::Send,
    V: std::fmt::Debug,
    V: std::clone::Clone,
    V: std::marker::Send,
    V: 'static,
    K: 'static,
    u32: From<K>,
{
    pub fn new(num_partitions: u32) -> Table<K,V> {
        let partitions = HashMap::new();
        Table { 
            partitions,
            num_partitions,
        }
    }

    pub async fn get(&self, key: K) -> Result<V, tokio::sync::oneshot::error::RecvError>{
        let part = n_mod_m(key, self.num_partitions.try_into().unwrap());
        let partiton_sender = self.partitions.get(&part.try_into().unwrap()).unwrap();
        let (responder_sender, responder_receiver) = oneshot::channel();
        partiton_sender.clone().send(Command::Get { key: key.try_into().unwrap(), responder: responder_sender }).unwrap();
        responder_receiver.await    
    }
    
    pub async fn set(&self, key_value: KeyValue<K,V>) {
        let part = n_mod_m(key_value.key.into(), self.num_partitions);
        let partiton_sender = self.partitions.get(&part).unwrap();
        partiton_sender.clone().send(Command::Set { key_value: key_value.clone()  }).unwrap();
    }
    pub fn run(&mut self) -> Vec<tokio::task::JoinHandle<()>>{
        println!("setting up partitions");
        let mut join_handlers = Vec::new();
        for part in 0..self.num_partitions{
            println!("setting up partition {}", part);
            let p: Partition<K,V> = Partition::new(part);
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
struct Partition<K,V> 
{
    partition_table: Arc<Mutex<HashMap<K,V>>>,
    name: u32,
}

impl<K,V> Partition<K,V> 
where
    K: Eq,
    K: Hash,
    V: std::fmt::Debug,
    V: std::clone::Clone,
    {
    fn new(name: u32) -> Self {
        Self{
            partition_table: Arc::new(Mutex::new(HashMap::new())),
            name,
        }
    }
    async fn recv(&self, mut receiver: mpsc::UnboundedReceiver<Command<K,V>>) -> Result<(), Box<dyn std::error::Error + Send>>{
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
pub enum Command<K,V>{
    Get{
        key: K,
        responder: oneshot::Sender<V>,
    },
    Set{
        key_value: KeyValue<K,V>,
    },
}

#[derive(Debug,Clone)]
pub struct KeyValue<K,V>{
    pub key: K,
    pub value: V,
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
