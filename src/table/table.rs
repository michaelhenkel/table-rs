use std::collections::HashMap;
use std::sync::{Arc,Mutex};
use futures::channel::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Debug,Clone)]
pub struct Table<K,V> {
    partitions: HashMap<u32,mpsc::UnboundedSender<Command<K,V>>>,
    num_partitions: u32,
}

impl <K,V>Table<K,V>
where
    K: std::clone::Clone,
    K: std::fmt::Debug,
    K: std::hash::Hash,
    K: std::cmp::Eq,
    K: std::marker::Send,
    K: 'static,
    V: std::fmt::Debug,
    V: std::clone::Clone,
    V: std::marker::Send,
    V: 'static,
{
    pub fn new(num_partitions: u32) -> Table<K,V> {
        let partitions = HashMap::new();
        Table { 
            partitions,
            num_partitions,
        }
    }

    pub async fn get(&self, key: K) -> Result<V, tokio::sync::oneshot::error::RecvError>{
        let key_hash = calculate_hash(&key);
        let part = n_mod_m(key_hash, self.num_partitions.try_into().unwrap());
        let partiton_sender = self.partitions.get(&part.try_into().unwrap()).unwrap();
        let (responder_sender, responder_receiver) = oneshot::channel();
        partiton_sender.clone().send(Command::Get { key: key.try_into().unwrap(), responder: responder_sender }).unwrap();
        responder_receiver.await
    }

    pub fn get_senders(&self) -> HashMap<u32, tokio::sync::mpsc::UnboundedSender<Command<K, V>>>{
        self.partitions.clone()
    }
    
    pub async fn set(&self, key_value: KeyValue<K,V>) {
        let key_hash = calculate_hash(&key_value.key);
        let part = n_mod_m(key_hash, self.num_partitions.try_into().unwrap());
        let partiton_sender = self.partitions.get(&part.try_into().unwrap()).unwrap();
        partiton_sender.clone().send(Command::Set { key_value: key_value.clone()  }).unwrap();
    }

    pub async fn len(&self) -> usize {
        let mut partitions_size = 0;
        let partitions = self.partitions.clone();
        for (_, partiton_sender) in partitions {
            let (responder_sender, responder_receiver) = oneshot::channel();
            partiton_sender.send(Command::Len { responder: responder_sender }).unwrap();
            let res = responder_receiver.await.unwrap();
            partitions_size = partitions_size + res;
        }
        partitions_size
    }

    pub async fn list(&self) -> Vec<KeyValue<K,V>> {
        let mut key_value_list = Vec::new();
        let partitions = self.partitions.clone();
        for (_, partiton_sender) in partitions {
            let (responder_sender, responder_receiver) = oneshot::channel();
            partiton_sender.send(Command::List { responder: responder_sender }).unwrap();
            let mut res = responder_receiver.await.unwrap();
            key_value_list.append(&mut res);
            //partitions_size = partitions_size + res;
        }
        key_value_list
    }

    pub fn run(&mut self) -> Vec<tokio::task::JoinHandle<()>>{
        let mut join_handlers = Vec::new();
        for part in 0..self.num_partitions{
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
    K: Clone,
    K: std::fmt::Debug,
    V: std::fmt::Debug,
    V: std::clone::Clone,
    {
    fn new(name: u32) -> Self {
        Self{
            partition_table: Arc::new(Mutex::new(HashMap::new())),
            name,
        }
    }

    async fn recv(&self, mut receiver: mpsc::UnboundedReceiver<Command<K,V>>) -> Result<(), Box<dyn std::error::Error + Send +'static>>{
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
                Command::Len { responder} => {
                    let partition_table = self.partition_table.lock().unwrap();
                    let res = partition_table.len();
                    let res = res.clone();
                    responder.send(res).unwrap();
                },
                Command::List { responder} => {
                    let partition_table = self.partition_table.lock().unwrap();
                    let mut key_value_list = Vec::new();
                    for (k, v) in partition_table.clone() {
                        let key_value = KeyValue{
                            key: k,
                            value: v,
                        };
                        key_value_list.push(key_value);
                    }
                    responder.send(key_value_list).unwrap();
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
    Len{
        responder: oneshot::Sender<usize>,
    },
    List{
        responder: oneshot::Sender<Vec<KeyValue<K,V>>>,
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
