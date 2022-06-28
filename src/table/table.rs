use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc,Mutex, MutexGuard};
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver};
use tokio::sync::{mpsc, oneshot};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use core::{borrow::Borrow};
use std::fmt::Debug;

#[derive(Debug,Clone)]
pub struct Table<K,V> {
    partitions: HashMap<u32,mpsc::UnboundedSender<Command<K,V>>>,
    num_partitions: u32,
}

impl <K,V>Table<K,V>
where
    K: Clone + Debug + Hash + Eq + Send + 'static,
    V: Debug + Clone + Send + 'static,
{
    pub fn new(num_partitions: u32) -> Table<K,V> {
        let partitions = HashMap::new();
        Table { 
            partitions,
            num_partitions,
        }
    }

    pub async fn get(&self, key: K) -> Result<Option<V>, tokio::sync::oneshot::error::RecvError>{
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
    
    pub async fn set(&self, key_value: KeyValue<K,V>) -> Result<Option<V>, tokio::sync::oneshot::error::RecvError> {
        let key_hash = calculate_hash(&key_value.key);
        let part = n_mod_m(key_hash, self.num_partitions.try_into().unwrap());
        let (responder_sender, responder_receiver) = oneshot::channel();
        let partiton_sender = self.partitions.get(&part.try_into().unwrap()).unwrap();
        partiton_sender.clone().send(Command::Set { key_value: key_value.clone(), responder: responder_sender}).unwrap();
        responder_receiver.await
    }

    pub async fn delete(&self, key: K) -> Result<Option<V>, tokio::sync::oneshot::error::RecvError>{
        let key_hash = calculate_hash(&key);
        let part = n_mod_m(key_hash, self.num_partitions.try_into().unwrap());
        let partiton_sender = self.partitions.get(&part.try_into().unwrap()).unwrap();
        let (responder_sender, responder_receiver) = oneshot::channel();
        partiton_sender.clone().send(Command::Delete { key, responder: responder_sender }).unwrap();
        responder_receiver.await
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

    pub fn run<P,S,G,D,L,X>(&mut self, p: P, s: S ,g: G, d: D, l: L, x: X) -> Vec<tokio::task::JoinHandle<()>>
    where
    P: Send + Sync + 'static + Clone,
    G: FnMut(K, MutexGuard<P>) -> Option<V> + Send + Clone + 'static + Sync,
    S: FnMut(KeyValue<K,V>, MutexGuard<P>) -> Option<V> + Send + Clone + 'static + Sync,
    D: FnMut(K, MutexGuard<P>) -> Option<V> + Send + Clone + 'static + Sync,
    L: FnMut(MutexGuard<P>) -> Option<Vec<KeyValue<K,V>>> + Send + Clone + 'static + Sync,
    X: FnMut(MutexGuard<P>) -> usize + Send + Clone + 'static + Sync,
    {
        let mut join_handlers = Vec::new();
        for part in 0..self.num_partitions{
            let g = g.clone();
            let s = s.clone();
            let d = d.clone();
            let l = l.clone();
            let x = x.clone();
            let p = p.clone();
            let p: Partition<P,S,G,D,L,X> = Partition::new(part, p, s, g, d, l, x);
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
struct Partition<P,S,G,D,L,X> 
{
    partition_table: Arc<Mutex<P>>,
    name: u32,
    setter: S,
    getter: G,
    deleter: D,
    lister: L,
    length: X,
}

impl<P,S,G,D,L,X> Partition<P,S,G,D,L,X> 
    {
    fn new(name: u32,p: P, s: S, g: G, d: D, l: L, x: X) -> Self {
        Self{
            partition_table: Arc::new(Mutex::new(p)),
            name,
            setter: s,
            getter: g,
            deleter: d,
            lister: l,
            length: x,
        }
    }

    async fn recv<K,V>(&self, mut receiver: mpsc::UnboundedReceiver<Command<K,V>>) -> Result<(), Box<dyn std::error::Error + Send +'static>>
    where
    G: FnMut(K, MutexGuard<P>) -> Option<V> + Clone,  
    S: FnMut(KeyValue<K,V>, MutexGuard<P>) -> Option<V> + Clone,
    D: FnMut(K, MutexGuard<P>) -> Option<V> + Clone,  
    L: FnMut(MutexGuard<P>) -> Option<Vec<KeyValue<K,V>>> + Clone,
    X: FnMut(MutexGuard<P>) -> usize + Clone, 
    K: Eq + Hash + Clone + std::fmt::Debug,
    V: Clone + std::fmt::Debug,
    {    
        while let Some(cmd) = receiver.recv().await {
            let mut getter = self.getter.clone();
            let mut setter = self.setter.clone();
            let mut deleter = self.deleter.clone();
            let mut length = self.length.clone();
            let mut lister = self.lister.clone();
            match cmd {
                Command::Get { key, responder} => {
                    let partition_table = self.partition_table.lock().unwrap();
                    let res = (getter)(key, partition_table);
                    let res = res.clone();
                    responder.send(res).unwrap();
                },
                Command::Set { key_value, responder} => {
                    let partition_table = self.partition_table.lock().unwrap();
                    let res = (setter)(key_value, partition_table);
                    responder.send(res).unwrap();
                },
                Command::Delete { key, responder } => {
                    let mut partition_table = self.partition_table.lock().unwrap();
                    let res = (deleter)(key, partition_table);
                    responder.send(res).unwrap();
                },
                Command::Len { responder} => {
                    let partition_table = self.partition_table.lock().unwrap();
                    let res = (length)(partition_table);
                    let res = res.clone();
                    responder.send(res).unwrap();
                },
                Command::List { responder} =>  {
                    let partition_table = self.partition_table.lock().unwrap();
                    let key_value_list = (lister)(partition_table);
                    responder.send(key_value_list.unwrap()).unwrap();
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
        responder: oneshot::Sender<Option<V>>,
    },
    Set{
        key_value: KeyValue<K,V>,
        responder: oneshot::Sender<Option<V>>,
    },
    Delete{
        key: K,
        responder: oneshot::Sender<Option<V>>,
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
    n % m
    //((n % m) + m) % m
}