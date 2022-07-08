use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::sync::{Arc,Mutex, MutexGuard};
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver};
use tokio::sync::{mpsc, oneshot};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use core::{borrow::Borrow};
use std::fmt::Debug;
use std::rc::Rc;
use crate::agent::agent::FlowNetKey;


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

    pub fn run<P,S,G,D,L,X>(&mut self, p: P, w: (G, S, D, L, X)) -> Vec<tokio::task::JoinHandle<()>>
    where
    V: Sync + Default,
    K: Sync + Default,
    P: Send + Sync + 'static + Clone + GenericMap<K,V>,
    G: FnMut(K, MutexGuard<P>) -> Option<V> + Send + Clone + 'static + Sync,
    S: FnMut(KeyValue<K,V>, MutexGuard<P>) -> Option<V> + Send + Clone + 'static + Sync,
    D: FnMut(K, MutexGuard<P>) -> Option<V> + Send + Clone + 'static + Sync,
    L: FnMut(MutexGuard<P>) -> Option<Vec<KeyValue<K,V>>> + Send + Clone + 'static + Sync,
    X: FnMut(MutexGuard<P>) -> usize + Send + Clone + 'static + Sync,
    {
        let mut join_handlers = Vec::new();
        for part in 0..self.num_partitions{
            let g = w.0.clone();
            let s = w.1.clone();
            let d = w.2.clone();
            let l = w.3.clone();
            let x = w.4.clone();
            let p = p.clone();
            let mut p: Partition<P,S,G,D,L,X,K,V> = Partition::new(part, p, s, g, d, l, x);
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
struct Partition<P,S,G,D,L,X,K,V> 
where
    P: GenericMap<K, V>,
{
    partition_table: Arc<Mutex<P>>,
    name: u32,
    setter: S,
    getter: G,
    deleter: D,
    lister: L,
    length: X,
    bla: K,
    bla2: V,
}

impl<P,S,G,D,L,X,K,V> Partition<P,S,G,D,L,X,K,V> 
where 
    P: GenericMap<K,V>,
    K: Default,
    V: Default,
    {
    fn new(name: u32,p: P, s: S ,g: G, d: D, l: L, x: X) -> Self {
        Self{
            partition_table: Arc::new(Mutex::new(p)),
            name,
            setter: s,
            getter: g,
            deleter: d,
            lister: l,
            length: x,
            bla: K::default(),
            bla2: V::default(),
        }
    }

    async fn recv(&mut self, mut receiver: mpsc::UnboundedReceiver<Command<K,V>>) -> Result<(), Box<dyn std::error::Error + Send +'static>>
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
            match cmd {
                Command::Get { key, responder} => {
                    let partition_table = self.partition_table.lock().unwrap();
                    let res = (self.getter)(key.clone(),partition_table);
                    let res = res.clone();
                    responder.send(res).unwrap();
                },
                Command::Set { key_value, responder} => {
                    let mut partition_table = self.partition_table.lock().unwrap();
                    let res = partition_table.setter(key_value);
                    responder.send(res).unwrap();
                },
                Command::Delete { key, responder } => {
                    let mut partition_table = self.partition_table.lock().unwrap();
                    let res = partition_table.deleter(key);
                    responder.send(res).unwrap();
                },
                Command::Len { responder} => {
                    let mut partition_table = self.partition_table.lock().unwrap();
                    let res = partition_table.length();
                    let res = res.clone();
                    responder.send(res).unwrap();
                },
                Command::List { responder} =>  {
                    let mut partition_table = self.partition_table.lock().unwrap();
                    let res = partition_table.lister();
                    responder.send(res.unwrap()).unwrap();
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

#[derive(Clone)]
pub struct FlowMap<K, V> 
where
    K: Eq + Hash + Ord + Clone,
    V: Clone,
{
    src_map: Arc<BTreeMap<u32, HashMap<(u32,u16), bool>>>,
    dst_map: Arc<BTreeMap<u32, HashMap<(u32,u16), bool>>>,
    flow_map: Arc<HashMap<K,V>>,
}

impl<K,V> FlowMap<K,V> 
where
    K: Eq + Hash + Ord + Clone,
    V: Clone,
{
    pub fn new() -> Self{
        Self { 
            src_map: Arc::new(BTreeMap::new()),
            dst_map: Arc::new(BTreeMap::new()),
            flow_map: Arc::new(HashMap::new())
        }
    }
    fn get(&mut self, key: &K) -> Option<V> {
        self.flow_map.get(key).cloned()
    }
    fn list(&mut self) -> Vec<KeyValue<K,V>> {
        let mut res = Vec::new();
        for (key, value) in self.flow_map.as_ref() {
            res.push(KeyValue{
                key: key.clone(),
                value: value.clone(),
            });
        }
        res
    }
    fn delete(&mut self, key: &K) -> Option<V> {
        let flow_map = Arc::get_mut(&mut self.flow_map).unwrap(); 
        flow_map.remove(key)
    }
    fn len(&mut self) -> usize {
        self.flow_map.len()
    }
    fn add(&mut self, key: K, value: V) -> Option<V> {
        let flow_map = Arc::get_mut(&mut self.flow_map).unwrap(); 
        flow_map.insert(key, value)
    }
}

impl<K, V> GenericMap<K, V> for FlowMap<K, V>
where
    K: Eq + Hash + Ord + Clone,
    V: Clone,
{
    fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.add(key, value)
    }

    fn setter(&mut self, key_value: KeyValue<K,V>) -> Option<V> {
        self.insert(key_value.key, key_value.value)
    }

    fn getter(&mut self, key: K) -> Option<V> {
        self.get(&key)
    }

    fn lister(&mut self) -> Option<Vec<KeyValue<K,V>>> {
        Some(self.list())
    }

    fn deleter(&mut self, key: K) -> Option<V> {
        self.delete(&key)
    }

    fn length(&mut self) -> usize {
        self.len()
    }
}

pub trait GenericMap<K, V> {
    fn insert(&mut self, key: K, value: V) -> Option<V>;
    fn getter(&mut self, key: K) -> Option<V>;
    fn setter(&mut self, key_value: KeyValue<K,V>) -> Option<V>;
    fn lister(&mut self) -> Option<Vec<KeyValue<K,V>>>;
    fn deleter(&mut self, key: K) -> Option<V>;
    fn length(&mut self) -> usize;
}

impl<K, V> GenericMap<K, V> for HashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.insert(key, value)
    }

    fn getter(&mut self, key: K) -> Option<V> {
        self.get(&key).cloned()
    }

    fn setter(&mut self, key_value: KeyValue<K,V>) -> Option<V> {
        self.insert(key_value.key, key_value.value)
    }

    fn lister(&mut self) -> Option<Vec<KeyValue<K,V>>> {
        let mut res = Vec::new();
        for (k, v) in self.iter() {
            let key_value = KeyValue{
                key: k.clone(),
                value: v.clone(),
            };
            res.push(key_value);
        }
        Some(res)
    }

    fn deleter(&mut self, key: K) -> Option<V> {
        self.get(&key).cloned()
    }

    fn length(&mut self) -> usize {
        self.len()
    }
}

impl<K, V> GenericMap<K, V> for BTreeMap<K, V>
where
    K: Eq + Hash + Ord + Clone,
    V: Clone,
{
    fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.insert(key, value)
    }

    fn setter(&mut self, key_value: KeyValue<K,V>) -> Option<V> {
        self.insert(key_value.key, key_value.value)
    }

    fn getter(&mut self, key: K) -> Option<V> {
        self.get(&key).cloned()
    }

    fn lister(&mut self) -> Option<Vec<KeyValue<K,V>>> {
        let mut res = Vec::new();
        for (k, v) in self.iter() {
            let k = k;
            let key_value = KeyValue{
                key: k.clone(),
                value: v.clone(),
            };
            res.push(key_value);
        }
        Some(res)
    }

    fn deleter(&mut self, key: K) -> Option<V> {
        self.get(&key).cloned()
    }

    fn length(&mut self) -> usize {
        self.len()
    }
}


pub fn flow_map_funcs() -> 
    (
        impl FnMut(FlowNetKey, MutexGuard<FlowMap<FlowNetKey, String>>) -> Option<String> + Clone,
        impl FnMut(KeyValue<FlowNetKey,String>, MutexGuard<FlowMap<FlowNetKey, String>>) -> Option<String> + Clone,
        impl FnMut(FlowNetKey, MutexGuard<FlowMap<FlowNetKey, String>>) -> Option<String> + Clone,
        impl FnMut(MutexGuard<FlowMap<FlowNetKey, String>>) -> Option<Vec<KeyValue<FlowNetKey, String>>> + Clone,
        impl FnMut(MutexGuard<FlowMap<FlowNetKey, String>>) -> usize + Clone,
    )
{
    let deleter = |k: FlowNetKey, mut p: MutexGuard<FlowMap<FlowNetKey, String>>| {
        p.deleter(k)
    };

    let getter = |key: FlowNetKey, mut p: MutexGuard<FlowMap<FlowNetKey, String>>| {
        // match specific src/dst port first
        println!("blabla");
        let src_net_specific = get_net_port(key.src_net, key.src_port, p.src_map.clone());
        let dst_net_specific = get_net_port(key.dst_net, key.dst_port, p.dst_map.clone());
        if src_net_specific.is_some() && dst_net_specific.is_some(){
            let (src_net, src_mask,  src_port) = src_net_specific.unwrap();
            let (dst_net, dst_mask, dst_port) = dst_net_specific.unwrap();
            let res = p.flow_map.get(&(FlowNetKey{
                src_net,
                src_mask,
                src_port,
                dst_net,dst_mask,
                dst_port}
            ));
            return res.cloned()
        }

        // match specific src_port and 0 dst_port
        let src_net_0 = get_net_port(key.src_net, 0, p.src_map.clone());
        if src_net_0.is_some() && dst_net_specific.is_some(){
            let (src_net, src_mask, src_port) = src_net_0.unwrap();
            let (dst_net, dst_mask, dst_port) = dst_net_specific.unwrap();
            let res = p.flow_map.get(&(FlowNetKey{src_net, src_mask, src_port, dst_net, dst_mask, dst_port}));
            return res.cloned()
        }

        // match 0 src_port and specific dst_port
        let dst_net_0 = get_net_port(key.dst_net, 0, p.dst_map.clone());
        if src_net_specific.is_some() && dst_net_0.is_some(){
            let (src_net,src_mask, src_port) = src_net_specific.unwrap();
            let (dst_net,dst_mask, dst_port) = dst_net_0.unwrap();
            let res = p.flow_map.get(&(FlowNetKey{src_net, src_mask, src_port, dst_net, dst_mask, dst_port}));
            return res.cloned()
        }

        // match 0 src_port and 0 dst_port
        if src_net_0.is_some() && dst_net_0.is_some(){
            let (src_net,src_mask, src_port) = src_net_0.unwrap();
            let (dst_net,dst_mask, dst_port) = dst_net_0.unwrap();
            let res = p.flow_map.get(&(FlowNetKey{src_net, src_mask, src_port, dst_net, dst_mask, dst_port}));
            return res.cloned()
        }
        None
    };

    let setter = |k: KeyValue<FlowNetKey,String>, mut p: MutexGuard<FlowMap<FlowNetKey, String>>| {
        let src_map = Arc::get_mut(&mut p.src_map).unwrap(); 
        let src_mask: u32 = 4294967295 - k.key.src_mask;
        let res = src_map.get_mut(&src_mask);
        match res {
            Some(map) => {
                map.insert((k.key.src_net, k.key.src_port), true);
            },
            None => {
                let mut map = HashMap::new();
                map.insert((k.key.src_net, k.key.src_port), true);
                src_map.insert(src_mask, map);
            },
        }

        let dst_mask = 4294967295 - k.key.dst_mask;
        let dst_map = Arc::get_mut(&mut p.dst_map).unwrap(); 
        let res = dst_map.get_mut(&dst_mask);
        match res {
            Some(map) => {
                map.insert((k.key.dst_net, k.key.dst_port), true);
            },
            None => {
                let mut map = HashMap::new();
                map.insert((k.key.dst_net, k.key.dst_port), true);
                dst_map.insert(dst_mask, map);
            },
        }
        let flow_map = Arc::get_mut(&mut p.flow_map).unwrap(); 
        flow_map.insert(FlowNetKey{
            src_net: k.key.src_net, 
            src_mask: src_mask, 
            src_port: k.key.src_port,
            dst_net: k.key.dst_net,
            dst_mask: dst_mask,
            dst_port: k.key.dst_port}
            , k.value)
    };

    let lister = |mut p: MutexGuard<FlowMap<FlowNetKey, String>>| {
        p.lister()
    };

    let length = |mut p: MutexGuard<FlowMap<FlowNetKey, String>>| {
        p.length()
    };
    (getter, setter, deleter, lister, length)
}

fn get_net_port(ip: u32, port: u16, map: Arc<BTreeMap<u32, HashMap<(u32,u16), bool>>>) -> Option<(u32,u32,u16)>{
    for (mask, map) in map.as_ref() {
        let mask_bin = 4294967295 - mask;
        let masked: u32 = ip & mask_bin;
        let kv = map.get_key_value(&(masked, port));
        match kv {
            Some(((net, port),_)) => { return Some((net.clone(),mask_bin, port.clone())) },
            None => { },
        }
    }
    None
}

pub fn defaults<K,V,P>() -> 
    (
        impl FnMut(K, MutexGuard<P>) -> Option<V> + Clone,
        impl FnMut(KeyValue<K,V>, MutexGuard<P>) -> Option<V> + Clone,
        impl FnMut(K, MutexGuard<P>) -> Option<V> + Clone,
        impl FnMut(MutexGuard<P>) -> Option<Vec<KeyValue<K, V>>> + Clone,
        impl FnMut(MutexGuard<P>) -> usize + Clone,
    )
    where
        P: GenericMap<K,V>,
        K: Clone,
        V: Clone,
{
    let deleter = |k: K, mut p: MutexGuard<P>| {
        p.deleter(k)
    };

    let getter = |k: K, mut p: MutexGuard<P>| {
        p.getter(k)
    };

    let setter = |k: KeyValue<K,V>, mut p: MutexGuard<P>| {
        p.setter(k)
    };

    let lister = |mut p: MutexGuard<P>| {
        p.lister()
    };

    let length = |mut p: MutexGuard<P>| {
        p.length()
    };
    (getter, setter, deleter, lister, length)
}

