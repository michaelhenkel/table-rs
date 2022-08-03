use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc};
use tokio::sync::{mpsc, oneshot};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::fmt::Debug;
use crate::agent::agent::{FlowNetKey, FlowAction};
use std::time::{Duration, Instant};


#[derive(Debug,Clone)]
pub struct Table<K,V> {
    partitions: HashMap<u32,mpsc::UnboundedSender<Command<K,V>>>,
    num_partitions: u32,
    name: String,

}

impl <K,V>Table<K,V>
where
    K: Clone + Debug + Hash + Eq + Send + 'static,
    
    V: Debug + Clone + Send + 'static,
{
    pub fn new(name: String, num_partitions: u32) -> Table<K,V> {
        let partitions = HashMap::new();
        Table { 
            partitions,
            num_partitions,
            name,
        }
    }

    pub async fn get<PK>(&self, partition_key: PK, key: K) -> Result<Option<V>, tokio::sync::oneshot::error::RecvError>
    where
        PK: Clone + Debug + Hash + Eq + Send + 'static,
    {
        let key_hash = calculate_hash(&partition_key);
        let part = n_mod_m(key_hash, self.num_partitions.try_into().unwrap());
        let partiton_sender = self.partitions.get(&part.try_into().unwrap()).unwrap();
        let (responder_sender, responder_receiver) = oneshot::channel();
        partiton_sender.clone().send(Command::Get { key: key.try_into().unwrap(), responder: responder_sender }).unwrap();
        responder_receiver.await
    }

    pub fn get_senders(&self) -> HashMap<u32, tokio::sync::mpsc::UnboundedSender<Command<K, V>>>{
        self.partitions.clone()
    }
    
    pub async fn set<PK>(&self, partition_key: PK, key_value: KeyValue<K,V>) -> Result<Option<V>, tokio::sync::oneshot::error::RecvError> 
    where
        PK: Clone + Debug + Hash + Eq + Send + 'static,
    {
        let key_hash = calculate_hash(&partition_key);
        let part = n_mod_m(key_hash, self.num_partitions.try_into().unwrap());
        let (responder_sender, responder_receiver) = oneshot::channel();
        let partiton_sender = self.partitions.get(&part.try_into().unwrap()).unwrap();
        partiton_sender.clone().send(Command::Set { key_value: key_value.clone(), responder: responder_sender}).unwrap();
        responder_receiver.await
    }

    pub async fn delete<PK>(&self, partition_key: PK, key: K) -> Result<Option<V>, tokio::sync::oneshot::error::RecvError>
    where
        PK: Clone + Debug + Hash + Eq + Send + 'static,
    {
        let key_hash = calculate_hash(&partition_key);
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

    pub async fn get_stats(&self) -> HashMap<String,(u32,u32,u32)> {
        let mut stats_map = HashMap::new();
        let partitions = self.partitions.clone();
        for (_, partiton_sender) in partitions {
            let (responder_sender, responder_receiver) = oneshot::channel();
            partiton_sender.send(Command::Stats { responder: responder_sender }).unwrap();
            let (table_name, part, hits, misses) = responder_receiver.await.unwrap();
            stats_map.insert(table_name, (part, hits, misses));
        }
        stats_map
    }

    pub fn run<P,S,G,D,L,X>(&mut self, w: (G, S, D, L, X)) -> Vec<tokio::task::JoinHandle<()>>
    where
    V: Sync + Default,
    K: Sync + Default,
    P: Send + Sync + 'static + Clone + GenericMap<K,V>,
    G: FnMut(K, &mut P) -> Option<V> + Send + Clone + 'static + Sync,
    S: FnMut(KeyValue<K,V>, &mut P) -> Option<V> + Send + Clone + 'static + Sync,
    D: FnMut(K, &mut P) -> Option<V> + Send + Clone + 'static + Sync,
    L: FnMut(&mut P) -> Option<Vec<KeyValue<K,V>>> + Send + Clone + 'static + Sync,
    X: FnMut(&mut P) -> usize + Send + Clone + 'static + Sync,
    {
        let mut join_handlers = Vec::new();
        for part in 0..self.num_partitions{
            let g = w.0.clone();
            let s = w.1.clone();
            let d = w.2.clone();
            let l = w.3.clone();
            let x = w.4.clone();
            let mut p: Partition<P,S,G,D,L,X,K,V> = Partition::new(self.name.clone(),s, g, d, l, x, part);
            //p.set_id(self.name.clone(), part);
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
    partition_table: Arc<P>,
    name: String,
    setter: S,
    getter: G,
    deleter: D,
    lister: L,
    length: X,
    key_type: K,
    value_type: V,
    hits: u32,
    misses: u32,
    part_number: u32,
}

impl<P,S,G,D,L,X,K,V> Partition<P,S,G,D,L,X,K,V> 
where 
    P: GenericMap<K,V>,
    K: Default,
    V: Default,
    {
    fn new(name: String,s: S ,g: G, d: D, l: L, x: X, part_number: u32) -> Self {
        println!("creating partition {} for {}", part_number, name);
        let p = P::new();
        Self{
            partition_table: Arc::new(p),
            name,
            setter: s,
            getter: g,
            deleter: d,
            lister: l,
            length: x,
            key_type: K::default(),
            value_type: V::default(),
            hits: 0,
            misses: 0,
            part_number,
        }
    }

    fn get_stats(&mut self) -> (String, u32, u32,u32){
        (self.name.clone(), self.part_number, self.hits, self.misses)
    }

    async fn recv(&mut self, mut receiver: mpsc::UnboundedReceiver<Command<K,V>>) -> Result<(), Box<dyn std::error::Error + Send +'static>>
    where
    G: FnMut(K, &mut P) -> Option<V> + Clone,  
    S: FnMut(KeyValue<K,V>, &mut P) -> Option<V> + Clone,
    D: FnMut(K, &mut P) -> Option<V> + Clone,  
    L: FnMut(&mut P) -> Option<Vec<KeyValue<K,V>>> + Clone,
    X: FnMut(&mut P) -> usize + Clone, 
    K: Eq + Hash + Clone + std::fmt::Debug,
    V: Clone + std::fmt::Debug,
    {
        while let Some(cmd) = receiver.recv().await {
            match cmd {
                Command::Get { key, responder} => {
                    //let start = Instant::now();
                    let partition_table = Arc::get_mut(&mut self.partition_table).unwrap();
                    let res = (self.getter)(key.clone(),partition_table);
                    let res = res.clone();
                    if res.is_some(){
                        self.hits = self.hits + 1;
                    } else {
                        self.misses = self.misses + 1;
                    }
                    responder.send(res).unwrap();
                },
                Command::Set { key_value, responder} => {
                    let partition_table = Arc::get_mut(&mut self.partition_table).unwrap();
                    let res = (self.setter)(key_value,partition_table);
                    responder.send(res).unwrap();
                },
                Command::Delete { key, responder } => {
                    let partition_table = Arc::get_mut(&mut self.partition_table).unwrap();
                    let res = (self.deleter)(key,partition_table);
                    responder.send(res).unwrap();
                },
                Command::Len { responder} => {
                    let partition_table = Arc::get_mut(&mut self.partition_table).unwrap();
                    let res = (self.length)(partition_table);
                    responder.send(res).unwrap();
                },
                Command::List { responder} =>  {
                    let partition_table = Arc::get_mut(&mut self.partition_table).unwrap();
                    let res = (self.lister)(partition_table);
                    responder.send(res.unwrap()).unwrap();
                },
                Command::Stats { responder} =>  {
                    let stats = self.get_stats();
                    responder.send(stats).unwrap();
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
    Stats{
        responder: oneshot::Sender<(String, u32, u32, u32)>,
    },

}

#[derive(Debug,Clone,PartialEq, Eq, Hash)]
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
    src_dst_map: Arc<BTreeMap<u32, HashMap<(u32,u16), BTreeMap<u32, HashMap<(u32,u16), V>>>>>,
    flow_map: Arc<HashMap<K,V>>,
    name: String,
    part_number: u32,
}

impl<K,V> FlowMap<K,V> 
where
    K: Eq + Hash + Ord + Clone,
    V: Clone,
{
    pub fn new() -> Self{
        Self { 
            src_dst_map: Arc::new(BTreeMap::new()),
            flow_map: Arc::new(HashMap::new()),
            name: "".to_string(),
            part_number: 0,
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
    fn new() -> Self {
        FlowMap::<K,V>::new()
    }
    fn set_id(&mut self, name: String, part_number: u32){

    }
    fn get_id(&mut self) -> (String, u32) {
        ("".to_string(),0)
    }
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
    fn set_id(&mut self, name: String, part_number: u32);
    fn get_id(&mut self) -> (String, u32);
    fn new() -> Self;
}

impl<K, V> GenericMap<K, V> for HashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn new() -> Self {
        HashMap::<K,V>::new()
    }
    fn set_id(&mut self, name: String, part_number: u32){

    }
    fn get_id(&mut self) -> (String, u32) {
        ("".to_string(),0)
    }
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
        self.remove(&key)
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
    fn new() -> Self {
        BTreeMap::<K,V>::new()
    }
    fn set_id(&mut self, name: String, part_number: u32){

    }
    fn get_id(&mut self) -> (String, u32) {
        ("".to_string(),0)
    }
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
        self.remove(&key)
    }

    fn length(&mut self) -> usize {
        self.len()
    }
}


pub fn flow_map_funcs() -> 
    (
        impl FnMut(FlowNetKey, &mut FlowMap<FlowNetKey, FlowAction>) -> Option<FlowAction> + Clone,
        impl FnMut(KeyValue<FlowNetKey,FlowAction>, &mut FlowMap<FlowNetKey, FlowAction>) -> Option<FlowAction> + Clone,
        impl FnMut(FlowNetKey, &mut FlowMap<FlowNetKey, FlowAction>) -> Option<FlowAction> + Clone,
        impl FnMut(&mut FlowMap<FlowNetKey, FlowAction>) -> Option<Vec<KeyValue<FlowNetKey, FlowAction>>> + Clone,
        impl FnMut(&mut FlowMap<FlowNetKey, FlowAction>) -> usize + Clone,
    )
{
    let deleter = |k: FlowNetKey, p: &mut FlowMap<FlowNetKey, FlowAction>| {
        p.deleter(k)
    };

    let getter = |key: FlowNetKey, p: &mut FlowMap<FlowNetKey, FlowAction>| {
        let flow_map = Arc::get_mut(&mut p.src_dst_map).unwrap();

        let mut mis_counter = 0;

        let mut lookup = |src_mask: &u32, src_key: &mut HashMap<(u32, u16), BTreeMap<u32, HashMap<(u32, u16), FlowAction>>>, src_port: u16, dst_port: u16| {
            let mask_bin = 4294967295 - src_mask;
            let masked: u32 = key.src_net & mask_bin;
            let res = src_key.get(&(masked, src_port));
            match res {
                Some(dst_map) => {
                    for (dst_mask, dst_key) in dst_map {
                        let mask_bin = 4294967295 - dst_mask;
                        let masked: u32 = key.dst_net & mask_bin;
                        let res = dst_key.get(&(masked, dst_port));
                        match res {
                            Some(action) => {
                                let action = action.clone();
                                return Some(action);
                            },
                            None => {},
                        }
                    }
                },
                None => {},
            };
            mis_counter = mis_counter + 1;
            None
        };

        for (src_mask, src_key) in flow_map {
            let res = lookup(src_mask, src_key, key.src_port, key.dst_port);
            match res {
                Some(action) => {
                    //println!("miscounter {}", mis_counter);
                    return Some(action);
                },
                None => {
                    let res = lookup(src_mask, src_key, 0, key.dst_port);
                    match res {
                        Some(action) => {
                            //println!("miscounter {}", mis_counter);
                            return Some(action);
                        },
                        None => {
                            let res = lookup(src_mask, src_key, key.src_port, 0);
                            match res {
                                Some(action) => {
                                    //println!("miscounter {}", mis_counter);
                                    return Some(action);
                                },
                                None => {
                                    let res = lookup(src_mask, src_key, 0, 0);
                                    match res {
                                        Some(action) => {
                                            //println!("miscounter {}", mis_counter);
                                            return Some(action);
                                        },
                                        None => {
                                            
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        None    
    };

    let setter = |k: KeyValue<FlowNetKey,FlowAction>, p: &mut FlowMap<FlowNetKey, FlowAction>| {
        let src_mask = 4294967295 - k.key.src_mask;
        let dst_mask = 4294967295 - k.key.dst_mask;
        let flow_map = Arc::get_mut(&mut p.flow_map).unwrap();
        flow_map.insert(k.clone().key, k.clone().value);
        let src_map = Arc::get_mut(&mut p.src_dst_map).unwrap();
        let res = src_map.get_mut(&src_mask);
        match res {
            Some(src_key) => {
                let res = src_key.get_mut(&(k.key.src_net, k.key.src_port));
                match res {
                    Some(dst_map) => {
                        
                        let res = dst_map.get_mut(&dst_mask);
                        match res {
                            Some(dst_key) => {
                                let res = dst_key.insert((k.key.dst_net, k.key.dst_port), k.value.clone());
                                match res {
                                    Some(action) => {
                                        //println!("updated action {:?} with {:?}", action, k.value);
                                        return Some(action);
                                    },
                                    None =>{
                                        //println!("added new dst_key {:?} {:?}",(flow.dst_net, flow.dst_port), flow.action.clone());
                                    },
                                }
                            },
                            None => {
                                let mut dst_key = HashMap::new();
                                let res = dst_key.insert((k.key.dst_net, k.key.dst_port), k.value.clone());
                                dst_map.insert(dst_mask, dst_key);
                                return res;

                            },
                        }
                    },
                    None => {
                        let mut dst_key = HashMap::new();
                        let res = dst_key.insert((k.key.dst_net, k.key.dst_port), k.value.clone());
                        let mut dst_map = BTreeMap::new();
                        dst_map.insert(dst_mask, dst_key);
                        src_key.insert((k.key.src_net, k.key.src_port), dst_map);
                        return res;
                    },
                }
            },
            None => {
                let mut dst_key = HashMap::new();
                let res = dst_key.insert((k.key.dst_net, k.key.dst_port), k.value.clone());
                let mut dst_map = BTreeMap::new();
                dst_map.insert(dst_mask, dst_key);
                let mut src_key = HashMap::new();
                src_key.insert((k.key.src_net, k.key.src_port), dst_map);
                src_map.insert(src_mask, src_key);
                return res;
            },
        }
        None
    };

    let lister = |p: &mut FlowMap<FlowNetKey, FlowAction>| {
        p.lister()
    };

    let length = |p: &mut FlowMap<FlowNetKey, FlowAction>| {
        p.length()
    };
    (getter, setter, deleter, lister, length)
}

pub fn defaults<K,V,P>() -> 
    (
        impl FnMut(K, &mut P) -> Option<V> + Clone,
        impl FnMut(KeyValue<K,V>, &mut P) -> Option<V> + Clone,
        impl FnMut(K, &mut P) -> Option<V> + Clone,
        impl FnMut(&mut P) -> Option<Vec<KeyValue<K, V>>> + Clone,
        impl FnMut(&mut P) -> usize + Clone,
    )
    where
        P: GenericMap<K,V>,
        K: Clone,
        V: Clone,
{
    let deleter = |k: K, p: &mut P| {
        p.deleter(k)
    };

    let getter = |k: K, p: &mut P| {
        p.getter(k)
    };

    let setter = |k: KeyValue<K,V>, p: &mut P| {
        p.setter(k)
    };

    let lister = |p: &mut P| {
        p.lister()
    };

    let length = |p: &mut P| {
        p.length()
    };
    (getter, setter, deleter, lister, length)
}

