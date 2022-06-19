use std::collections::HashMap;
use std::sync::{Arc,Mutex};
use std::thread::JoinHandle;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use rand::Rng;
use std::time::Instant;
use itertools::Itertools;
use std::time::Duration;
use tokio::runtime::Runtime;




#[tokio::main]
async fn main() {

    let num_partitions = 8;
    let num_values = 10000;
    let num_runs: u32 = 1000000;
    let num_chunks = num_runs/num_partitions;
    //let num_chunks = num_runs/1;


    //let partitions = Arc::new(Mutex::new(HashMap::new()));

    let flow_table = FlowTable::new(num_partitions);

    let res = flow_table.run();
    //sleep(Duration::from_secs(2)).await;
    //let flow_table_clone = flow_table.clone();
    //tokio::join!(res);
    let partitions = flow_table.flow_table_partitions.lock().unwrap();
    //let partitions_clone = partitions.clone();
    /*
    let mut join_handlers = Vec::new();

    let partitions = partitions.lock().unwrap();
    let mut partitions_clone = partitions.clone();
    for part in 0..num_partitions{
        let p = FlowTablePartition::new(part);
        let (sender, receiver) = mpsc::unbounded_channel();
        
        partitions_clone.insert(part, sender);
        let handle = tokio::spawn(async move{
            p.recv(receiver).await.unwrap();
        });
        join_handlers.push(handle);
    }
    */
    
    let mut key_list = Vec::new();

    for n in 0..num_values {
        key_list.push(n);
    }
    let partitions_clone = partitions.clone();
    for n in key_list {
        let part = n_mod_m(n, num_partitions);
        let partiton_sender = partitions_clone.get(&part).unwrap();
        let key_value = KeyValue { key: n, value: format!("bla{}",n) };
        partiton_sender.clone().send(Command::Set { key_value: key_value.clone()  }).unwrap();
    }

    let mut sample_list = Vec::new();
    for _ in 0..num_runs {
        let mut rng = rand::thread_rng();
        let random_src_idx: usize = rng.gen_range(0..num_values.try_into().unwrap());
        sample_list.push(random_src_idx);
    }

    let mut chunk_list = Vec::new();
    for chunk in &sample_list.clone().into_iter().chunks(num_chunks.try_into().unwrap()) {
        chunk_list.push(chunk.collect::<Vec<_>>());
    }
    let now = Instant::now();


    let mut send_handlers = Vec::new();
    for chunk in chunk_list.clone() {
        println!("chunks {} size {}", chunk_list.len(), chunk.len());
        let partitions = partitions_clone.clone();
        let res = tokio::spawn(async move {
            for sample in chunk{
                let part = n_mod_m(sample, num_partitions.clone().try_into().unwrap());
                let partiton_sender = partitions.get(&part.try_into().unwrap()).unwrap();
                let (responder_sender, responder_receiver) = oneshot::channel();
                partiton_sender.clone().send(Command::Get { key: sample.try_into().unwrap(), responder: responder_sender }).unwrap();
                let res = responder_receiver.await.unwrap();
                //println!("res {}",res);
            }
        });
        send_handlers.push(res);
    }
    futures::future::join_all(send_handlers).await;
    println!("millisecs {}",now.elapsed().as_millis());

    //tokio::join!(res);
    //res.await;
    //futures::future::join_all(join_handlers).await;
}

#[derive(Debug)]
enum Command{
    Get{
        key: u32,
        responder: oneshot::Sender<String>,
    },
    Set{
        key_value: KeyValue,
    },
}

#[derive(Debug,Clone)]
struct KeyValue{
    key: u32,
    value: String,
}
#[derive(Debug,Clone)]
struct FlowTable {
    flow_table_partitions: Arc<Mutex<HashMap<u32,mpsc::UnboundedSender<Command>>>>,
    partitions: u32,
}

impl FlowTable{
    fn new(partitions: u32) -> Self {
        Self { 
            flow_table_partitions: Arc::new(Mutex::new(HashMap::new())),
            partitions,
        }
    }
    //async fn run(&self) -> Vec<Result<(), tokio::task::JoinError>>{
    fn run(&self) -> Vec<tokio::task::JoinHandle<()>>{
        println!("setting up partitions");
        let mut join_handlers = Vec::new();
        let mut partitions = self.flow_table_partitions.lock().unwrap();
        for part in 0..self.partitions{
            println!("setting up partition {}", part);
            let p = FlowTablePartition::new(part);
            let (sender, receiver) = mpsc::unbounded_channel();
            partitions.insert(part, sender);
            let handle = tokio::spawn(async move{
                p.recv(receiver).await.unwrap();
            });
            //handle.await;
            join_handlers.push(handle);
        }
        //futures::future::join_all(join_handlers).await;
        join_handlers
    }
}

#[derive(Clone)]
struct FlowTablePartition {
    flow_table: Arc<Mutex<HashMap<u32,String>>>,
    name: u32,
}

impl FlowTablePartition {
    fn new(name: u32) -> Self {
        Self{
            flow_table: Arc::new(Mutex::new(HashMap::new())),
            name,
        }
    }

    async fn recv(&self, mut receiver: mpsc::UnboundedReceiver<Command>) -> Result<(), Box<dyn std::error::Error + Send> >{
        while let Some(cmd) = receiver.recv().await {
            match cmd {
                Command::Get { key, responder} => {
                    let flow_table = self.flow_table.lock().unwrap();
                    let res = flow_table.get(&key).unwrap();
                    let res = res.clone();
                    responder.send(res).unwrap();
                },
                Command::Set { key_value } => {
                    let mut data = self.flow_table.lock().unwrap();
                    data.insert(key_value.key, key_value.value);
                },
            }
        }
        Ok(())
    }
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