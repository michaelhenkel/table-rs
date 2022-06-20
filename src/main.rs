use rand::Rng;
use std::time::Instant;
use itertools::Itertools;

mod table;

use table::table::{Table, KeyValue};

#[tokio::main]
async fn main() {

    let num_partitions = 8;
    let num_values = 10000;
    let num_runs: u32 = 1000000;
    let num_chunks = num_runs/num_partitions;

    let mut flow_table = Table::new(num_partitions);

    let res = flow_table.run();

    let mut key_list = Vec::new();

    for n in 0..num_values {
        key_list.push(n);
    }

    for n in key_list {
        let key_value = KeyValue { key: n, value: format!("bla{}",n) };
        flow_table.set(key_value).await;
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
    let flow_table = flow_table.clone();
    for chunk in chunk_list.clone() {
        println!("chunks {} size {}", chunk_list.len(), chunk.len());
        let flow_table = flow_table.clone();
        let res = tokio::spawn(async move {           
            for sample in chunk{               
                let bla = flow_table.get(sample.try_into().unwrap()).await.unwrap();        
            }
        });
        send_handlers.push(res);
    }
    futures::future::join_all(send_handlers).await;
    println!("millisecs {}",now.elapsed().as_millis());
    futures::future::join_all(res).await;
}