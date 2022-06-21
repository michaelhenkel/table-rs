
use std::net::Ipv4Addr;
use std::net::IpAddr;
use rand::Rng;
use std::sync::{Arc,RwLock};
use itertools::Itertools;
#[derive(Clone)]
pub struct Datapath{
    pub partitions: Vec<Partition>,

}

impl Datapath{
    pub fn new() -> Self {
        Self {
            partitions: Vec::new(),
        }
    }
    pub fn add_partitions(&mut self, src_hosts: Vec<Ipv4Addr>, dst_hosts: Vec<Ipv4Addr>, num_of_packets: u32, num_of_partitions: u32){
        let complete_packet_list = generate_packets(num_of_packets, src_hosts, dst_hosts);
        let mut chunk_list = Vec::new();
        let num_chunks = num_of_packets/num_of_partitions;
        for chunk in &complete_packet_list.clone().into_iter().chunks(num_chunks.try_into().unwrap()) {
            chunk_list.push(chunk.collect::<Vec<_>>());
        }
        for chunk in chunk_list.clone() {
            let partition = Partition::new(chunk);
            self.partitions.push(partition);
        }
    }
}
#[derive(Clone)]
pub struct Partition{
    pub packet_list: Vec<Packet>, 
}

impl Partition{
    pub fn new(packet_list: Vec<Packet>) -> Self {
        Self { 
            packet_list,
        }
    }
}

#[derive(Clone)]
pub struct Packet{
    pub src_ip: Ipv4Addr,
    pub dst_ip: Ipv4Addr,
    pub src_port: u16,
    pub dst_port: u16,
}

pub fn get_packet(src_hosts: Vec<Ipv4Addr>, dst_hosts: Vec<Ipv4Addr>) -> Packet {
    let src_ip_res: Ipv4Addr;
    let dst_ip_res: Ipv4Addr;
    loop{
        let (src_ip, dst_ip) = random(src_hosts.clone(), dst_hosts.clone());
        if src_ip != dst_ip {
            src_ip_res = src_ip;
            dst_ip_res = dst_ip;
            break;
        }
    }
    let mut rng = rand::thread_rng();
    let random_src_port: u16 = rng.gen_range(1..65535);
    let random_dst_port: u16 = rng.gen_range(1..65535);
    let random_direction: u8 = rng.gen_range(1..8);
    if random_direction == 1 {
        Packet{
            src_ip: src_ip_res,
            dst_ip: dst_ip_res,
            src_port: random_src_port,
            dst_port: random_dst_port
        }
    } else {
        Packet{
            src_ip: dst_ip_res,
            dst_ip: src_ip_res,
            src_port: random_dst_port,
            dst_port: random_src_port
        }
    }

}
pub fn random(src_hosts: Vec<Ipv4Addr>, dst_hosts: Vec<Ipv4Addr>) -> (Ipv4Addr,Ipv4Addr) {
    let mut rng = rand::thread_rng();
    let random_src_idx: usize = rng.gen_range(0..src_hosts.clone().len());
    let random_dst_idx: usize = rng.gen_range(0..dst_hosts.clone().len());
    let src_ip = src_hosts.get(random_src_idx).unwrap();
    let dst_ip = dst_hosts.get(random_dst_idx).unwrap();


    (*src_ip, *dst_ip)
}

pub fn generate_packets(num_of_packets: u32, src_hosts: Vec<Ipv4Addr>, dst_hosts: Vec<Ipv4Addr>) -> Vec<Packet>{
    let mut packet_list = Vec::new();
    for p in 1..num_of_packets{
        let packet = get_packet(src_hosts.clone(), dst_hosts.clone());
        packet_list.push(packet);
    }
    packet_list
}