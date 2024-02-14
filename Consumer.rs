use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Serialize, Deserialize};
use csv::Reader;
use std::fs::File;
use std::error::Error;

#[derive(Debug, Serialize, Deserialize)]
struct NetworkTrafficData {
    #[serde(rename = "Unnamed: 0")]
    unnamed0: String,
    #[serde(rename = "Flow ID")]
    flow_id: String,
    #[serde(rename = "Source IP")]
    source_ip: String,
    #[serde(rename = "Source Port")]
    source_port: i32,
    #[serde(rename = "Destination IP")]
    destination_ip: String,
    #[serde(rename = "Destination Port")]
    destination_port: i32,
    protocol: i32,
    timestamp: String,
    #[serde(rename = "Flow Duration")]
    flow_duration: i64,
    #[serde(rename = "Total Fwd Packets")]
    total_fwd_packets: i32,
    #[serde(rename = "Total Backward Packets")]
    total_backward_packets: i32,
    #[serde(rename = "Total Length of Fwd Packets")]
    total_length_of_fwd_packets: i64,
    #[serde(rename = "Total Length of Bwd Packets")]
    total_length_of_bwd_packets: i64,
    #[serde(rename = "Fwd Packet Length Max")]
    fwd_packet_length_max: i32,
    #[serde(rename = "Fwd Packet Length Min")]
    fwd_packet_length_min: i32,
    #[serde(rename = "Fwd Packet Length Mean")]
    fwd_packet_length_mean: f64,
    #[serde(rename = "Fwd Packet Length Std")]
    fwd_packet_length_std: f64,
    #[serde(rename = "Bwd Packet Length Max")]
    bwd_packet_length_max: i32,
    #[serde(rename = "Bwd Packet Length Min")]
    bwd_packet_length_min: i32,
    #[serde(rename = "Bwd Packet Length Mean")]
    bwd_packet_length_mean: f64,
    #[serde(rename = "Bwd Packet Length Std")]
    bwd_packet_length_std: f64,
    #[serde(rename = "Flow Bytes/s")]
    flow_bytes_per_second: f64,
    #[serde(rename = "Flow Packets/s")]
    flow_packets_per_second: f64,
    #[serde(rename = "Flow IAT Mean")]
    flow_iat_mean: f64,
    #[serde(rename = "Flow IAT Std")]
    flow_iat_std: f64,
    #[serde(rename = "Flow IAT Max")]
    flow_iat_max: i64,
    #[serde(rename = "Flow IAT Min")]
    flow_iat_min: i64,
    #[serde(rename = "Fwd IAT Total")]
    fwd_iat_total: i64,
    #[serde(rename = "Fwd IAT Mean")]
    fwd_iat_mean: f64,
    #[serde(rename = "Fwd IAT Std")]
    fwd_iat_std: f64,
    #[serde(rename = "Fwd IAT Max")]
    fwd_iat_max: i64,
    #[serde(rename = "Fwd IAT Min")]
    fwd_iat_min: i64,
    #[serde(rename = "Bwd IAT Total")]
    bwd_iat_total: i64,
    #[serde(rename = "Bwd IAT Mean")]
    bwd_iat_mean: f64,
    #[serde(rename = "Bwd IAT Std")]
    bwd_iat_std: f64,
    #[serde(rename = "Bwd IAT Max")]
    bwd_iat_max: i64,
    #[serde(rename = "Bwd IAT Min")]
    bwd_iat_min: i64,
    #[serde(rename = "Fwd PSH Flags")]
    fwd_psh_flags: i32,
    #[serde(rename = "Bwd PSH Flags")]
    bwd_psh_flags: i32,
    #[serde(rename = "Fwd URG Flags")]
    fwd_urg_flags: i32,
    #[serde(rename = "Bwd URG Flags")]
    bwd_urg_flags: i32,
    #[serde(rename = "Fwd Header Length")]
    fwd_header_length: i32,
    #[serde(rename = "Bwd Header Length")]
    bwd_header_length: i32,
    #[serde(rename = "Fwd Packets/s")]
    fwd_packets_per_second: f64,
    #[serde(rename = "Bwd Packets/s")]
    bwd_packets_per_second: f64,
    #[serde(rename = "Min Packet Length")]
    min_packet_length: i32,
    #[serde(rename = "Max Packet Length")]
    max_packet_length: i32,
    #[serde(rename = "Packet Length Mean")]
    packet_length_mean: f64,
    #[serde(rename = "Packet Length Std")]
    packet_length_std: f64,
    #[serde(rename = "Packet Length Variance")]
    packet_length_variance: f64,
    #[serde(rename = "FIN Flag Count")]
    fin_flag_count: i32,
    #[serde(rename = "SYN Flag Count")]
    syn_flag_count: i32,
    #[serde(rename = "RST Flag Count")]
    rst_flag_count: i32,
    #[serde(rename = "PSH Flag Count")]
    psh_flag_count: i32,
    #[serde(rename = "ACK Flag Count")]
    ack_flag_count: i32,
    #[serde(rename = "URG Flag Count")]
    urg_flag_count: i32,
    #[serde(rename = "CWE Flag Count")]
    cwe_flag_count: i32,
    #[serde(rename = "ECE Flag Count")]
    ece_flag_count: i32,
    #[serde(rename = "Down/Up Ratio")]
    down_up_ratio: i32,
    #[serde(rename = "Average Packet Size")]
    average_packet_size: f64,
    #[serde(rename = "Avg Fwd Segment Size")]
    avg_fwd_segment_size: f64,
    #[serde(rename = "Avg Bwd Segment Size")]
    avg_bwd_segment_size: f64,
    #[serde(rename = "Fwd Header Length.1")]
    fwd_header_length_1: i32,
    #[serde(rename = "Fwd Avg Bytes")]
    fn produce_to_kafka(data: &NetworkTrafficData) -> Result<(), Box<dyn Error>> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "broker1:9092,broker2:9092,broker3:9092")
            .create()?;
        
        let topic = "network-traffic";
        let serialized_data = serde_json::to_string(data)?;
        producer.send(
            FutureRecord::to(topic)
                .payload(&serialized_data)
                .key(&data.flow_id),
            0,
        ).await?;
    
        Ok(())
    }
    
    fn main() -> Result<(), Box<dyn Error>> {
        let file_paths = vec![
            "file1.csv", "file2.csv", "file3.csv", // add the rest of your files
        ];
    
        for file_path in file_paths {
            let file = File::open(file_path)?;
            let mut rdr = Reader::from_reader(file);
    
            for result in rdr.deserialize() {
                let record: NetworkTrafficData = result?;
                produce_to_kafka(&record)?;
            }
        }
    
        Ok(())
    }
