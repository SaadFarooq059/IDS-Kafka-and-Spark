use serde::Serialize;
use sysinfo::{System, SystemExt, ProcessExt};
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{S3Client, S3, PutObjectRequest, PutObjectError};
use tokio;


struct ProcessInfo {
    pid: i32,
    name: String,
}

fn get_processes() -> Vec<(i32, String)> {
    let mut sys = System::new_all();
    sys.refresh_all();

    sys.processes()
       .iter()
       .map(|(&pid, proc_)| (pid as i32, proc_.name().to_string()))
       .collect()
}

fn convert_to_json(processes: Vec<(i32, String)>) -> String {
    let process_info: Vec<ProcessInfo> = processes
        .into_iter()
        .map(|(pid, name)| ProcessInfo { pid, name })
        .collect();

    serde_json::to_string(&process_info).unwrap()
}

async fn upload_to_s3(data: String, bucket: String, key: String) -> Result<(), RusotoError<PutObjectError>> {
    let s3_client = S3Client::new(Region::default());
    let put_request = PutObjectRequest {
        bucket,
        key,
        body: Some(data.into_bytes().into()),
        ..Default::default()
    };

    s3_client.put_object(put_request).await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let processes = get_processes();
    let json_data = convert_to_json(processes);
    
    let bucket = "".to_string();
    let key = "".to_string();

    match upload_to_s3(json_data, bucket, key).await {
        Ok(_) => println!("Upload successful"),
        Err(e) => eprintln!("Error uploading to S3: {}", e),
    }
}
