use std::path::Path;

fn main() {
    // Test the filename generation logic
    let file1_path = "demo1.csv";
    let file2_path = "demo2.csv";
    
    let file1_name = Path::new(file1_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("file1");
    let file2_name = Path::new(file2_path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("file2");
    
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    
    let report_filename = format!("csv_diff_{}_vs_{}_{}.txt", file1_name, file2_name, timestamp);
    
    println!("Generated report filename: {}", report_filename);
}

