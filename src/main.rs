use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::sync::{Arc, Mutex};
use std::thread;

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};

// DataCount 用于存储每行数据及其出现次数
#[derive(Clone)]
struct DataCount {
    line: String,
    count: usize,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// 导入的源文件
    #[arg(short, long, default_value_t = String::from("JXJ.txt"))]
    file_path: String,

    /// 输出的文件名
    #[arg(short, long, default_value_t = String::from("result.csv"))]
    result_path: String,

    /// 使用的线程数量
    #[arg(short, long, default_value_t = 5)]
    concurrency: usize,
}

fn main() {
    let args = Args::parse();

    // 获取文件行数
    let total_lines = count_lines(&args.file_path).unwrap();

    // 创建 channel 用于传递数据
    let (data_sender, data_receiver) = std::sync::mpsc::channel();
    // 使用 Arc<Mutex<_>> 包裹 data_receiver
    let data_receiver = Arc::new(Mutex::new(data_receiver));
    // 创建进度条 (修正后的代码)
    let pb = ProgressBar::new(total_lines as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .unwrap() // 处理潜在的错误
            .progress_chars("#>-"),
    );
    pb.set_message("写入中");
    // 启动并发线程统计数据
    let data_count = Arc::new(Mutex::new(HashMap::new()));
    let mut handles = vec![];
    for _ in 0..args.concurrency {
        let data_count = data_count.clone();
        // 克隆 data_receiver 的 Arc 指针
        let data_receiver = data_receiver.clone();
        let handle = thread::spawn(move || {
            // 在循环中使用 lock() 获取 data_receiver
            for line in data_receiver.lock().unwrap().iter() {
                let mut data_count = data_count.lock().unwrap();
                *data_count.entry(line).or_insert(0) += 1;
            }
        });
        handles.push(handle);
    }
    // 读取文件并分块发送数据
    read_file(&args.file_path, data_sender, &pb).unwrap();

    // 等待所有线程完成
    for handle in handles {
        handle.join().unwrap();
    }

    // 写入结果、排序并合并
    write_sort_and_merge_result(
        &args.result_path,
        &mut data_count.lock().unwrap().clone(),
        &pb,
    )
        .unwrap();

    pb.finish_with_message("完成");
}

// 读取文件并将数据分块发送到 channel
fn read_file(
    file_path: &str,
    data_sender: std::sync::mpsc::Sender<String>,
    pb: &ProgressBar,
) -> std::io::Result<()> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        data_sender.send(line).unwrap();
        pb.inc(1);
    }

    Ok(())
}

// 统计每行数据的出现次数
fn count_data(
    data_receiver: std::sync::mpsc::Receiver<String>,
    data_count: Arc<Mutex<HashMap<String, usize>>>,
) {
    for line in data_receiver {
        let mut data_count = data_count.lock().unwrap();
        *data_count.entry(line).or_insert(0) += 1;
    }
}

// 将结果写入 CSV 文件、按 count 降序排序并合并重复数据
fn write_sort_and_merge_result(
    result_path: &str,
    data_count: &mut HashMap<String, usize>,
    pb: &ProgressBar,
) -> std::io::Result<()> {
    let mut data_count_list: Vec<DataCount> = data_count
        .iter()
        .map(|(line, count)| DataCount {
            line: line.clone(),
            count: *count,
        })
        .collect();

    // 按 count 降序排序
    data_count_list.sort_by(|a, b| b.count.cmp(&a.count));

    // 创建结果文件
    let mut result_file = File::create(result_path)?;

    // 写入 CSV 头部
    writeln!(result_file, "Line,Count")?;

    // 写入排序后的数据
    for data_count in data_count_list {
        writeln!(
            result_file,
            "{},{}",
            data_count.line, data_count.count
        )?;
        pb.inc(1);
    }

    Ok(())
}

// 统计文件行数
fn count_lines(file_path: &str) -> std::io::Result<u64> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    Ok(reader.lines().count() as u64)
}
