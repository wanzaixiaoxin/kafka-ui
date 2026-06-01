use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter};
use tracing::{info, warn};

use crate::kafka::consumer_service::ConsumedMessage;
use crate::store::connection_store::ConnectionRecord;

// ---- 数据结构 ----

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportOptions {
    pub topic: String,
    pub partition: Option<i32>,
    pub offset_start: Option<String>,
    pub offset_end: Option<String>,
    pub max_count: u32,
    pub format: String,
    pub save_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportOptions {
    pub topic: String,
    pub file_path: String,
    pub format: String,
    pub key_field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportExportProgress {
    pub status: String, // "running" | "completed" | "error"
    pub current: u64,
    pub total: u64,
    pub percent: f64,
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub success_count: Option<u64>,
}

/// 输出格式枚举（避免 Box<dyn Write> 的 downcast 问题）
enum OutputWriter {
    Jsonl(BufWriter<File>),
    Csv(csv::Writer<File>),
}

impl OutputWriter {
    fn write_jsonl(&mut self, cm: &ConsumedMessage) {
        if let OutputWriter::Jsonl(ref mut w) = self {
            let line = serde_json::to_string(cm).unwrap_or_default();
            let _ = writeln!(w, "{}", line);
        }
    }

    fn write_csv(&mut self, cm: &ConsumedMessage) -> Result<(), String> {
        if let OutputWriter::Csv(ref mut w) = self {
            w.write_record(&[
                &cm.topic,
                &cm.partition.to_string(),
                &cm.offset,
                cm.key.as_deref().unwrap_or(""),
                &cm.value,
                &cm.timestamp.to_string(),
                &cm.headers.as_ref()
                    .map(|h| serde_json::to_string(h).unwrap_or_default())
                    .unwrap_or_default(),
            ]).map_err(|e| format!("CSV 写入失败: {e}"))?;
        }
        Ok(())
    }

    fn flush(&mut self) {
        match self {
            OutputWriter::Jsonl(w) => { let _ = w.flush(); }
            OutputWriter::Csv(w) => { let _ = w.flush(); }
        }
    }
}

// ---- 导出逻辑（复用 ConsumerService::fetch_messages 读消息，这里只写文件）----

pub fn write_export_file(
    app_handle: &AppHandle,
    messages: &[ConsumedMessage],
    format: &str,
    save_path: &str,
    _topic: &str,
) -> Result<u64, String> {
    let total = messages.len() as u64;
    info!("[export] 写入文件: {} 条消息, format={}, path={}", total, format, save_path);

    if total == 0 {
        info!("[export] 无消息可导出");
        return Ok(0);
    }

    let file = File::create(save_path)
        .map_err(|e| format!("无法创建文件 {}: {e}", save_path))?;
    let mut writer = match format {
        "csv" => {
            OutputWriter::Csv(csv::WriterBuilder::new().has_headers(true).from_writer(file))
        }
        _ => {
            OutputWriter::Jsonl(BufWriter::new(file))
        }
    };

    let t0 = std::time::Instant::now();
    for (i, msg) in messages.iter().enumerate() {
        match format {
            "csv" => { let _ = writer.write_csv(msg); }
            _ => writer.write_jsonl(msg),
        }

        // 每 100 条或每 500ms 发一次进度
        if i % 100 == 0 || (i > 0 && i as u64 == total - 1) {
            let percent = ((i + 1) as f64 / total as f64 * 100.0).min(100.0);
            let _ = app_handle.emit("kafka:export:progress", ImportExportProgress {
                status: "running".into(),
                current: (i + 1) as u64,
                total,
                percent,
                error: None,
                error_count: None,
                success_count: None,
            });
        }
    }

    writer.flush();
    drop(writer);

    let elapsed = t0.elapsed().as_secs_f64();
    info!("[export] 文件写入完成: {} 条消息, 耗时 {:.1}s", total, elapsed);
    Ok(total)
}

// ---- 导入逻辑 ----

pub fn import_messages(
    app_handle: &AppHandle,
    _conn: &ConnectionRecord,
    producer: &FutureProducer<DefaultClientContext>,
    opts: &ImportOptions,
    cancel: Arc<AtomicBool>,
) -> Result<(), String> {
    let topic = &opts.topic;
    let format = opts.format.as_str();
    let file_path = &opts.file_path;

    info!("[import] 开始导入 topic='{}', format={}, path={}", topic, format, file_path);

    // 文件大小作为进度总量估算（避免预扫描二次读文件）
    let total_bytes = std::fs::metadata(file_path)
        .map(|m| m.len())
        .unwrap_or(0);

    let mut current: u64 = 0;
    let mut success: u64 = 0;
    let mut errors: u64 = 0;
    let mut last_progress = std::time::Instant::now();
    let t0 = std::time::Instant::now();

    if format == "csv" {
        // CSV 模式
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(BufReader::new(
                File::open(file_path)
                    .map_err(|e| format!("无法打开 CSV 文件 {}: {e}", file_path))?
            ));
        let csv_headers: Vec<String> = rdr.headers()
            .map_err(|e| format!("CSV 表头解析失败: {e}"))?
            .iter()
            .map(|s| s.to_string())
            .collect();

        for result in rdr.records() {
            if cancel.load(Ordering::Relaxed) {
                info!("[import] 用户取消导入");
                break;
            }
            current += 1;

            match result {
                Ok(record) => {
                    match parse_csv_to_kafka_message(&record, &csv_headers, topic, opts.key_field.as_deref()) {
                        Ok(msg) => {
                            match send_with_retry(producer, &msg) {
                                Ok(_) => success += 1,
                                Err(e) => {
                                    errors += 1;
                                    warn!("[import] 行 {} 发送失败: {}", current, e);
                                }
                            }
                        }
                        Err(e) => {
                            errors += 1;
                            warn!("[import] 行 {} 解析失败: {}", current, e);
                        }
                    }
                }
                Err(e) => {
                    errors += 1;
                    warn!("[import] 行 {} 读取失败: {}", current, e);
                }
            }

            emit_import_progress(app_handle, &mut last_progress, current, total_bytes, success, errors, None);
        }
    } else {
        // JSONL 模式
        let file = File::open(file_path)
            .map_err(|e| format!("无法打开文件 {}: {e}", file_path))?;
        let reader = BufReader::new(file);
        for line_result in reader.lines() {
            if cancel.load(Ordering::Relaxed) {
                info!("[import] 用户取消导入");
                break;
            }
            current += 1;

            match line_result {
                Ok(line) => {
                    if line.trim().is_empty() { continue; }
                    match parse_jsonl_to_kafka_message(&line, topic) {
                        Ok(msg) => {
                            match send_with_retry(producer, &msg) {
                                Ok(_) => success += 1,
                                Err(e) => {
                                    errors += 1;
                                    warn!("[import] 行 {} 发送失败: {}", current, e);
                                }
                            }
                        }
                        Err(e) => {
                            errors += 1;
                            warn!("[import] 行 {} 解析失败: {}", current, e);
                        }
                    }
                }
                Err(e) => {
                    errors += 1;
                    warn!("[import] 行 {} 读取失败: {}", current, e);
                }
            }

            emit_import_progress(app_handle, &mut last_progress, current, total_bytes, success, errors, None);
        }
    }

    let elapsed = t0.elapsed().as_secs_f64();
    info!("[import] 完成: 总计 {} 行, 成功 {}, 失败 {}, 耗时 {:.1}s",
          current, success, errors, elapsed);

    let _ = app_handle.emit("kafka:import:progress", ImportExportProgress {
        status: "completed".into(),
        current,
        total: current,
        percent: 100.0,
        error: None,
        error_count: Some(errors),
        success_count: Some(success),
    });

    Ok(())
}

// ---- 辅助函数 ----

fn send_with_retry(
    producer: &FutureProducer<DefaultClientContext>,
    msg: &crate::kafka::producer_service::KafkaMessage,
) -> Result<(), String> {
    use rdkafka::util::Timeout;

    for attempt in 0..3 {
        let mut record = FutureRecord::to(&msg.topic)
            .payload(&msg.value);

        if let Some(ref k) = msg.key {
            record = record.key(k);
        }
        if let Some(p) = msg.partition {
            record = record.partition(p);
        }

        // FutureProducer::send 返回 Future，需要用 block_on
        match futures::executor::block_on(
            producer.send(record, Timeout::After(Duration::from_secs(5)))
        ) {
            Ok(_) => return Ok(()),
            Err((e, _original)) if attempt < 2 => {
                warn!("[import] 发送重试 {}/3: {}", attempt + 1, e);
                std::thread::sleep(Duration::from_millis(500));
            }
            Err((e, _)) => return Err(format!("发送失败: {e}")),
        }
    }
    Err("发送失败: 超出重试次数".into())
}

fn parse_jsonl_to_kafka_message(line: &str, default_topic: &str) -> Result<crate::kafka::producer_service::KafkaMessage, String> {
    let v: serde_json::Value = serde_json::from_str(line)
        .map_err(|e| format!("JSON 解析失败: {e}"))?;

    let value = v.get("value")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| v.to_string());

    let key = v.get("key")
        .and_then(|k| k.as_str())
        .map(|s| s.to_string());

    let headers = v.get("headers")
        .and_then(|h| {
            serde_json::from_value::<HashMap<String, String>>(h.clone()).ok()
        });

    Ok(crate::kafka::producer_service::KafkaMessage {
        topic: default_topic.to_string(),
        key,
        value,
        partition: None,
        headers,
    })
}

fn parse_csv_to_kafka_message(
    record: &csv::StringRecord,
    headers: &[String],
    default_topic: &str,
    key_field: Option<&str>,
) -> Result<crate::kafka::producer_service::KafkaMessage, String> {
    let mut value = String::new();
    let mut key: Option<String> = None;
    let mut headers_map: Option<HashMap<String, String>> = None;

    for (i, h) in headers.iter().enumerate() {
        let field_val = record.get(i).unwrap_or("").to_string();
        match h.as_str() {
            "value" => value = field_val,
            "key" => key = if field_val.is_empty() { None } else { Some(field_val) },
            "headers" => {
                if !field_val.is_empty() {
                    headers_map = serde_json::from_str::<HashMap<String, String>>(&field_val).ok();
                }
            }
            _ => {}
        }
    }

    if let Some(kf) = key_field {
        if let Some(idx) = headers.iter().position(|h| h == kf) {
            key = record.get(idx).map(|s| s.to_string());
        }
    }

    if value.is_empty() {
        return Err("缺少 value 字段".into());
    }

    Ok(crate::kafka::producer_service::KafkaMessage {
        topic: default_topic.to_string(),
        key,
        value,
        partition: None,
        headers: headers_map,
    })
}

fn emit_import_progress(
    app_handle: &AppHandle,
    last_progress: &mut std::time::Instant,
    current: u64,
    total: u64,
    success: u64,
    errors: u64,
    error_msg: Option<String>,
) {
    let now = std::time::Instant::now();
    if current % 100 == 0 || now.duration_since(*last_progress) > Duration::from_millis(500) {
        *last_progress = now;
        let percent = if total > 0 {
            current as f64 / total as f64 * 100.0
        } else {
            0.0
        };
        let _ = app_handle.emit("kafka:import:progress", ImportExportProgress {
            status: if error_msg.is_some() { "error".into() } else { "running".into() },
            current,
            total,
            percent,
            error: error_msg,
            error_count: Some(errors),
            success_count: Some(success),
        });
    }
}
