use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::broadcast;

// 定义广播通道类型
type Tx = broadcast::Sender<String>;

#[tokio::main]
async fn main() {
    // 创建广播通道，容量设为 100
    let (tx, _rx) = broadcast::channel::<String>(100);
    let app_state = Arc::new(tx);

    // 路由配置
    let app = Router::new()
        // 手机 App 连接的 WebSocket 接口
        .route("/ws", get(ws_handler))
        // 闲鱼系统填写的 Webhook 接口
        .route("/webhook", post(webhook_handler))
        .with_state(app_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("=========================================");
    println!("服务器启动，监听: {}", addr);
    println!("Webhook 地址: http://<你的服务器IP>:3000/webhook");
    println!("WebSocket 地址: ws://<你的服务器IP>:3000/ws");
    println!("=========================================");

    // 监听端口，优雅处理端口被占用的错误
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("❌ 启动失败: 无法绑定端口 {}。错误信息: {}", addr, e);
            eprintln!("提示: 可能是 3000 端口已被其他程序占用，请关闭之前的实例或使用 `fuser -k 3000/tcp` 清理。");
            std::process::exit(1);
        }
    };

    // 启动服务
    if let Err(e) = axum::serve(listener, app).await {
        eprintln!("❌ 服务器运行出错: {}", e);
    }
}

// 处理手机的 WebSocket 连接
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(tx): State<Arc<Tx>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, tx))
}

async fn handle_socket(mut socket: WebSocket, tx: Arc<Tx>) {
    let mut rx = tx.subscribe();
    println!("✅ 新的手机客户端已连接");

    // 定时发送 Ping 保活，防止 Nginx 或客户端断开长时间不活动的连接
    let mut interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        tokio::select! {
            // 收到新消息，推给手机
            Ok(msg) = rx.recv() => {
                if socket.send(Message::Text(msg)).await.is_err() {
                    break;
                }
            }
            // 每隔 30 秒发一个 Ping
            _ = interval.tick() => {
                if socket.send(Message::Ping(vec![])).await.is_err() {
                    break;
                }
            }
            // 监听手机端发来的控制帧（比如 Pong 或 Close）
            Some(Ok(_)) = socket.recv() => {
                // 收到客户端回复，说明连接正常，忽略即可
            }
        }
    }
    println!("❌ 手机客户端已断开");
}

// 处理来自闲鱼系统的 HTTP POST 请求
async fn webhook_handler(
    State(tx): State<Arc<Tx>>,
    body: axum::body::Bytes, // 使用 Bytes 接收，防止非 UTF-8 数据导致崩溃
) -> &'static str {
    // 尝试把收到的字节转为字符串
    match String::from_utf8(body.to_vec()) {
        Ok(text) => {
            // 方案二核心逻辑：尝试将收到的数据解析为 JSON
            if let Ok(json_val) = serde_json::from_str::<Value>(&text) {
                // serde_json 解析后，\u6d4b 会自动变成 "测"，然后再序列化回字符串
                let decoded_text = serde_json::to_string(&json_val).unwrap_or_else(|_| text.clone());
                println!("📩 收到 Webhook 数据 (已转码): {}", decoded_text);
                let _ = tx.send(decoded_text);
            } else {
                // 如果不是标准 JSON，直接原样转发
                println!("📩 收到 Webhook 数据 (纯文本): {}", text);
                let _ = tx.send(text);
            }
            "OK"
        }
        Err(_) => {
            println!("⚠️ 收到非 UTF-8 数据，已忽略");
            "Bad Data"
        }
    }
}
