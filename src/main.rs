use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use futures_util::SinkExt;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::broadcast;

// 定义一个广播通道，用于将 HTTP 收到的消息分发给所有的 WebSocket 客户端
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
    println!("服务器启动，监听: {}", addr);
    println!("Webhook 地址: http://<你的服务器IP>:3000/webhook");
    println!("WebSocket 地址: ws://<你的服务器IP>:3000/ws");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// 处理手机的 WebSocket 连接
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(tx): State<Arc<Tx>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, tx))
}

async fn handle_socket(mut socket: WebSocket, tx: Arc<Tx>) {
    // 订阅消息
    let mut rx = tx.subscribe();

    println!("新的手机客户端已连接");

    // 循环等待接收广播消息，并发送给手机
    while let Ok(msg) = rx.recv().await {
        if socket.send(Message::Text(msg)).await.is_err() {
            // 发送失败说明手机断开了
            break;
        }
    }
    println!("手机客户端已断开");
}

// 处理来自闲鱼系统的 HTTP POST 请求
async fn webhook_handler(
    State(tx): State<Arc<Tx>>,
    body: String, // 直接接收原始 body 字符串
) -> &'static str {
    println!("收到 Webhook 数据: {}", body);

    // 将收到的内容通过广播通道发送给所有连接的手机
    // 如果你需要解析 JSON，可以在这里用 serde_json 处理
    let _ = tx.send(body);

    "OK"
}
