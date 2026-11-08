use futures_util::{SinkExt, StreamExt};
use std::env;
use std::io::BufReader;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::Message;

fn load_tls_acceptor(cert_path: &str, key_path: &str) -> TlsAcceptor {
    let cert_file = std::fs::File::open(cert_path).expect("open cert failed");
    let certs: Vec<CertificateDer> = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<Result<_, _>>()
        .expect("parse certs failed");

    let key_file = std::fs::File::open(key_path).expect("open key failed");
    let key: PrivateKeyDer = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .expect("parse key failed")
        .expect("no private key found");

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("build tls config failed");

    TlsAcceptor::from(Arc::new(config))
}

async fn handle_ws_bridge<S>(stream: S, target_url: String)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let client_ws = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!("ws accept failed: {e}");
            return;
        }
    };

    let backend_ws = match tokio_tungstenite::connect_async(&target_url).await {
        Ok((ws, _)) => ws,
        Err(e) => {
            eprintln!("connect target {target_url} failed: {e}");
            return;
        }
    };

    println!("bridged to {target_url}");

    let (mut client_tx, mut client_rx) = client_ws.split();
    let (mut backend_tx, mut backend_rx) = backend_ws.split();

    let c2b = async {
        while let Some(item) = client_rx.next().await {
            match item {
                Ok(msg) => {
                    if backend_tx.send(msg).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("client recv error: {e}");
                    break;
                }
            }
        }
        let _ = backend_tx.send(Message::Close(None)).await;
    };

    let b2c = async {
        while let Some(item) = backend_rx.next().await {
            match item {
                Ok(msg) => {
                    if client_tx.send(msg).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("backend recv error: {e}");
                    break;
                }
            }
        }
        let _ = client_tx.send(Message::Close(None)).await;
    };

    tokio::select! {
        _ = c2b => {},
        _ = b2c => {},
    }

    println!("bridge closed");
}

async fn handle_connection(mut stream: TcpStream, target_url: String, tls_acceptor: TlsAcceptor) {
    let mut first_byte = [0u8; 1];
    if stream.peek(&mut first_byte).await.is_err() {
        return;
    }

    if first_byte[0] == 0x16 {
        // TLS 握手，之后走 wss 路径
        match tls_acceptor.accept(stream).await {
            Ok(tls_stream) => handle_ws_bridge(tls_stream, target_url).await,
            Err(e) => eprintln!("tls accept failed: {e}"),
        }
    } else {
        // 明文，走 ws 路径
        handle_ws_bridge(stream, target_url).await;
    }
}

#[tokio::main]
async fn main() {
    let listen_addr = env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:9000".into());
    let target_url = env::var("TARGET_URL")
        .unwrap_or_else(|_| "ws://127.0.0.1:8080/notify".into());
    let cert_path = env::var("TLS_CERT").unwrap_or_else(|_| "cert.pem".into());
    let key_path = env::var("TLS_KEY").unwrap_or_else(|_| "key.pem".into());

    let tls_acceptor = load_tls_acceptor(&cert_path, &key_path);
    let listener = TcpListener::bind(&listen_addr).await.expect("bind failed");

    println!("listening {listen_addr}, ws/wss auto, target {target_url}");

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let target = target_url.clone();
                let acceptor = tls_acceptor.clone();
                tokio::spawn(async move {
                    handle_connection(stream, target, acceptor).await;
                    let _ = addr;
                });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
}
