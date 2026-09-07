use futures_util::{StreamExt, TryStreamExt, future};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let addr = String::from("127.0.0.1:3434");
    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.expect("Failed to bind");
    println!("Listening on: {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(accept_connection(stream));
    }
}

async fn accept_connection(stream: TcpStream) {
    let addr = stream
        .peer_addr()
        .expect("Connected streams should have a peer address");

    println!("Accepted connection from: {}", addr);

    let ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("Error during the websocket handshake");

    println!("New WebSocket connection: {}", addr);

    let (write, read) = ws_stream.split();
    read.try_filter(|msg| future::ready(msg.is_text() || msg.is_binary()))
        .forward(write)
        .await
        .expect("Failed to forward messages");
}
