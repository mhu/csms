use chrono::Local;
use futures_util::{SinkExt, StreamExt};
use rust_ocpp::v2_0_1::messages::boot_notification::{
    BootNotificationRequest, BootNotificationResponse,
};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};

struct CallError {
    code: String,
    description: String,
}

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

    let (mut write, mut read) = ws_stream.split();

    while let Some(message) = read.next().await {
        match message {
            Ok(msg) => {
                if !msg.is_text() {
                    continue;
                }

                match msg.to_text() {
                    Ok(msg_str) => match serde_json::from_str::<Value>(msg_str) {
                        Ok(request) => {
                            let unique_id = request.get(1).unwrap().as_str().unwrap();
                            let action = request.get(2).unwrap().as_str().unwrap();

                            match action {
                                "BootNotification" => {
                                    match handle_boot_notification_request(&request) {
                                        Ok(response) => {
                                            let json_string =
                                                serde_json::to_value(&response).unwrap();
                                            let response_array = json!([
                                                3,
                                                unique_id,
                                                "BootNotificationResponse",
                                                json_string
                                            ])
                                            .to_string();

                                            println!(
                                                "Sending response to {}: {}",
                                                action, response_array
                                            );

                                            write
                                                .send(Message::Text(Utf8Bytes::from(
                                                    response_array,
                                                )))
                                                .await
                                                .unwrap();
                                        }
                                        Err(e) => {
                                            let response_array =
                                                json!([4, unique_id, e.code, e.description])
                                                    .to_string();

                                            println!(
                                                "Sending response to {}: {}",
                                                action, response_array
                                            );

                                            write
                                                .send(Message::Text(Utf8Bytes::from(
                                                    response_array,
                                                )))
                                                .await
                                                .unwrap();
                                        }
                                    }
                                }
                                _ => println!("Received unknown request of type '{action}'"),
                            }
                        }
                        Err(e) => println!("Failed to parse message '{msg_str}': {e}"),
                    },
                    Err(e) => {
                        println!("Failed to parse websocket message to string: {e}")
                    }
                }
            }
            Err(e) => println!("Error: {e}"),
        }
    }
}

fn handle_boot_notification_request(
    request: &Value,
) -> Result<BootNotificationResponse, CallError> {
    match request.get(3) {
        Some(payload) => {
            match serde_json::from_value::<BootNotificationRequest>(payload.to_owned()) {
                Ok(_payload) => Ok(BootNotificationResponse {
                    current_time: Local::now().to_utc(),
                    interval: 5,
                    status: rust_ocpp::v2_0_1::enumerations::registration_status_enum_type::RegistrationStatusEnumType::Accepted,
                    status_info: None
                }),
                Err(e) => {
                    Err(CallError {
                        code: String::from("FormationViolation"),
                        description: String::from("Invalid payload format: ") + &e.to_string()
                    })
                }
            }
        }
        None => Err(CallError {
            code: String::from("FormationViolation"),
            description: String::from("Missing payload"),
        }),
    }
}
