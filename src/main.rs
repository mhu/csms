use chrono::Local;
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use rust_ocpp::v2_0_1::messages::boot_notification::{
    BootNotificationRequest, BootNotificationResponse,
};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, Utf8Bytes},
};

enum CallErrorCode {
    FormationViolation,
    NotImplemented,
}

impl CallErrorCode {
    fn as_str(&self) -> &'static str {
        match self {
            CallErrorCode::FormationViolation => "FormationViolation",
            CallErrorCode::NotImplemented => "NotImplemented",
        }
    }
}

struct CallError {
    code: CallErrorCode,
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
                            let message_id = request.get(1).unwrap().as_str().unwrap();
                            let action = request.get(2).unwrap().as_str().unwrap();

                            match action {
                                "BootNotification" => {
                                    match handle_boot_notification_request(&request) {
                                        Ok(response) => {
                                            write_call_result(&mut write, message_id, response)
                                                .await;
                                        }
                                        Err(e) => {
                                            write_call_error(&mut write, message_id, e).await;
                                        }
                                    }
                                }
                                _ => {
                                    write_call_error(
                                        &mut write,
                                        message_id,
                                        CallError {
                                            code: CallErrorCode::NotImplemented,
                                            description: format!(
                                                "Action '{action}' is not implemented"
                                            ),
                                        },
                                    )
                                    .await;
                                }
                            }
                        }
                        Err(e) => {
                            // TODO: Can we send a `CallError` message here, since the message ID can not be parsed?
                            println!("Failed to parse message '{msg_str}': {e}")
                        }
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
                        code: CallErrorCode::FormationViolation,
                        description: String::from("Invalid payload format: ") + &e.to_string()
                    })
                }
            }
        }
        None => Err(CallError {
            code: CallErrorCode::FormationViolation,
            description: String::from("Missing payload"),
        }),
    }
}

async fn write_call_result(
    write: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
    call_unique_id: &str,
    payload: BootNotificationResponse, // TODO: Allow for more response payloads to be used
) {
    let response_type = String::from("BootNotificationResponse"); // TODO: Determine response type based on payload
    let json_string = serde_json::to_value(&payload).unwrap();
    let response_array = json!([3, call_unique_id, response_type, json_string]).to_string();

    write
        .send(Message::Text(Utf8Bytes::from(response_array)))
        .await
        .unwrap();
}

async fn write_call_error(
    write: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
    call_unique_id: &str,
    error: CallError,
) {
    let response_array =
        json!([4, call_unique_id, error.code.as_str(), error.description]).to_string();

    write
        .send(Message::Text(Utf8Bytes::from(response_array)))
        .await
        .unwrap();
}
