use chrono::Local;
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use rust_ocpp::v2_0_1::messages::boot_notification::{
    BootNotificationRequest, BootNotificationResponse,
};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Error, Message, Utf8Bytes,
        handshake::server::{ErrorResponse, Request, Response},
    },
};

struct Call {
    message_type_id: u8,
    message_id: String,
    action: String,
    payload: Value,
}

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

    let process_headers_callback = |request: &Request, response: Response| {
        let uri = request.uri().to_string();
        let headers = request.headers();
        let protocol_header = headers
            .iter()
            .find(|header| header.0.eq("sec-websocket-protocol"));

        if !uri.starts_with("/ocpp/") {
            Err(ErrorResponse::new(Some(String::from("Malformed URI"))))
        } else if protocol_header.is_none() {
            Err(ErrorResponse::new(Some(String::from(
                "Missing header 'sec-websocket-protocol'",
            ))))
        } else if !protocol_header
            .unwrap()
            .1
            .to_str()
            .unwrap()
            .to_owned()
            .contains("ocpp2.0.1")
        {
            Err(ErrorResponse::new(Some(String::from(
                "Server supports OCPP 2.0.1 only",
            ))))
        } else {
            Ok(response)
        }
    };

    let ws_stream = tokio_tungstenite::accept_hdr_async(stream, process_headers_callback)
        .await
        .expect("Error during the websocket handshake");

    println!("New WebSocket connection: {}", addr);

    let (mut write, mut read) = ws_stream.split();

    while let Some(message) = read.next().await {
        match parse_message(message) {
            Ok(call) => match call.action.as_str() {
                "BootNotification" => match handle_boot_notification_request(call.payload) {
                    Ok(response) => {
                        write_call_result(&mut write, call.message_id, response).await;
                    }
                    Err(e) => {
                        write_call_error(&mut write, call.message_id, e).await;
                    }
                },
                _ => {
                    let action = call.action;

                    write_call_error(
                        &mut write,
                        call.message_id,
                        CallError {
                            code: CallErrorCode::NotImplemented,
                            description: format!("Action '{action}' is not implemented"),
                        },
                    )
                    .await;
                }
            },
            Err(e) => {
                println!("Failed to read websocket message: {e}");
            }
        }
    }
}

fn parse_message(message: Result<Message, Error>) -> Result<Call, String> {
    match message {
        Ok(msg) => {
            if msg.is_empty() {
                Err(String::from("Empty message"))
            } else if !msg.is_text() {
                Err(String::from("Message is not text"))
            } else {
                match msg.to_text() {
                    Ok(msg_str) => match serde_json::from_str::<Value>(msg_str) {
                        Ok(request) => {
                            // TODO: Improve parsing of `String` values
                            let message_type_id: u8 =
                                serde_json::from_value(request.get(0).unwrap().to_owned()).unwrap();
                            let message_id: String =
                                serde_json::from_value(request.get(1).unwrap().to_owned()).unwrap();
                            let action: String =
                                serde_json::from_value(request.get(2).unwrap().to_owned()).unwrap();
                            let payload = request.get(3).unwrap().to_owned();

                            let call = Call {
                                message_type_id,
                                message_id,
                                action,
                                payload,
                            };

                            if call.message_type_id != 2 {
                                Err(format!("Wrong message type: {message_type_id}"))
                            } else {
                                Ok(call)
                            }
                        }
                        Err(e) => Err(format!("Failed to parse JSON: {e}")),
                    },
                    Err(e) => Err(format!("Failed transform input to text: {e}")),
                }
            }
        }
        Err(e) => Err(format!("Received no message: {e}")),
    }
}

fn handle_boot_notification_request(payload: Value) -> Result<BootNotificationResponse, CallError> {
    match serde_json::from_value::<BootNotificationRequest>(payload) {
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

async fn write_call_result(
    write: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
    call_unique_id: String,
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
    call_unique_id: String,
    error: CallError,
) {
    let response_array =
        json!([4, call_unique_id, error.code.as_str(), error.description]).to_string();

    write
        .send(Message::Text(Utf8Bytes::from(response_array)))
        .await
        .unwrap();
}
