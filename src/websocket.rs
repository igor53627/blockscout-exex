//! Phoenix-style WebSocket handler for Blockscout frontend compatibility
//!
//! Implements Phoenix Channels V2 protocol with JSON serialization.
//! Message format: [join_ref, message_ref, topic, event, payload]

use std::collections::HashSet;
use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::{broadcast, Mutex};

use crate::api::ApiState;

#[derive(Clone, Debug)]
pub struct BroadcastMessage {
    pub topic: String,
    pub event: String,
    pub payload: Value,
}

pub type Broadcaster = broadcast::Sender<BroadcastMessage>;

pub fn create_broadcaster() -> Broadcaster {
    let (tx, _) = broadcast::channel(1024);
    tx
}

pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<ApiState>) {
    let (sender, mut receiver) = socket.split();
    let sender = Arc::new(Mutex::new(sender));
    let joined_topics: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    let mut broadcast_rx = state.broadcaster.subscribe();
    let sender_clone = sender.clone();
    let topics_clone = joined_topics.clone();

    let broadcast_task = tokio::spawn(async move {
        while let Ok(msg) = broadcast_rx.recv().await {
            let topics = topics_clone.lock().await;
            if topics.contains(&msg.topic) {
                let phoenix_msg = json!([null, null, &msg.topic, &msg.event, &msg.payload]);
                let mut sender = sender_clone.lock().await;
                if sender
                    .send(Message::Text(phoenix_msg.to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    });

    while let Some(msg) = receiver.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!("WebSocket receive error: {}", e);
                break;
            }
        };

        match msg {
            Message::Text(text) => {
                if let Some(response) = handle_phoenix_message(&text, &joined_topics).await {
                    let mut sender = sender.lock().await;
                    if sender.send(Message::Text(response.into())).await.is_err() {
                        break;
                    }
                }
            }
            Message::Binary(_) => {
                tracing::debug!("Received binary message, ignoring");
            }
            Message::Ping(data) => {
                let mut sender = sender.lock().await;
                if sender.send(Message::Pong(data)).await.is_err() {
                    break;
                }
            }
            Message::Pong(_) => {}
            Message::Close(_) => break,
        }
    }

    broadcast_task.abort();
    tracing::debug!("WebSocket connection closed");
}

async fn handle_phoenix_message(
    text: &str,
    joined_topics: &Arc<Mutex<HashSet<String>>>,
) -> Option<String> {
    let msg: Value = serde_json::from_str(text).ok()?;
    let arr = msg.as_array()?;

    if arr.len() < 5 {
        tracing::debug!("Invalid Phoenix message format: {:?}", arr);
        return None;
    }

    let join_ref = &arr[0];
    let msg_ref = &arr[1];
    let topic = arr[2].as_str()?;
    let event = arr[3].as_str()?;
    let payload = &arr[4];

    tracing::debug!(
        "Phoenix message: topic={}, event={}, payload={:?}",
        topic,
        event,
        payload
    );

    match event {
        "phx_join" => {
            let mut topics = joined_topics.lock().await;
            topics.insert(topic.to_string());
            tracing::info!("Client joined topic: {}", topic);

            Some(
                json!([
                    join_ref,
                    msg_ref,
                    topic,
                    "phx_reply",
                    { "status": "ok", "response": {} }
                ])
                .to_string(),
            )
        }

        "phx_leave" => {
            let mut topics = joined_topics.lock().await;
            topics.remove(topic);
            tracing::info!("Client left topic: {}", topic);

            Some(
                json!([
                    join_ref,
                    msg_ref,
                    topic,
                    "phx_reply",
                    { "status": "ok", "response": {} }
                ])
                .to_string(),
            )
        }

        "heartbeat" => Some(
            json!([
                null,
                msg_ref,
                "phoenix",
                "phx_reply",
                { "status": "ok", "response": {} }
            ])
            .to_string(),
        ),

        _ => {
            tracing::debug!("Unhandled event: {} on topic: {}", event, topic);
            None
        }
    }
}
