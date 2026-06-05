mod frb_generated;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use android_logger::Config;
use convex::{
    ConvexClient,
    ConvexClientBuilder,
    FunctionResult,
    Value, // Convex client and result types
    WebSocketState as ConvexWebSocketState,
};
use flutter_rust_bridge::{frb, DartFnFuture};
use futures::{
    channel::oneshot::{self, Sender},
    pin_mut, select_biased, FutureExt, StreamExt,
};
use log::{debug, error, LevelFilter};
use parking_lot::Mutex;
use base64::Engine;
use serde::Deserialize;

/// Initialise android_logger.  The `tracing` crate is compiled with its
/// `log` feature, so tracing events from the Convex SDK automatically
/// fall through to the `log` crate (and thus to android_logger) when no
/// tracing subscriber is installed.
///
/// When `verbose` is false only warnings and errors are emitted, keeping
/// logcat quiet in normal operation.
fn init_logging(verbose: bool) {
    let level = if verbose {
        LevelFilter::Debug
    } else {
        LevelFilter::Warn
    };
    android_logger::init_once(Config::default().with_max_level(level));
}

// Custom error type for Convex client operations, exposed to Dart.
#[derive(Debug, thiserror::Error)]
#[frb]
pub enum ClientError {
    /// An internal error within the mobile Convex client.
    #[error("InternalError: {msg}")]
    InternalError { msg: String },
    /// An application-specific error from a remote Convex backend function.
    #[error("ConvexError: {data}")]
    ConvexError { data: String },
    /// An unexpected server-side error from a remote Convex function.
    #[error("ServerError: {msg}")]
    ServerError { msg: String },
}

impl From<anyhow::Error> for ClientError {
    fn from(value: anyhow::Error) -> Self {
        Self::InternalError {
            msg: value.to_string(),
        }
    }
}

/// Upper bound on how long a single `subscribe` may wait for the worker to
/// flush its Subscribe message to a live socket.
///
/// The Convex SDK worker only acks a Subscribe after `communicate()` flushes it
/// to a *connected* socket (see convex `client/worker.rs`); on a dead or
/// still-connecting socket that flush parks until the connection comes up. With
/// no ceiling an FFI `subscribe` — e.g. one fired by the Dart resume-reconnect
/// while the iOS network stack isn't ready yet — could hang its Dart caller
/// indefinitely, wedging the reconnect orchestration. The Dart layer also bounds
/// the reconnect body, but this guarantees no native call hangs forever
/// regardless of which caller invokes it.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(30);

/// JWT claims structure for extracting expiration time.
#[derive(Deserialize)]
struct JwtClaims {
    exp: u64,
}

/// Decodes a JWT token and extracts the expiration timestamp.
/// Returns None if the token is malformed or doesn't contain an exp claim.
fn decode_jwt_expiry(token: &str) -> Option<u64> {
    // JWT format: header.payload.signature
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }

    // Decode payload (second part) using URL-safe base64
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .ok()?;

    let claims: JwtClaims = serde_json::from_slice(&payload).ok()?;
    Some(claims.exp)
}

/// WebSocket connection state exposed to Flutter/Dart.
///
/// This enum represents the current state of the WebSocket connection
/// to the Convex backend, allowing real-time connection monitoring.
#[derive(Debug, Clone)]
#[frb]
pub enum WebSocketConnectionState {
    /// The WebSocket is open and connected to the Convex backend.
    Connected,
    /// The WebSocket is closed and is connecting or reconnecting.
    Connecting,
}

impl From<ConvexWebSocketState> for WebSocketConnectionState {
    fn from(state: ConvexWebSocketState) -> Self {
        match state {
            ConvexWebSocketState::Connected => WebSocketConnectionState::Connected,
            ConvexWebSocketState::Connecting => WebSocketConnectionState::Connecting,
        }
    }
}

/// Trait defining the interface for handling subscription updates.
// Not directly exposed to Dart, used internally by subscribers.
pub trait QuerySubscriber: Send + Sync {
    fn on_update(&self, value: String); // Called when a new update is received
    fn on_error(&self, message: String, value: Option<String>); // Called on error with optional value
}

/// Adapter struct to implement QuerySubscriber using Dart callbacks.
pub struct CallbackSubscriber {
    on_update: Box<dyn Fn(String) + Send + Sync>, // Callback for updates
    on_error: Box<dyn Fn(String, Option<String>) + Send + Sync>, // Callback for errors
}

impl QuerySubscriber for CallbackSubscriber {
    fn on_update(&self, value: String) {
        (self.on_update)(value);
    }

    fn on_error(&self, message: String, value: Option<String>) {
        (self.on_error)(message, value);
    }
}

/// Opaque type for Dart, representing a subscription handle with cancellation.
#[frb(opaque)]
pub struct SubscriptionHandle {
    cancel_sender: Arc<Mutex<Option<Sender<()>>>>, // Sender to cancel the subscription
}

impl SubscriptionHandle {
    fn new(cancel_sender: Sender<()>) -> Self {
        SubscriptionHandle {
            cancel_sender: Arc::new(Mutex::new(Some(cancel_sender))),
        }
    }

    /// Cancels the subscription by sending a cancellation signal.
    #[frb(sync)]
    pub fn cancel(&self) {
        if let Some(sender) = self.cancel_sender.lock().take() {
            sender.send(()).unwrap();
        }
    }
}

/// Opaque type for Dart, representing an auth session handle with lifecycle management.
/// Used to control the token refresh loop and check authentication state.
#[frb(opaque)]
pub struct AuthHandle {
    cancel_sender: Arc<Mutex<Option<Sender<()>>>>,
    is_authenticated: Arc<AtomicBool>,
}

impl AuthHandle {
    fn new(cancel_sender: Sender<()>, is_authenticated: Arc<AtomicBool>) -> Self {
        AuthHandle {
            cancel_sender: Arc::new(Mutex::new(Some(cancel_sender))),
            is_authenticated,
        }
    }

    /// Disposes the auth session, stopping the token refresh loop and clearing authentication.
    #[frb(sync)]
    pub fn dispose(&self) {
        if let Some(sender) = self.cancel_sender.lock().take() {
            let _ = sender.send(());
        }
    }

    /// Returns whether the user is currently authenticated.
    #[frb(sync)]
    pub fn is_authenticated(&self) -> bool {
        self.is_authenticated.load(Ordering::SeqCst)
    }
}

/// Adapter for Dart functions as subscribers, handling async callbacks.
pub struct CallbackSubscriberDartFn {
    on_update: Box<dyn Fn(String) -> DartFnFuture<()> + Send + Sync>, // Async update callback
    on_error: Box<dyn Fn(String, Option<String>) -> DartFnFuture<()> + Send + Sync>, // Async error callback
}

impl QuerySubscriber for CallbackSubscriberDartFn {
    fn on_update(&self, value: String) {
        let future = (self.on_update)(value);
        tokio::spawn(async move {
            future.await;
        });
    }

    fn on_error(&self, message: String, value: Option<String>) {
        let future = (self.on_error)(message, value);
        tokio::spawn(async move {
            future.await;
        });
    }
}

/// Main Convex client struct, opaque to Dart, managing connections and operations.
#[frb(opaque)]
pub struct MobileConvexClient {
    // Interior-mutable so the in-app deployment switcher (W5) can repoint a live
    // client at a different deployment via `set_deployment_url`. parking_lot
    // Mutex (already used for `state_change_sender`) — reads are brief clones.
    deployment_url: Mutex<String>, // URL of the Convex deployment
    client_id: String,           // Client ID for authentication
    // Swappable inner client so we can force a full reconnect (drop + re-init)
    // on app resume after iOS suspension. tokio::sync::Mutex because init is
    // async (ConvexClientBuilder::build).
    client: tokio::sync::Mutex<Option<ConvexClient>>,
    rt: tokio::runtime::Runtime, // Tokio runtime for async operations
    // Channel sender for WebSocket state change notifications
    state_change_sender: Arc<Mutex<Option<tokio::sync::mpsc::Sender<ConvexWebSocketState>>>>,
}

impl MobileConvexClient {
    /// Creates a new MobileConvexClient instance with the given deployment URL and client ID.
    #[frb(sync)]
    pub fn new(deployment_url: String, client_id: String, verbose_logging: bool) -> MobileConvexClient {
        init_logging(verbose_logging);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        MobileConvexClient {
            deployment_url: Mutex::new(deployment_url),
            client_id,
            client: tokio::sync::Mutex::new(None),
            rt,
            state_change_sender: Arc::new(Mutex::new(None)),
        }
    }

    /// Sets up WebSocket connection state change listener.
    ///
    /// Must be called BEFORE any queries/mutations to capture all state changes.
    /// The callback will be invoked whenever the WebSocket transitions between
    /// Connected and Connecting states.
    ///
    /// # Arguments
    ///
    /// * `on_state_change` - Async callback invoked when connection state changes
    ///
    /// # Example
    ///
    /// ```dart
    /// await client.onWebsocketStateChange(
    ///   onStateChange: (state) async {
    ///     print('Connection state: ${state.name}');
    ///   },
    /// );
    /// ```
    #[frb]
    pub async fn on_websocket_state_change(
        &self,
        on_state_change: impl Fn(WebSocketConnectionState) -> DartFnFuture<()> + Send + Sync + 'static,
    ) -> Result<(), ClientError> {
        debug!("on_websocket_state_change() called");

        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel::<ConvexWebSocketState>(10);

        {
            let mut sender = self.state_change_sender.lock();
            *sender = Some(state_tx);
        }

        let on_state_change = Arc::new(on_state_change);
        self.rt.spawn(async move {
            debug!("WS state listener task started");
            while let Some(state) = state_rx.recv().await {
                debug!("WS state changed: {:?}", state);
                let dart_state = WebSocketConnectionState::from(state);
                let callback = on_state_change.clone();
                let future = (callback)(dart_state);
                let _ = future.await;
            }
            debug!("WS state listener exiting (channel closed)");
        });

        Ok(())
    }

    /// Retrieves or initializes a connected Convex client.
    ///
    /// Holds the inner client behind a `Mutex<Option<…>>` so [`force_reconnect`]
    /// can drop the existing instance and have the next call build a fresh
    /// connection.
    async fn connected_client(&self) -> anyhow::Result<ConvexClient> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }

        let url = self.deployment_url.lock().clone();
        let state_sender = self.state_change_sender.lock().clone();
        let client_id = self.client_id.to_owned();

        debug!("Building ConvexClient for {}", url);
        let mut builder = ConvexClientBuilder::new(url.as_str()).with_client_id(&client_id);

        if let Some(sender) = state_sender {
            builder = builder.with_on_state_change(sender);
        } else {
            error!("No state_change sender — state changes will not be emitted");
        }

        match builder.build().await {
            Ok(client) => {
                debug!("ConvexClient built successfully");
                *guard = Some(client.clone());
                Ok(client)
            }
            Err(e) => {
                error!("Failed to build ConvexClient: {:?}", e);
                Err(e)
            }
        }
    }

    /// Drops the underlying Convex client so the next operation builds a fresh
    /// WebSocket connection.
    ///
    /// This is the foundation for "reconnect on app resume" — iOS suspends the
    /// process while backgrounded, leaving the WS in a zombie state that the
    /// Rust SDK's 5s ping / 30s inactivity heartbeat takes up to ~35s to detect
    /// and recover from. Calling this forces immediate recovery.
    ///
    /// Callers (Dart side) are responsible for re-establishing auth via
    /// [`set_auth_with_refresh`] and re-firing any active subscriptions after
    /// invoking this method, since the previous [`AuthHandle`] and
    /// [`SubscriptionHandle`] values point at the dropped client and will no
    /// longer receive updates.
    #[frb]
    pub async fn force_reconnect(&self) -> Result<(), ClientError> {
        debug!("force_reconnect: dropping inner ConvexClient");
        let mut guard = self.client.lock().await;
        *guard = None;
        Ok(())
    }

    /// Repoints this client at a different deployment URL and drops the inner
    /// client so the next operation builds a fresh WebSocket against the new
    /// deployment. This is the native primitive behind the in-app deployment
    /// switcher (W5): the same `MobileConvexClient` instance, tokio runtime, and
    /// websocket-state channel are kept alive; only the inner Convex client is
    /// recreated against the new address (the URL is immutable inside the Convex
    /// SDK, so swapping deployments requires a transport rebuild).
    ///
    /// Like [`force_reconnect`], callers (Dart side) must replay auth via
    /// [`set_auth_with_refresh`] and re-fire any active subscriptions afterwards
    /// — the previous [`AuthHandle`]/[`SubscriptionHandle`] values point at the
    /// dropped client. Auth tokens validate across deployments that share an
    /// issuer, so no re-login is needed when switching among them.
    #[frb]
    pub async fn set_deployment_url(&self, url: String) -> Result<(), ClientError> {
        debug!("set_deployment_url: switching deployment to {}", url);
        *self.deployment_url.lock() = url;
        let mut guard = self.client.lock().await;
        *guard = None;
        Ok(())
    }

    /// Executes a query on the Convex backend.
    #[frb]
    pub async fn query(
        &self,
        name: String,
        args: HashMap<String, String>,
    ) -> Result<String, ClientError> {
        let mut client = self.connected_client().await?;
        debug!("got the client");
        let result = client.query(name.as_str(), parse_json_args(args)).await?;
        debug!("got the result");
        handle_direct_function_result(result)
    }

    /// Subscribes to real-time updates from a Convex query.
    #[frb]
    pub async fn subscribe(
        &self,
        name: String,
        args: HashMap<String, String>,
        on_update: impl Fn(String) -> DartFnFuture<()> + Send + Sync + 'static,
        on_error: impl Fn(String, Option<String>) -> DartFnFuture<()> + Send + Sync + 'static,
    ) -> Result<SubscriptionHandle, ClientError> {
        let subscriber = Arc::new(CallbackSubscriberDartFn {
            on_update: Box::new(on_update),
            on_error: Box::new(on_error),
        });
        self.internal_subscribe(name, args, subscriber)
            .await
            .map_err(Into::into)
    }

    /// Internal method for subscription logic.
    async fn internal_subscribe(
        &self,
        name: String,
        args: HashMap<String, String>,
        subscriber: Arc<dyn QuerySubscriber>,
    ) -> anyhow::Result<SubscriptionHandle> {
        let mut client = self.connected_client().await?;
        debug!("New subscription");
        // Bound the Subscribe flush: the SDK worker parks until the socket is
        // connected, so without this a subscribe on a dead/connecting socket
        // (e.g. during a resume reconnect on iOS) would hang the FFI caller
        // forever. See [`SUBSCRIBE_TIMEOUT`].
        let mut subscription = match tokio::time::timeout(
            SUBSCRIBE_TIMEOUT,
            client.subscribe(name.as_str(), parse_json_args(args)),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "subscribe '{}' timed out after {:?} (socket not connected)",
                    name,
                    SUBSCRIBE_TIMEOUT
                ));
            }
        };
        let (cancel_sender, cancel_receiver) = oneshot::channel::<()>();
        self.rt.spawn(async move {
            let cancel_fut = cancel_receiver.fuse();
            pin_mut!(cancel_fut);
            loop {
                select_biased! {
                    new_val = subscription.next().fuse() => {
                        let new_val = match new_val {
                            Some(val) => val,
                            None => {
                                log::warn!("Subscription stream ended for {}", &name);
                                break;
                            }
                        };
                        match new_val {
                            FunctionResult::Value(value) => {
                                debug!("Updating with {value:?}");
                                subscriber.on_update(serde_json::to_string(
                                    &serde_json::Value::from(value),
                                ).unwrap());
                            }
                            FunctionResult::ErrorMessage(message) => {
                                subscriber.on_error(message, None);
                            }
                            FunctionResult::ConvexError(error) => subscriber.on_error(
                                error.message,
                                Some(serde_json::ser::to_string(
                                    &serde_json::Value::from(error.data),
                                ).unwrap()),
                            ),
                        }
                    }
                    _ = cancel_fut => {
                        break;
                    }
                }
            }
            debug!("Subscription canceled");
        });
        Ok(SubscriptionHandle::new(cancel_sender))
    }

    /// Executes a mutation on the Convex backend.
    #[frb]
    pub async fn mutation(
        &self,
        name: String,
        args: HashMap<String, String>,
    ) -> Result<String, ClientError> {
        let result = self.internal_mutation(name, args).await?;
        handle_direct_function_result(result)
    }

    /// Internal method for mutation logic.
    async fn internal_mutation(
        &self,
        name: String,
        args: HashMap<String, String>,
    ) -> anyhow::Result<FunctionResult> {
        let mut client = self.connected_client().await?;
        self.rt
            .spawn(async move { client.mutation(&name, parse_json_args(args)).await })
            .await?
    }

    /// Executes an action on the Convex backend.
    #[frb]
    pub async fn action(
        &self,
        name: String,
        args: HashMap<String, String>,
    ) -> Result<String, ClientError> {
        debug!("Running action: {}", name);
        let result = self.internal_action(name, args).await?;
        debug!("Got action result: {:?}", result);
        handle_direct_function_result(result)
    }

    /// Internal method for action logic.
    async fn internal_action(
        &self,
        name: String,
        args: HashMap<String, String>,
    ) -> anyhow::Result<FunctionResult> {
        let mut client = self.connected_client().await?;
        debug!("Running action: {}", name);
        self.rt
            .spawn(async move { client.action(&name, parse_json_args(args)).await })
            .await?
    }

    /// Sets authentication token for the client.
    #[frb]
    pub async fn set_auth(&self, token: Option<String>) -> Result<(), ClientError> {
        Ok(self.internal_set_auth(token).await?)
    }

    /// Internal method for setting authentication.
    async fn internal_set_auth(&self, token: Option<String>) -> anyhow::Result<()> {
        let mut client = self.connected_client().await?;
        self.rt
            .spawn(async move { client.set_auth(token).await })
            .await
            .map_err(|e| e.into())
    }

    /// Sets authentication with automatic token refresh.
    ///
    /// The `fetch_token` callback is called:
    /// - Immediately to get the initial token
    /// - Automatically when the token is about to expire (60 seconds before expiry)
    ///
    /// The `on_auth_change` callback is called whenever auth state changes.
    ///
    /// Returns an AuthHandle that can be used to dispose the auth session.
    #[frb]
    pub async fn set_auth_with_refresh(
        &self,
        fetch_token: impl Fn() -> DartFnFuture<Option<String>> + Send + Sync + 'static,
        on_auth_change: impl Fn(bool) -> DartFnFuture<()> + Send + Sync + 'static,
    ) -> Result<AuthHandle, ClientError> {
        let is_authenticated = Arc::new(AtomicBool::new(false));
        let (cancel_sender, cancel_receiver) = oneshot::channel::<()>();

        let client = self.connected_client().await?;
        let is_auth_clone = is_authenticated.clone();

        let fetch_token = Arc::new(fetch_token);
        let on_auth_change = Arc::new(on_auth_change);

        // Buffer time before token expiry to trigger refresh (60 seconds)
        const REFRESH_BUFFER_SECS: u64 = 60;
        // Minimum refresh interval to prevent tight loops on errors
        const MIN_REFRESH_INTERVAL_SECS: u64 = 5;
        // Default refresh interval when JWT can't be decoded (5 minutes)
        const DEFAULT_REFRESH_INTERVAL_SECS: u64 = 300;

        // Spawn the token refresh loop
        self.rt.spawn(async move {
            let mut cancel_fut = cancel_receiver.fuse();
            let mut was_authenticated = false;

            loop {
                // Fetch token from Dart
                let fetch_token_clone = fetch_token.clone();
                let token_future = (fetch_token_clone)();

                let token_result = select_biased! {
                    _ = cancel_fut => {
                        // Cancelled - clear auth and exit
                        debug!("Auth refresh cancelled");
                        let mut client = client.clone();
                        let _ = client.set_auth(None).await;
                        if was_authenticated {
                            let on_auth_change_clone = on_auth_change.clone();
                            let future = (on_auth_change_clone)(false);
                            let _ = future.await;
                        }
                        break;
                    }
                    token = token_future.fuse() => token,
                };

                let now_secs = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                match token_result {
                    Some(token) => {
                        // Set the token
                        let mut client = client.clone();
                        client.set_auth(Some(token.clone())).await;

                        // Notify state change if needed
                        if !was_authenticated {
                            was_authenticated = true;
                            is_auth_clone.store(true, Ordering::SeqCst);
                            let on_auth_change_clone = on_auth_change.clone();
                            let future = (on_auth_change_clone)(true);
                            tokio::spawn(async move {
                                let _ = future.await;
                            });
                        }

                        // Decode expiry and schedule next refresh
                        let sleep_duration = if let Some(exp) = decode_jwt_expiry(&token) {
                            let refresh_at = exp.saturating_sub(REFRESH_BUFFER_SECS);
                            if refresh_at > now_secs {
                                Duration::from_secs(refresh_at - now_secs)
                            } else {
                                // Token already expired or about to, refresh immediately
                                Duration::from_secs(MIN_REFRESH_INTERVAL_SECS)
                            }
                        } else {
                            // Can't decode JWT, use default refresh interval
                            debug!("Could not decode JWT expiry, using default refresh interval");
                            Duration::from_secs(DEFAULT_REFRESH_INTERVAL_SECS)
                        };

                        debug!("Next token refresh in {:?}", sleep_duration);

                        // Sleep until refresh time or cancellation
                        let sleep_fut = tokio::time::sleep(sleep_duration).fuse();
                        pin_mut!(sleep_fut);
                        select_biased! {
                            _ = cancel_fut => {
                                debug!("Auth refresh cancelled during sleep");
                                let mut client = client.clone();
                                let _ = client.set_auth(None).await;
                                if was_authenticated {
                                    let on_auth_change_clone = on_auth_change.clone();
                                    let future = (on_auth_change_clone)(false);
                                    let _ = future.await;
                                }
                                break;
                            }
                            _ = sleep_fut => {
                                // Time to refresh, continue loop
                            }
                        }
                    }
                    None => {
                        // No token - clear auth
                        debug!("Token fetcher returned None, clearing auth");
                        let mut client = client.clone();
                        let _ = client.set_auth(None).await;

                        if was_authenticated {
                            is_auth_clone.store(false, Ordering::SeqCst);
                            let on_auth_change_clone = on_auth_change.clone();
                            let future = (on_auth_change_clone)(false);
                            tokio::spawn(async move {
                                let _ = future.await;
                            });
                        }

                        // Exit the loop when fetch_token returns None
                        break;
                    }
                }
            }

            debug!("Auth refresh loop ended");
        });

        Ok(AuthHandle::new(cancel_sender, is_authenticated))
    }
}

/// Utility function to parse HashMap arguments into Convex Value format.
fn parse_json_args(raw_args: HashMap<String, String>) -> BTreeMap<String, Value> {
    raw_args
        .into_iter()
        .map(|(k, v)| {
            (
                k,
                Value::try_from(
                    serde_json::from_str::<serde_json::Value>(&v)
                        .expect("Invalid JSON data from FFI"),
                )
                .expect("Invalid Convex data from FFI"),
            )
        })
        .collect()
}

/// Utility function to handle and serialize FunctionResult into a string or error.
fn handle_direct_function_result(result: FunctionResult) -> Result<String, ClientError> {
    match result {
        FunctionResult::Value(v) => serde_json::to_string(&serde_json::Value::from(v))
            .map_err(|e| ClientError::InternalError { msg: e.to_string() }),
        FunctionResult::ConvexError(e) => Err(ClientError::ConvexError {
            data: serde_json::ser::to_string(&serde_json::Value::from(e.data)).unwrap(),
        }),
        FunctionResult::ErrorMessage(msg) => Err(ClientError::ServerError { msg }),
    }
}
