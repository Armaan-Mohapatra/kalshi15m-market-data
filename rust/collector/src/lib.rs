//! Read-only, isolated Kalshi 15-minute capture engine.
//!
//! Public network polling requires two explicit CLI safeguards; normal tests
//! never open network sockets. No trading endpoints or order submission exist.
pub mod api;
pub mod archive;
pub mod book;
pub mod discovery;
pub mod event;
pub mod replay;
pub mod runtime;
pub mod storage;
pub mod websocket;
