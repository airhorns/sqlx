mod socket;
pub mod tls;

pub use socket::{
    connect_tcp, connect_uds, connect_with, BufferedSocket, Socket, SocketFactory, SocketIntoBox,
    WithSocket, WriteBuffer,
};
