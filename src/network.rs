use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, ToSocketAddrs, UdpSocket};
use tokio::sync::{mpsc, watch};

use crate::models::Pixel;

pub async fn tcp_listener(
    addr: impl ToSocketAddrs,
    canvas_size: (u32, u32),
    mut stop_rx: watch::Receiver<()>,
    queue_tx: mpsc::UnboundedSender<Pixel>,
) {
    let listener = TcpListener::bind(addr).await.unwrap();

    loop {
        let (mut socket, _) = tokio::select! {
            biased;
            socket = listener.accept() => socket.unwrap(),
            _ = stop_rx.changed() => break,
        };

        let mut stop_rx = stop_rx.clone();
        let queue_tx = queue_tx.clone();

        tokio::spawn(async move {
            let mut data = [0; 11];

            loop {
                if tokio::select! {
                    biased;
                    result = socket.read_exact(&mut data) => result.is_err(),
                    _ = stop_rx.changed() => break,
                } {
                    break;
                }

                let pixel = Pixel::from_bytes(&data);

                if pixel.x >= canvas_size.0 || pixel.y >= canvas_size.1 {
                    continue;
                }

                queue_tx.send(pixel).ok();
            }
        });
    }
}

pub async fn udp_listener(
    addr: impl ToSocketAddrs,
    canvas_size: (u32, u32),
    mut stop_rx: watch::Receiver<()>,
    queue_tx: mpsc::UnboundedSender<Pixel>,
) {
    let socket = UdpSocket::bind(addr).await.unwrap();
    #[expect(clippy::large_stack_arrays)]
    // max UDP packet size excluding the header
    let mut data = [0; u16::MAX as usize - 8];

    while let Some(len) = tokio::select! {
        biased;
        len = socket.recv(&mut data) => len.ok(),
        _ = stop_rx.changed() => None,
    } {
        for pixel in data[..len].as_chunks().0.iter().map(Pixel::from_bytes) {
            if pixel.x >= canvas_size.0 || pixel.y >= canvas_size.1 {
                continue;
            }

            queue_tx.send(pixel).ok();
        }
    }
}
