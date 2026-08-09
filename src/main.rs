use std::io::Cursor;
use std::sync::{Arc, RwLock, mpsc};
use std::thread;
use std::time::Duration;

use async_signal::{Signal, Signals};
use clap::Parser;
use image::{ImageBuffer, ImageFormat, ImageReader, Rgb, RgbImage};
use poem::endpoint::StaticFileEndpoint;
use poem::http::StatusCode;
use poem::listener::TcpListener;
use poem::middleware::Tracing;
use poem::web::sse::{Event, SSE};
use poem::web::{Data, Json};
use poem::{EndpointExt, IntoResponse, Response, Route, Server, handler};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

use crate::cli::Cli;

mod cli;

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct ServerState {
    canvas: Arc<RwLock<RgbImage>>,
    canvas_size: (u32, u32),
    queue: mpsc::Sender<Option<Pixel>>,
    updated_pixels: broadcast::WeakSender<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct Pixel {
    x: u32,
    y: u32,
    r: u8,
    g: u8,
    b: u8,
}

impl Pixel {
    const fn color(&self) -> Rgb<u8> {
        Rgb([self.r, self.g, self.b])
    }
}

#[expect(clippy::needless_pass_by_value, clippy::significant_drop_tightening)]
fn process_queue(
    canvas: Arc<RwLock<RgbImage>>,
    queue_rx: mpsc::Receiver<Option<Pixel>>,
    updated_pixels_tx: broadcast::Sender<String>,
) {
    while let Ok(Some(mut pixel)) = queue_rx.recv() {
        let mut canvas = canvas.write().unwrap();

        loop {
            let current_color = canvas.get_pixel_mut(pixel.x, pixel.y);
            let new_color = pixel.color();

            if *current_color != new_color {
                *current_color = new_color;

                updated_pixels_tx
                    .send(serde_json::to_string(&pixel).unwrap())
                    .ok();
            }

            let Ok(new_pixel) = queue_rx.try_recv() else {
                break;
            };

            let Some(new_pixel) = new_pixel else {
                return;
            };

            pixel = new_pixel;
        }
    }
}

#[handler]
#[expect(clippy::needless_pass_by_value)]
fn get_image(state: Data<&ServerState>) -> Response {
    let mut buffer = Cursor::new(Vec::new());

    state
        .canvas
        .read()
        .unwrap()
        .write_to(&mut buffer, ImageFormat::Png)
        .unwrap();

    Response::from(buffer.into_inner())
        .set_content_type("image/png")
        .with_header("Cache-Control", "no-store")
        .into_response()
}

#[handler]
#[expect(clippy::needless_pass_by_value)]
fn get_updates(state: Data<&ServerState>) -> SSE {
    let receiver = {
        let sender = state.updated_pixels.upgrade().unwrap();
        sender.subscribe()
    };

    let stream = BroadcastStream::new(receiver).map(|message| Event::message(message.unwrap()));

    SSE::new(stream).keep_alive(CONNECTION_TIMEOUT)
}

#[handler]
#[expect(clippy::needless_pass_by_value)]
fn set_pixel(state: Data<&ServerState>, Json(json): Json<Pixel>) -> Response {
    if json.x >= state.canvas_size.0 || json.y >= state.canvas_size.1 {
        return StatusCode::BAD_REQUEST
            .with_body("pixel outside of drawing area")
            .into_response();
    }

    state.queue.send(Some(json)).unwrap();

    StatusCode::NO_CONTENT.into()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();
    let args = Cli::parse();

    let canvas = {
        let image = ImageReader::open("data/image.png").map_or_else(
            |_| ImageBuffer::new(1920, 1080),
            |mut image_reader| {
                image_reader.set_format(ImageFormat::Png);
                image_reader.decode().unwrap().into_rgb8()
            },
        );

        Arc::new(RwLock::new(image))
    };

    let (queue_tx, queue_rx) = mpsc::channel::<Option<Pixel>>();
    let (updated_pixels_tx, _) = broadcast::channel::<String>(16);
    let updated_pixels_weak = updated_pixels_tx.downgrade();

    let handle = {
        let canvas = canvas.clone();

        thread::spawn(|| process_queue(canvas, queue_rx, updated_pixels_tx))
    };

    let app = Route::new()
        .at("/", StaticFileEndpoint::new("static/index.html"))
        .at("/image", poem::get(get_image))
        .at("/updates", poem::get(get_updates))
        .at("/pixel", poem::post(set_pixel))
        .with(Tracing)
        .data(ServerState {
            canvas: canvas.clone(),
            canvas_size: {
                let canvas = canvas.read().unwrap();
                (canvas.width(), canvas.height())
            },
            queue: queue_tx.clone(),
            updated_pixels: updated_pixels_weak,
        });

    Server::new(TcpListener::bind((args.host, args.port)))
        .idle_timeout(CONNECTION_TIMEOUT)
        .run_with_graceful_shutdown(
            app,
            async {
                let mut signals = Signals::new([Signal::Term, Signal::Int]).unwrap();
                signals.next().await.unwrap().unwrap();
                queue_tx.send(None).unwrap();
            },
            Some(CONNECTION_TIMEOUT),
        )
        .await
        .unwrap();

    handle.join().unwrap();

    canvas
        .read()
        .unwrap()
        .save_with_format("data/image.png", ImageFormat::Png)
        .unwrap();
}
