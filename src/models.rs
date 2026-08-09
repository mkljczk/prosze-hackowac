use std::sync::{Arc, RwLock, mpsc};

use clap::Parser;
use image::{Rgb, RgbImage};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

#[derive(Parser)]
pub struct Cli {
    /// host to bin the server to
    #[arg(long, default_value = "localhost")]
    pub host: String,
    /// port to bind the server to
    #[arg(long, default_value_t = 80)]
    pub port: u16,
}

#[derive(Clone)]
pub struct ServerState {
    pub canvas: Arc<RwLock<RgbImage>>,
    pub canvas_size: (u32, u32),
    pub queue: mpsc::Sender<Option<Pixel>>,
    pub updated_pixels: broadcast::WeakSender<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Pixel {
    pub x: u32,
    pub y: u32,
    r: u8,
    g: u8,
    b: u8,
}

impl Pixel {
    pub const fn color(&self) -> Rgb<u8> {
        Rgb([self.r, self.g, self.b])
    }
}
