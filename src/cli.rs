use clap::Parser;

#[derive(Parser)]
pub struct Cli {
    /// host to bin the server to
    #[arg(long, default_value = "localhost")]
    pub host: String,
    /// port to bind the server to
    #[arg(long, default_value_t = 80)]
    pub port: u16,
}
