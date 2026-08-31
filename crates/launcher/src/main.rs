//! One-command launcher for the headless runtime and native/local renderers.

#![forbid(unsafe_code)]

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if let Err(error) = insider_launcher::run(&args) {
        eprintln!("insider: {error}");
        std::process::exit(2);
    }
}
