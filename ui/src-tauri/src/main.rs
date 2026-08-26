fn main() {
    if let Err(error) = insidertrader_desktop_lib::run() {
        eprintln!("InsiderTrader desktop: {error}");
        std::process::exit(1);
    }
}
