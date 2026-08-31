use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::app::Page;
use crate::chart::{ChartInterval, ChartOverlays, ChartStyle};

const MAGIC: &str = "IT_TERMINAL_PREFERENCES_V5";
const LEGACY_MAGIC: &str = "IT_TERMINAL_PREFERENCES_V4";
const MAX_BYTES: u64 = 65_536;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Preferences {
    pub page: Page,
    pub selected_instrument: Option<u128>,
    pub selected_symbol: String,
    pub news_scope: String,
    pub chart_window: usize,
    pub chart_interval: ChartInterval,
    pub chart_style: ChartStyle,
    pub chart_overlays: ChartOverlays,
    pub screener_mode: String,
}

impl Default for Preferences {
    fn default() -> Self {
        Self {
            page: Page::Home,
            selected_instrument: None,
            selected_symbol: String::new(),
            news_scope: "relevant".into(),
            chart_window: 120,
            chart_interval: ChartInterval::default(),
            chart_style: ChartStyle::default(),
            chart_overlays: ChartOverlays::default(),
            screener_mode: "MOVERS".into(),
        }
    }
}

impl Preferences {
    pub fn load(path: &Path) -> Result<Self, String> {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => return Err(format!("open terminal preferences: {error}")),
        };
        let mut text = String::new();
        file.take(MAX_BYTES + 1)
            .read_to_string(&mut text)
            .map_err(|error| format!("read terminal preferences: {error}"))?;
        if text.len() as u64 > MAX_BYTES {
            return Err("terminal preferences exceed 64 KiB bound".into());
        }
        Self::decode(&text)
    }

    pub fn save(&self, path: &Path) -> Result<(), String> {
        let parent = path
            .parent()
            .filter(|value| !value.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)
            .map_err(|error| format!("create terminal preference directory: {error}"))?;
        let file_name = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or("terminal preference path needs a UTF-8 file name")?;
        let temporary = parent.join(format!(".{file_name}.tmp-{}", std::process::id()));
        let encoded = self.encode();
        fs::write(&temporary, encoded)
            .map_err(|error| format!("write terminal preferences: {error}"))?;
        fs::rename(&temporary, path)
            .map_err(|error| format!("commit terminal preferences: {error}"))?;
        Ok(())
    }

    fn encode(&self) -> String {
        format!(
            "{MAGIC}\npage={}\ninstrument={}\nsymbol={}\nnews_scope={}\nchart_window={}\nchart_interval={}\nchart_style={}\nchart_overlays={}\nscreener_mode={}\n",
            self.page.mnemonic(),
            self.selected_instrument
                .map_or_else(String::new, |value| value.to_string()),
            self.selected_symbol,
            self.news_scope,
            self.chart_window,
            self.chart_interval.name(),
            self.chart_style.name(),
            self.chart_overlays.encode(),
            self.screener_mode
        )
    }

    #[allow(clippy::too_many_lines)]
    fn decode(text: &str) -> Result<Self, String> {
        let mut lines = text.lines();
        let legacy = match lines.next() {
            Some(MAGIC) => false,
            Some(LEGACY_MAGIC) => true,
            _ => return Err("invalid terminal preference header".into()),
        };
        let mut value = Self::default();
        let mut page_seen = false;
        let mut instrument_seen = false;
        let mut symbol_seen = false;
        let mut news_scope_seen = false;
        let mut chart_window_seen = false;
        let mut chart_interval_seen = false;
        let mut chart_style_seen = false;
        let mut chart_overlays_seen = false;
        let mut screener_mode_seen = false;
        for line in lines {
            let (key, raw) = line
                .split_once('=')
                .ok_or("malformed terminal preference entry")?;
            match key {
                "page" if !page_seen => {
                    value.page =
                        Page::from_mnemonic(raw).ok_or("unknown terminal preference page")?;
                    page_seen = true;
                }
                "instrument" if !instrument_seen => {
                    value.selected_instrument = if raw.is_empty() {
                        None
                    } else {
                        let identity = raw
                            .parse::<u128>()
                            .map_err(|_| "invalid preferred instrument")?;
                        if identity == 0 {
                            return Err("preferred instrument must be positive".into());
                        }
                        Some(identity)
                    };
                    instrument_seen = true;
                }
                "symbol" if !symbol_seen => {
                    if raw.len() > 64
                        || !raw
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || b".-_/".contains(&byte))
                    {
                        return Err("invalid preferred symbol".into());
                    }
                    raw.clone_into(&mut value.selected_symbol);
                    symbol_seen = true;
                }
                "news_scope" if !news_scope_seen => {
                    if raw != "relevant" && raw != "all" {
                        return Err("invalid preferred news scope".into());
                    }
                    raw.clone_into(&mut value.news_scope);
                    news_scope_seen = true;
                }
                "chart_window" if !chart_window_seen => {
                    value.chart_window = raw
                        .parse::<usize>()
                        .map_err(|_| "invalid preferred chart window")?;
                    if ![30, 60, 120, 240, 480, 960].contains(&value.chart_window) {
                        return Err("invalid preferred chart window".into());
                    }
                    chart_window_seen = true;
                }
                "chart_interval" if !chart_interval_seen && !legacy => {
                    value.chart_interval =
                        ChartInterval::parse(raw).ok_or("invalid preferred chart interval")?;
                    chart_interval_seen = true;
                }
                "chart_style" if !chart_style_seen && !legacy => {
                    value.chart_style =
                        ChartStyle::parse(raw).ok_or("invalid preferred chart style")?;
                    chart_style_seen = true;
                }
                "chart_overlays" if !chart_overlays_seen && !legacy => {
                    value.chart_overlays =
                        ChartOverlays::decode(raw).ok_or("invalid preferred chart overlays")?;
                    chart_overlays_seen = true;
                }
                "screener_mode" if !screener_mode_seen => {
                    if ![
                        "ALL", "MOVERS", "GAINERS", "LOSERS", "VOLUME", "SPREAD", "STALE",
                    ]
                    .contains(&raw)
                    {
                        return Err("invalid preferred screener mode".into());
                    }
                    raw.clone_into(&mut value.screener_mode);
                    screener_mode_seen = true;
                }
                "page" | "instrument" | "symbol" | "news_scope" | "chart_window"
                | "chart_interval" | "chart_style" | "chart_overlays" | "screener_mode" => {
                    return Err(format!("duplicate terminal preference key: {key}"));
                }
                _ => return Err(format!("unknown terminal preference key: {key}")),
            }
        }
        if !page_seen
            || !instrument_seen
            || !symbol_seen
            || !news_scope_seen
            || !chart_window_seen
            || (!legacy && (!chart_interval_seen || !chart_style_seen || !chart_overlays_seen))
            || !screener_mode_seen
        {
            return Err("terminal preferences are incomplete".into());
        }
        Ok(value)
    }
}

pub fn default_path() -> Option<PathBuf> {
    if let Some(root) = std::env::var_os("XDG_STATE_HOME").filter(|value| !value.is_empty()) {
        return Some(PathBuf::from(root).join("insidertrader/terminal.state"));
    }
    std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(|root| PathBuf::from(root).join(".local/state/insidertrader/terminal.state"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_contains_only_presentation_state() {
        let value = Preferences {
            page: Page::News,
            selected_instrument: Some(42),
            selected_symbol: "BRK.B".into(),
            news_scope: "all".into(),
            chart_window: 240,
            chart_interval: ChartInterval::Fifteen,
            chart_style: ChartStyle::Line,
            chart_overlays: ChartOverlays {
                sma20: false,
                sma50: true,
                vwap: true,
            },
            screener_mode: "VOLUME".into(),
        };
        assert_eq!(Preferences::decode(&value.encode()), Ok(value));
    }

    #[test]
    fn duplicate_and_unknown_keys_fail_closed() {
        let duplicate = format!(
            "{MAGIC}\npage=HOME\npage=NEWS\ninstrument=\nsymbol=\nnews_scope=relevant\nchart_window=120\nchart_interval=1x\nchart_style=CANDLE\nchart_overlays=SMA20,VWAP\nscreener_mode=MOVERS\n"
        );
        assert!(Preferences::decode(&duplicate).is_err());
        let unknown = format!(
            "{MAGIC}\npage=HOME\ninstrument=\nsymbol=\nnews_scope=relevant\nchart_window=120\nchart_interval=1x\nchart_style=CANDLE\nchart_overlays=SMA20,VWAP\nscreener_mode=MOVERS\naccount=7\n"
        );
        assert!(Preferences::decode(&unknown).is_err());
    }

    #[test]
    fn invalid_identity_and_symbol_are_rejected() {
        let identity = format!(
            "{MAGIC}\npage=HOME\ninstrument=0\nsymbol=\nnews_scope=relevant\nchart_window=120\nchart_interval=1x\nchart_style=CANDLE\nchart_overlays=SMA20,VWAP\nscreener_mode=MOVERS\n"
        );
        assert!(Preferences::decode(&identity).is_err());
        let symbol = format!(
            "{MAGIC}\npage=HOME\ninstrument=\nsymbol=A A\nnews_scope=relevant\nchart_window=120\nchart_interval=1x\nchart_style=CANDLE\nchart_overlays=SMA20,VWAP\nscreener_mode=MOVERS\n"
        );
        assert!(Preferences::decode(&symbol).is_err());
    }

    #[test]
    fn legacy_v4_preferences_migrate_to_validated_chart_defaults() {
        let legacy = format!(
            "{LEGACY_MAGIC}\npage=CHART\ninstrument=42\nsymbol=AAPL\nnews_scope=relevant\nchart_window=240\nscreener_mode=MOVERS\n"
        );
        let value = Preferences::decode(&legacy).unwrap_or_default();
        assert_eq!(value.page, Page::Chart);
        assert_eq!(value.chart_interval, ChartInterval::One);
        assert_eq!(value.chart_style, ChartStyle::Candles);
        assert_eq!(value.chart_overlays, ChartOverlays::default());
    }

    #[test]
    fn malformed_chart_preferences_fail_closed() {
        for entry in [
            "chart_interval=2x\nchart_style=CANDLE\nchart_overlays=SMA20,VWAP",
            "chart_interval=1x\nchart_style=AREA\nchart_overlays=SMA20,VWAP",
            "chart_interval=1x\nchart_style=CANDLE\nchart_overlays=SMA20,SMA20",
        ] {
            let text = format!(
                "{MAGIC}\npage=CHART\ninstrument=\nsymbol=\nnews_scope=relevant\nchart_window=120\n{entry}\nscreener_mode=MOVERS\n"
            );
            assert!(Preferences::decode(&text).is_err());
        }
    }
}
