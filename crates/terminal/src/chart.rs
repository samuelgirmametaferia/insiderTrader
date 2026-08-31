//! Bounded, deterministic chart presentation calculations.
//!
//! The runtime snapshot remains authoritative. These helpers only aggregate and
//! annotate the bounded OHLCV window already delivered to the terminal.

use crate::model::BarView;

pub const CHART_WINDOWS: [usize; 6] = [30, 60, 120, 240, 480, 960];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ChartInterval {
    #[default]
    One,
    Five,
    Fifteen,
    Thirty,
    Sixty,
}

impl ChartInterval {
    pub const fn factor(self) -> usize {
        match self {
            Self::One => 1,
            Self::Five => 5,
            Self::Fifteen => 15,
            Self::Thirty => 30,
            Self::Sixty => 60,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::One => "1x",
            Self::Five => "5x",
            Self::Fifteen => "15x",
            Self::Thirty => "30x",
            Self::Sixty => "60x",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_uppercase().trim_end_matches('X') {
            "1" => Some(Self::One),
            "5" => Some(Self::Five),
            "15" => Some(Self::Fifteen),
            "30" => Some(Self::Thirty),
            "60" => Some(Self::Sixty),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ChartStyle {
    #[default]
    Candles,
    Ohlc,
    Line,
}

impl ChartStyle {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Candles => "CANDLE",
            Self::Ohlc => "OHLC",
            Self::Line => "LINE",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_uppercase().as_str() {
            "CANDLE" | "CANDLES" => Some(Self::Candles),
            "OHLC" | "BAR" | "BARS" => Some(Self::Ohlc),
            "LINE" => Some(Self::Line),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Overlay {
    Sma20,
    Sma50,
    Vwap,
}

impl Overlay {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Sma20 => "SMA20",
            Self::Sma50 => "SMA50",
            Self::Vwap => "VWAP",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_uppercase().as_str() {
            "SMA20" | "MA20" => Some(Self::Sma20),
            "SMA50" | "MA50" => Some(Self::Sma50),
            "VWAP" => Some(Self::Vwap),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChartOverlays {
    pub sma20: bool,
    pub sma50: bool,
    pub vwap: bool,
}

impl Default for ChartOverlays {
    fn default() -> Self {
        Self {
            sma20: true,
            sma50: false,
            vwap: true,
        }
    }
}

impl ChartOverlays {
    pub const fn none() -> Self {
        Self {
            sma20: false,
            sma50: false,
            vwap: false,
        }
    }

    pub fn enabled(self, overlay: Overlay) -> bool {
        match overlay {
            Overlay::Sma20 => self.sma20,
            Overlay::Sma50 => self.sma50,
            Overlay::Vwap => self.vwap,
        }
    }

    pub fn set(&mut self, overlay: Overlay, enabled: bool) {
        match overlay {
            Overlay::Sma20 => self.sma20 = enabled,
            Overlay::Sma50 => self.sma50 = enabled,
            Overlay::Vwap => self.vwap = enabled,
        }
    }

    pub fn toggle(&mut self, overlay: Overlay) -> bool {
        let enabled = !self.enabled(overlay);
        self.set(overlay, enabled);
        enabled
    }

    pub fn encode(self) -> String {
        [Overlay::Sma20, Overlay::Sma50, Overlay::Vwap]
            .into_iter()
            .filter(|overlay| self.enabled(*overlay))
            .map(Overlay::name)
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn decode(value: &str) -> Option<Self> {
        if value == "NONE" || value.is_empty() {
            return Some(Self::none());
        }
        let mut overlays = Self::none();
        for raw in value.split(',') {
            let overlay = Overlay::parse(raw)?;
            if overlays.enabled(overlay) {
                return None;
            }
            overlays.set(overlay, true);
        }
        Some(overlays)
    }

    pub fn legend(self) -> String {
        let encoded = self.encode();
        if encoded.is_empty() {
            "NONE".into()
        } else {
            encoded
        }
    }
}

#[derive(Clone, Debug)]
pub struct RenderBar {
    pub bar: BarView,
    /// Inclusive index in the interval-aggregated input.
    pub source_start: usize,
    /// Exclusive index in the interval-aggregated input.
    pub source_end: usize,
}

/// Aggregate source bars into right-anchored fixed-size buckets.
///
/// Right anchoring ensures the newest bucket is always a complete interval when
/// enough source bars are available. A leading partial bucket is retained rather
/// than silently discarded.
pub fn aggregate_interval(bars: &[BarView], interval: ChartInterval) -> Vec<BarView> {
    let factor = interval.factor();
    if bars.is_empty() || factor == 1 {
        return bars.to_vec();
    }
    let leading = bars.len() % factor;
    let capacity = bars.len().div_ceil(factor);
    let mut output = Vec::with_capacity(capacity);
    let mut start = 0;
    if leading != 0 {
        if let Some(bar) = aggregate_slice(&bars[..leading]) {
            output.push(bar);
        }
        start = leading;
    }
    for values in bars[start..].chunks(factor) {
        if let Some(bar) = aggregate_slice(values) {
            output.push(bar);
        }
    }
    output
}

/// Compress interval bars to a bounded render width without overlapping buckets.
pub fn compress_for_width(bars: &[BarView], maximum: usize) -> Vec<RenderBar> {
    if maximum == 0 || bars.is_empty() {
        return Vec::new();
    }
    let count = bars.len().min(maximum);
    let mut output = Vec::with_capacity(count);
    for bucket in 0..count {
        let start = bucket.saturating_mul(bars.len()) / count;
        let end = (bucket.saturating_add(1)).saturating_mul(bars.len()) / count;
        let end = end.min(bars.len());
        if let Some(bar) = aggregate_slice(&bars[start..end]) {
            output.push(RenderBar {
                bar,
                source_start: start,
                source_end: end,
            });
        }
    }
    output
}

pub fn simple_moving_average(bars: &[BarView], period: usize) -> Vec<Option<i64>> {
    if period == 0 {
        return vec![None; bars.len()];
    }
    let mut output = Vec::with_capacity(bars.len());
    let mut sum = 0_i128;
    for (index, bar) in bars.iter().enumerate() {
        sum = sum.saturating_add(i128::from(bar.close));
        if index >= period {
            sum = sum.saturating_sub(i128::from(bars[index - period].close));
        }
        if index + 1 >= period {
            output.push(i64::try_from(sum / i128::try_from(period).unwrap_or(1)).ok());
        } else {
            output.push(None);
        }
    }
    output
}

/// Visible-window cumulative VWAP. The V14 chart snapshot has no session marker,
/// so this deliberately does not claim to be exchange-session VWAP.
pub fn window_vwap(bars: &[BarView]) -> Vec<Option<i64>> {
    let mut output = Vec::with_capacity(bars.len());
    let mut value_volume = 0_i128;
    let mut volume = 0_i128;
    for bar in bars {
        let bar_volume = i128::from(bar.volume.max(0));
        value_volume =
            value_volume.saturating_add(i128::from(bar.close).saturating_mul(bar_volume));
        volume = volume.saturating_add(bar_volume);
        output.push(if volume == 0 {
            None
        } else {
            i64::try_from(value_volume / volume).ok()
        });
    }
    output
}

pub fn cursor_index(length: usize, from_latest: Option<usize>) -> Option<usize> {
    let from_latest = from_latest?;
    length.checked_sub(from_latest.saturating_add(1))
}

pub fn zoom_window(current: usize, inward: bool) -> usize {
    let index = CHART_WINDOWS
        .iter()
        .position(|candidate| *candidate == current)
        .unwrap_or(2);
    if inward {
        CHART_WINDOWS[index.saturating_sub(1)]
    } else {
        CHART_WINDOWS[(index + 1).min(CHART_WINDOWS.len() - 1)]
    }
}

fn aggregate_slice(values: &[BarView]) -> Option<BarView> {
    let first = values.first()?;
    let last = values.last()?;
    Some(BarView {
        start_time_ns: first.start_time_ns,
        interval_ns: values
            .iter()
            .fold(0_u64, |sum, bar| sum.saturating_add(bar.interval_ns)),
        open: first.open,
        high: values
            .iter()
            .map(|bar| bar.high)
            .max()
            .unwrap_or(first.high),
        low: values.iter().map(|bar| bar.low).min().unwrap_or(first.low),
        close: last.close,
        volume: values
            .iter()
            .fold(0_i64, |sum, bar| sum.saturating_add(bar.volume)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_aggregation_is_right_anchored_and_preserves_ohlcv() {
        let bars = (1..=7)
            .map(|value| bar(value, value + 2, value - 1, value + 1, value * 10))
            .collect::<Vec<_>>();
        let output = aggregate_interval(&bars, ChartInterval::Five);
        assert_eq!(output.len(), 2);
        assert_eq!(output[0].open, 1);
        assert_eq!(output[0].close, 3);
        assert_eq!(output[0].volume, 30);
        assert_eq!(output[1].open, 3);
        assert_eq!(output[1].high, 9);
        assert_eq!(output[1].low, 2);
        assert_eq!(output[1].close, 8);
        assert_eq!(output[1].volume, 250);
    }

    #[test]
    fn render_compression_uses_complete_non_overlapping_ranges() {
        let bars = (0..5)
            .map(|value| bar(value, value, value, value, 1))
            .collect::<Vec<_>>();
        let output = compress_for_width(&bars, 3);
        assert_eq!(output.len(), 3);
        assert_eq!(output[0].source_start, 0);
        assert_eq!(output[0].source_end, 1);
        assert_eq!(output[1].source_start, 1);
        assert_eq!(output[1].source_end, 3);
        assert_eq!(output[2].source_start, 3);
        assert_eq!(output[2].source_end, 5);
        assert_eq!(output.iter().map(|value| value.bar.volume).sum::<i64>(), 5);
    }

    #[test]
    fn indicators_are_bounded_and_do_not_invent_missing_history() {
        let bars = [
            bar(10, 10, 10, 10, 0),
            bar(20, 20, 20, 20, 2),
            bar(30, 30, 30, 30, 1),
        ];
        assert_eq!(simple_moving_average(&bars, 2), [None, Some(15), Some(25)]);
        assert_eq!(window_vwap(&bars), [None, Some(20), Some(23)]);
    }

    #[test]
    fn preferences_and_cursor_helpers_fail_closed() {
        assert_eq!(ChartInterval::parse("15x"), Some(ChartInterval::Fifteen));
        assert_eq!(ChartInterval::parse("2"), None);
        assert_eq!(ChartStyle::parse("bars"), Some(ChartStyle::Ohlc));
        assert_eq!(ChartStyle::parse("area"), None);
        assert_eq!(
            ChartOverlays::decode("SMA20,VWAP"),
            Some(ChartOverlays::default())
        );
        assert_eq!(ChartOverlays::decode("SMA20,SMA20"), None);
        assert_eq!(cursor_index(4, Some(0)), Some(3));
        assert_eq!(cursor_index(4, Some(4)), None);
        assert_eq!(zoom_window(120, true), 60);
        assert_eq!(zoom_window(120, false), 240);
    }

    fn bar(open: i64, high: i64, low: i64, close: i64, volume: i64) -> BarView {
        BarView {
            start_time_ns: open.saturating_mul(60_000_000_000),
            interval_ns: 60_000_000_000,
            open,
            high,
            low,
            close,
            volume,
        }
    }
}
