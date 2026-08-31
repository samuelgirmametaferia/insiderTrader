use std::collections::VecDeque;

const MAX_COMMAND_BYTES: usize = 16_384;
const MAX_HISTORY_ENTRIES: usize = 128;
const MAX_HISTORY_BYTES: usize = 262_144;

const FUNCTIONS: &[&str] = &[
    "ACK",
    "AGG",
    "ALERTS",
    "ANALYZE",
    "ATTRIB",
    "AUTO",
    "BACK",
    "BACKTESTS",
    "BUY",
    "CANCEL",
    "CHART",
    "CHARTRESET",
    "CHARTSTYLE",
    "CONFIG",
    "CONFIRM",
    "CROSSHAIR",
    "DEPTH",
    "DETAIL",
    "EXPERIMENTS",
    "HALT",
    "HEALTH",
    "HELP",
    "HOME",
    "INTERVAL",
    "MARKET",
    "METRICS",
    "METRICSET",
    "MODE",
    "MODELS",
    "NEWS",
    "NEWSNEXT",
    "NEWSPREV",
    "ORDER",
    "ORDERS",
    "OVERLAY",
    "PAN",
    "PORT",
    "PREVIEW",
    "QUIT",
    "REFRESH",
    "RISK",
    "RISKSTATE",
    "SCREEN",
    "SEARCH",
    "SELL",
    "STRAT",
    "STRATSET",
    "STYLE",
    "TAPE",
    "TCA",
    "THEME",
    "TF",
    "TIMEFRAME",
    "TRACE",
    "TRADINGVIEW",
    "TV",
    "XHAIR",
    "ZOOM",
];

pub(crate) fn is_function(value: &str) -> bool {
    FUNCTIONS
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(value))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Completion {
    Applied(String),
    Ambiguous(Vec<&'static str>),
    None,
}

#[derive(Debug, Default)]
pub(crate) struct CommandLine {
    text: String,
    cursor: usize,
    history: VecDeque<String>,
    history_bytes: usize,
    history_index: Option<usize>,
    history_draft: String,
}

impl CommandLine {
    pub(crate) fn text(&self) -> &str {
        &self.text
    }

    pub(crate) const fn cursor(&self) -> usize {
        self.cursor
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.text.is_empty()
    }

    pub(crate) fn set(&mut self, value: &str) -> Result<(), String> {
        if value.len() > MAX_COMMAND_BYTES {
            return Err(format!(
                "interactive command exceeds {MAX_COMMAND_BYTES}-byte bound"
            ));
        }
        value.clone_into(&mut self.text);
        self.cursor = self.text.len();
        self.reset_history_navigation();
        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        self.text.clear();
        self.cursor = 0;
        self.reset_history_navigation();
    }

    pub(crate) fn insert(&mut self, character: char) -> Result<(), String> {
        if character.is_control() {
            return Err("control characters are not accepted in terminal commands".into());
        }
        if self.text.len().saturating_add(character.len_utf8()) > MAX_COMMAND_BYTES {
            return Err(format!(
                "interactive command exceeds {MAX_COMMAND_BYTES}-byte bound"
            ));
        }
        self.detach_history();
        self.text.insert(self.cursor, character);
        self.cursor += character.len_utf8();
        Ok(())
    }

    pub(crate) fn backspace(&mut self) {
        let Some(previous) = self.text[..self.cursor].char_indices().next_back() else {
            return;
        };
        self.detach_history();
        self.text.drain(previous.0..self.cursor);
        self.cursor = previous.0;
    }

    pub(crate) fn delete(&mut self) {
        let Some(character) = self.text[self.cursor..].chars().next() else {
            return;
        };
        self.detach_history();
        self.text
            .drain(self.cursor..self.cursor.saturating_add(character.len_utf8()));
    }

    pub(crate) fn move_left(&mut self) -> bool {
        let Some(previous) = self.text[..self.cursor].char_indices().next_back() else {
            return false;
        };
        self.cursor = previous.0;
        true
    }

    pub(crate) fn move_right(&mut self) -> bool {
        let Some(character) = self.text[self.cursor..].chars().next() else {
            return false;
        };
        self.cursor = self.cursor.saturating_add(character.len_utf8());
        true
    }

    pub(crate) fn move_home(&mut self) {
        self.cursor = 0;
    }

    pub(crate) fn move_end(&mut self) {
        self.cursor = self.text.len();
    }

    pub(crate) fn previous_history(&mut self) -> bool {
        if self.history.is_empty() {
            return false;
        }
        let index = if let Some(index) = self.history_index {
            index.saturating_sub(1)
        } else {
            self.text.clone_into(&mut self.history_draft);
            self.history.len().saturating_sub(1)
        };
        self.history_index = Some(index);
        if let Some(value) = self.history.get(index) {
            value.clone_into(&mut self.text);
            self.cursor = self.text.len();
        }
        true
    }

    pub(crate) fn next_history(&mut self) -> bool {
        let Some(index) = self.history_index else {
            return false;
        };
        if index.saturating_add(1) < self.history.len() {
            let next = index + 1;
            self.history_index = Some(next);
            if let Some(value) = self.history.get(next) {
                value.clone_into(&mut self.text);
            }
        } else {
            self.history_index = None;
            self.history_draft.clone_into(&mut self.text);
            self.history_draft.clear();
        }
        self.cursor = self.text.len();
        true
    }

    pub(crate) fn submit(&mut self) -> Option<String> {
        let command = self.text.trim().to_owned();
        self.clear();
        if command.is_empty() {
            return None;
        }
        if self.history.back() != Some(&command) {
            while self.history.len() >= MAX_HISTORY_ENTRIES
                || self.history_bytes.saturating_add(command.len()) > MAX_HISTORY_BYTES
            {
                let Some(removed) = self.history.pop_front() else {
                    break;
                };
                self.history_bytes = self.history_bytes.saturating_sub(removed.len());
            }
            self.history_bytes = self.history_bytes.saturating_add(command.len());
            self.history.push_back(command.clone());
        }
        Some(command)
    }

    pub(crate) fn complete(&mut self) -> Completion {
        let before = &self.text[..self.cursor];
        let token_start = before
            .char_indices()
            .rev()
            .find_map(|(index, character)| character.is_whitespace().then_some(index + 1))
            .unwrap_or(0);
        let prefix = &self.text[token_start..self.cursor];
        let token_index = before[..token_start].split_whitespace().count();
        let first = self.text.split_whitespace().next().unwrap_or_default();
        let matches = candidates(first, token_index)
            .iter()
            .copied()
            .filter(|candidate| starts_with_ignore_ascii_case(candidate, prefix))
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return Completion::None;
        }
        let common = common_prefix(&matches);
        if common.len() > prefix.len() {
            self.replace_completion_token(token_start, &common);
        }
        if matches.len() == 1 {
            let value = matches[0];
            self.replace_completion_token(token_start, value);
            if self.cursor == self.text.len() && self.text.len() < MAX_COMMAND_BYTES {
                self.text.push(' ');
                self.cursor += 1;
            }
            Completion::Applied(value.into())
        } else {
            Completion::Ambiguous(matches)
        }
    }

    fn replace_completion_token(&mut self, start: usize, value: &str) {
        self.detach_history();
        self.text.replace_range(start..self.cursor, value);
        self.cursor = start + value.len();
    }

    fn detach_history(&mut self) {
        self.history_index = None;
        self.history_draft.clear();
    }

    fn reset_history_navigation(&mut self) {
        self.history_index = None;
        self.history_draft.clear();
    }
}

fn candidates(function: &str, token_index: usize) -> &'static [&'static str] {
    if token_index == 0 {
        return FUNCTIONS;
    }
    match (function.to_ascii_uppercase().as_str(), token_index) {
        ("SCREEN", 1) => &[
            "ALL", "MOVERS", "GAINERS", "LOSERS", "VOLUME", "SPREAD", "STALE",
        ],
        ("ZOOM", 1) => &["30", "60", "120", "240", "480", "960"],
        ("INTERVAL" | "TIMEFRAME" | "TF" | "AGG", 1) => &["1", "5", "15", "30", "60"],
        ("STYLE" | "CHARTSTYLE", 1) => &["CANDLE", "OHLC", "LINE"],
        ("OVERLAY", 1) => &["SMA20", "SMA50", "VWAP", "CLEAR", "DEFAULT"],
        ("OVERLAY", 2) => &["ON", "OFF", "TOGGLE"],
        ("XHAIR" | "CROSSHAIR", 1) => &["OLDER", "NEWER", "LATEST", "OFF"],
        ("PAN", 1) => &["OLDER", "NEWER"],
        ("NEWS", 1) => &["RELEVANT", "ALL", "SORT"],
        ("MODE", 1) => &["MANUAL", "HYBRID", "AUTO"],
        ("THEME", 1) => &["AMBER", "BLUE", "GREEN", "MONO"],
        ("RISKSTATE", 1) => &["RUNNING", "REDUCE", "CANCEL", "HALTED"],
        ("CONFIG", 1) => &["SHOW", "LOAD", "PROMPT"],
        ("STRATSET" | "METRICSET", 2) => &[
            "RESEARCH",
            "VALIDATED",
            "SHADOW",
            "CANARY",
            "PRODUCTION",
            "PAUSED",
            "RETIRED",
        ],
        ("BUY" | "SELL", 3) | ("ORDER", 4) => &["MKT", "LMT"],
        ("ORDER", 1) => &["BUY", "SELL"],
        ("PREVIEW", 2) => &["1.0", "0.75", "0.50", "0.25"],
        _ => &[],
    }
}

fn starts_with_ignore_ascii_case(value: &str, prefix: &str) -> bool {
    value
        .get(..prefix.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
}

fn common_prefix(values: &[&str]) -> String {
    let Some(first) = values.first() else {
        return String::new();
    };
    let mut length = first.len();
    for value in &values[1..] {
        length = first
            .bytes()
            .zip(value.bytes())
            .take(length)
            .take_while(|(left, right)| left.eq_ignore_ascii_case(right))
            .count();
    }
    first[..length].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edits_at_utf8_boundaries_and_enforces_the_byte_bound() {
        let mut line = CommandLine::default();
        line.insert('A').unwrap_or_default();
        line.insert('é').unwrap_or_default();
        line.insert('B').unwrap_or_default();
        assert_eq!(line.text(), "AéB");
        assert!(line.move_left());
        line.backspace();
        assert_eq!(line.text(), "AB");
        line.delete();
        assert_eq!(line.text(), "A");

        let oversized = "x".repeat(MAX_COMMAND_BYTES + 1);
        assert!(line.set(&oversized).is_err());
        assert!(line.insert('\u{1b}').is_err());
    }

    #[test]
    fn history_is_bounded_deduplicated_and_restores_the_draft() {
        let mut line = CommandLine::default();
        for index in 0..=MAX_HISTORY_ENTRIES {
            line.set(&format!("NEWS {index}")).unwrap_or_default();
            let _ = line.submit();
        }
        line.set("draft").unwrap_or_default();
        assert!(line.previous_history());
        assert_eq!(line.text(), format!("NEWS {MAX_HISTORY_ENTRIES}"));
        assert!(line.next_history());
        assert_eq!(line.text(), "draft");
        assert_eq!(line.history.len(), MAX_HISTORY_ENTRIES);

        line.set("NEWS 7").unwrap_or_default();
        let _ = line.submit();
        let entries = line.history.len();
        line.set("NEWS 7").unwrap_or_default();
        let _ = line.submit();
        assert_eq!(line.history.len(), entries);
        assert_eq!(line.history.back().map(String::as_str), Some("NEWS 7"));
    }

    #[test]
    fn history_enforces_an_aggregate_byte_bound() {
        let mut line = CommandLine::default();
        for index in 0..32 {
            let command = format!("ANALYZE {index} {}", "x".repeat(16_000));
            line.set(&command).unwrap_or_default();
            let _ = line.submit();
        }
        assert!(line.history_bytes <= MAX_HISTORY_BYTES);
        assert!(line.history.len() < 32);
    }

    #[test]
    fn completion_handles_functions_and_contextual_arguments() {
        let mut line = CommandLine::default();
        line.set("scr").unwrap_or_default();
        assert_eq!(line.complete(), Completion::Applied("SCREEN".into()));
        assert_eq!(line.text(), "SCREEN ");
        line.insert('g').unwrap_or_default();
        assert_eq!(line.complete(), Completion::Applied("GAINERS".into()));
        assert_eq!(line.text(), "SCREEN GAINERS ");

        line.set("STRATSET breakout pro").unwrap_or_default();
        assert_eq!(line.complete(), Completion::Applied("PRODUCTION".into()));
        assert_eq!(line.text(), "STRATSET breakout PRODUCTION ");

        line.set("BUY AAPL 10 l").unwrap_or_default();
        assert_eq!(line.complete(), Completion::Applied("LMT".into()));
        assert_eq!(line.text(), "BUY AAPL 10 LMT ");

        line.set("INTERVAL 1").unwrap_or_default();
        assert!(matches!(line.complete(), Completion::Ambiguous(_)));
        line.set("STYLE o").unwrap_or_default();
        assert_eq!(line.complete(), Completion::Applied("OHLC".into()));
        line.set("OVERLAY SMA20 on").unwrap_or_default();
        assert_eq!(line.complete(), Completion::Applied("ON".into()));
        line.set("XHAIR lat").unwrap_or_default();
        assert_eq!(line.complete(), Completion::Applied("LATEST".into()));

        line.set("new").unwrap_or_default();
        let completion = line.complete();
        assert!(matches!(completion, Completion::Ambiguous(_)));
        if let Completion::Ambiguous(matches) = completion {
            assert!(matches.contains(&"NEWS"));
            assert!(matches.contains(&"NEWSNEXT"));
        }
    }

    #[test]
    fn cursor_motion_preserves_suffix_during_completion() {
        let mut line = CommandLine::default();
        line.set("scr trailing").unwrap_or_default();
        line.move_home();
        assert!(line.move_right());
        assert!(line.move_right());
        assert!(line.move_right());
        assert_eq!(line.complete(), Completion::Applied("SCREEN".into()));
        assert_eq!(line.text(), "SCREEN trailing");
    }

    #[test]
    fn function_registry_contains_every_normative_terminal_function() {
        for function in [
            "HOME", "MARKET", "PORT", "ORDERS", "STRAT", "METRICS", "NEWS", "RISK", "AUTO",
            "ALERTS", "HEALTH", "HELP",
        ] {
            assert!(is_function(function), "missing {function}");
        }
    }
}
