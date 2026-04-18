//! Semantic-type detection scaffold.
//!
//! Currently implements an email detector using a simple regex.
//! Additional detectors can be added by extending [`SemanticDetector`].

use regex::Regex;

/// Threshold: if at least this fraction of sampled values match, assign the
/// semantic type.
const DETECTION_THRESHOLD: f64 = 0.9;

/// Detector that classifies string fields into semantic types.
pub struct SemanticDetector {
    email_re: Regex,
}

impl SemanticDetector {
    /// Create a new detector.
    pub fn new() -> Self {
        Self {
            email_re: Regex::new(r"(?i)^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$").unwrap(),
        }
    }

    /// Given a slice of sampled string values, return a semantic type name if
    /// one can be detected with sufficient confidence, or `None`.
    pub fn detect(&self, values: &[&str]) -> Option<String> {
        if values.is_empty() {
            return None;
        }
        let matches = values.iter().filter(|s| self.email_re.is_match(s)).count();
        if matches as f64 / values.len() as f64 >= DETECTION_THRESHOLD {
            Some("Email".to_owned())
        } else {
            None
        }
    }
}

impl Default for SemanticDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_email_field() {
        let det = SemanticDetector::new();
        let values = vec!["alice@example.com", "bob@test.org", "carol@mail.net"];
        assert_eq!(det.detect(&values), Some("Email".to_owned()));
    }

    #[test]
    fn does_not_detect_non_email() {
        let det = SemanticDetector::new();
        let values = vec!["hello", "world", "foo"];
        assert_eq!(det.detect(&values), None);
    }

    #[test]
    fn below_threshold_returns_none() {
        let det = SemanticDetector::new();
        // Only 1/3 are emails – below 90 %
        let values = vec!["alice@example.com", "not-an-email", "also-not"];
        assert_eq!(det.detect(&values), None);
    }
}
