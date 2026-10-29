//! Resource quantities (`500m`, `64Mi`, `2`, `1e3`) as floating-point base
//! units: cores for cpu, bytes for memory.

/// The value of a quantity string, or `None` when it is not one.
pub fn parse_quantity(s: &str) -> Option<f64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-' || c == '+'))
        .unwrap_or(s.len());
    let (number, suffix) = s.split_at(split);
    let (number, suffix) = match suffix.strip_prefix(['e', 'E']) {
        Some(exp)
            if exp
                .chars()
                .all(|c| c.is_ascii_digit() || c == '-' || c == '+') =>
        {
            let exp: i32 = exp.parse().ok()?;
            (number.parse::<f64>().ok()? * 10f64.powi(exp), "")
        }
        _ => (number.parse::<f64>().ok()?, suffix),
    };
    let scale = match suffix {
        "" => 1.0,
        "n" => 1e-9,
        "u" => 1e-6,
        "m" => 1e-3,
        "k" => 1e3,
        "M" => 1e6,
        "G" => 1e9,
        "T" => 1e12,
        "P" => 1e15,
        "E" => 1e18,
        "Ki" => 1024f64,
        "Mi" => 1024f64.powi(2),
        "Gi" => 1024f64.powi(3),
        "Ti" => 1024f64.powi(4),
        "Pi" => 1024f64.powi(5),
        "Ei" => 1024f64.powi(6),
        _ => return None,
    };
    Some(number * scale)
}

#[cfg(test)]
mod tests {
    use super::parse_quantity;

    #[test]
    fn parses_cpu_and_memory_forms() {
        assert_eq!(parse_quantity("2"), Some(2.0));
        assert_eq!(parse_quantity("1250m"), Some(1.25));
        assert_eq!(parse_quantity("0.5"), Some(0.5));
        assert_eq!(parse_quantity("64Mi"), Some(67108864.0));
        assert_eq!(parse_quantity("1Gi"), Some(1073741824.0));
        assert_eq!(parse_quantity("1G"), Some(1e9));
        assert_eq!(parse_quantity("1e3"), Some(1000.0));
        assert_eq!(parse_quantity("128974848"), Some(128974848.0));
        assert_eq!(parse_quantity("abc"), None);
        assert_eq!(parse_quantity("1X"), None);
        assert_eq!(parse_quantity(""), None);
    }
}
