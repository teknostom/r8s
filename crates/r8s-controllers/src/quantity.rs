//! Minimal Kubernetes Quantity arithmetic, just enough for the resource quota
//! controller to sum pod compute resources.
//!
//! Each parsed value is stored as a (numerator, scale_decimal_digits) pair
//! interpreted as `numerator * 10^(-scale)`. `scale` is non-negative so the
//! representation is exact for every Quantity we'll see in conformance (CPU
//! down to milli-units, memory in bytes). Output is the canonical "fewest
//! suffix bytes" form, but consumers only need it to parse back to the same
//! numeric value — `Quantity.Cmp` in upstream Go treats `"500m" == "0.5"`.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Quantity {
    /// Value in the smallest unit (no suffix). For CPU we keep at most 3
    /// decimal places (millis); for memory/storage everything is integer bytes.
    millis: i128,
}

impl std::ops::Add for Quantity {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            millis: self.millis + other.millis,
        }
    }
}

impl Quantity {
    pub fn zero() -> Self {
        Self { millis: 0 }
    }

    /// `millis = value * 1000`. Storing in thousandths means "500m" CPU is 500
    /// and "1Gi" memory is 1_073_741_824_000.
    pub fn from_millis(millis: i128) -> Self {
        Self { millis }
    }

    pub fn is_zero(&self) -> bool {
        self.millis == 0
    }

    /// Raw value in thousandths of the base unit (milli-CPU, or bytes × 1000).
    pub fn millis(&self) -> i128 {
        self.millis
    }

    /// Parse a Kubernetes Quantity string. Returns None for malformed input.
    /// Supports decimal suffixes (k/M/G/T/P/E), binary (Ki/Mi/Gi/Ti/Pi/Ei),
    /// and the milli suffix (m). Decimal numbers are allowed (e.g. "0.5").
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        // Strip suffix from the right; longest first so "Mi" beats "M".
        let (num_part, multiplier_thousands): (&str, i128) = {
            let suffixes: &[(&str, i128)] = &[
                // Binary: factor in bytes, then * 1000 to convert to "millis".
                ("Ki", 1_024 * 1000),
                ("Mi", 1_024i128.pow(2) * 1000),
                ("Gi", 1_024i128.pow(3) * 1000),
                ("Ti", 1_024i128.pow(4) * 1000),
                ("Pi", 1_024i128.pow(5) * 1000),
                ("Ei", 1_024i128.pow(6) * 1000),
                // Decimal SI:
                ("E", 10i128.pow(18) * 1000),
                ("P", 10i128.pow(15) * 1000),
                ("T", 10i128.pow(12) * 1000),
                ("G", 10i128.pow(9) * 1000),
                ("M", 10i128.pow(6) * 1000),
                ("k", 10i128.pow(3) * 1000),
                // Milli: m → 1 (since base is millis).
                ("m", 1),
            ];
            let mut found: Option<(&str, i128)> = None;
            for (suf, mult) in suffixes {
                if s.ends_with(*suf) {
                    found = Some((&s[..s.len() - suf.len()], *mult));
                    break;
                }
            }
            match found {
                Some(v) => v,
                None => (s, 1000),
            }
        };
        // num_part is a decimal number. Parse as (sign, integer, fraction).
        let (sign, body) = if let Some(rest) = num_part.strip_prefix('-') {
            (-1i128, rest)
        } else {
            (1i128, num_part)
        };
        let (int_str, frac_str) = match body.split_once('.') {
            Some((a, b)) => (a, b),
            None => (body, ""),
        };
        // The fraction must be bare digits: `str::parse` would otherwise accept a
        // sign (e.g. "5.-3" → frac -3), silently flipping the value.
        if !frac_str.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        // A suffix with no mantissa ("Ki", "-", "") is not a quantity.
        if int_str.is_empty() && frac_str.is_empty() {
            return None;
        }
        let int_v: i128 = if int_str.is_empty() {
            0
        } else {
            int_str.parse().ok()?
        };
        // Scale fractional part: 0.5 means we'd lose precision if we just
        // multiplied. Treat fraction as integer / 10^len, then combine.
        let frac_v: i128 = if frac_str.is_empty() {
            0
        } else {
            frac_str.parse().ok()?
        };
        let frac_pow = 10i128.checked_pow(frac_str.len() as u32)?;
        // value (decimal) = sign * (int + frac/frac_pow); then multiply by mult.
        // To keep integer arithmetic: value_in_millis = sign * (int*mult + frac*mult/frac_pow).
        let scaled_int = int_v.checked_mul(multiplier_thousands)?;
        // The fraction can carry finer precision than our milli scale holds (a
        // sub-milli CPU request, or many fraction digits on a binary suffix).
        // Round the remainder AWAY from zero, never toward it: a request counted
        // as less than it is would let a pod outrun the quota charged for it.
        // `frac_numer >= 0` and `frac_pow > 0`, so `sign` below makes "round up
        // in magnitude" the same as "away from zero".
        let frac_numer = frac_v.checked_mul(multiplier_thousands)?;
        let scaled_frac =
            frac_numer / frac_pow + i128::from(frac_numer % frac_pow != 0);
        Some(Self {
            millis: sign * (scaled_int + scaled_frac),
        })
    }

    /// Canonical "no suffix" form: bytes for memory, milli-units for CPU.
    /// Parses back identically. We always emit `"{n}m"` so a quota field that
    /// originally said `"1"` (CPU) round-trips as `"1000m"`, which `Quantity.Cmp`
    /// considers equal.
    pub fn to_canonical_string(&self) -> String {
        format!("{}m", self.millis)
    }
}

#[cfg(test)]
mod tests {
    use super::Quantity;

    fn millis(s: &str) -> i128 {
        Quantity::parse(s).unwrap_or_else(|| panic!("parse failed: {s:?}")).millis
    }

    #[test]
    fn plain_and_milli_cpu() {
        assert_eq!(millis("1"), 1000);
        assert_eq!(millis("500m"), 500);
        assert_eq!(millis("1000m"), 1000);
        assert_eq!(millis("0.5"), 500);
        // "500m" and "0.5" are the same value.
        assert_eq!(Quantity::parse("500m"), Quantity::parse("0.5"));
    }

    #[test]
    fn binary_suffixes() {
        assert_eq!(millis("1Ki"), 1_024 * 1000);
        assert_eq!(millis("1Mi"), 1_024i128.pow(2) * 1000);
        assert_eq!(millis("1Gi"), 1_024i128.pow(3) * 1000);
        assert_eq!(millis("1Ti"), 1_024i128.pow(4) * 1000);
        // "Mi" must win over "M": 1Mi is 1024² bytes, not 10⁶.
        assert_ne!(Quantity::parse("1Mi"), Quantity::parse("1M"));
    }

    #[test]
    fn decimal_suffixes() {
        assert_eq!(millis("1k"), 10i128.pow(3) * 1000);
        assert_eq!(millis("1M"), 10i128.pow(6) * 1000);
        assert_eq!(millis("1G"), 10i128.pow(9) * 1000);
        assert_eq!(millis("1T"), 10i128.pow(12) * 1000);
    }

    #[test]
    fn exact_fraction() {
        // 1.5Gi = 1610612736 bytes, exact at byte scale.
        assert_eq!(millis("1.5Gi"), 1_610_612_736_000);
        assert_eq!(millis("2.5"), 2500);
    }

    #[test]
    fn subscale_fraction_rounds_away_from_zero() {
        // 1.9999Ki = 2047.8976 bytes → must round UP, never down.
        assert_eq!(millis("1.9999Ki"), 2_047_898);
        // 1.5m CPU = 1.5 milli, below our scale → rounds up to 2.
        assert_eq!(millis("1.5m"), 2);
        // A request must never be counted as less than it is.
        assert!(millis("1.9999Ki") >= 2_047_897);
    }

    #[test]
    fn negative_and_ordering() {
        assert_eq!(millis("-500m"), -500);
        assert!(Quantity::parse("500m").unwrap() < Quantity::parse("1").unwrap());
        assert!(Quantity::parse("1Gi").unwrap() > Quantity::parse("1G").unwrap());
    }

    #[test]
    fn addition() {
        let a = Quantity::parse("500m").unwrap();
        let b = Quantity::parse("0.5").unwrap();
        assert_eq!((a + b), Quantity::parse("1").unwrap());
    }

    #[test]
    fn canonical_round_trip() {
        for s in ["1", "500m", "1Gi", "-500m", "0.5", "2.5"] {
            let q = Quantity::parse(s).unwrap();
            assert_eq!(
                Quantity::parse(&q.to_canonical_string()),
                Some(q),
                "round-trip failed for {s:?}"
            );
        }
    }

    #[test]
    fn malformed_is_none() {
        for s in ["", "  ", "abc", "1.2.3", "5.-3", "Ki", "-", "1.2x"] {
            assert_eq!(Quantity::parse(s), None, "expected None for {s:?}");
        }
    }
}
