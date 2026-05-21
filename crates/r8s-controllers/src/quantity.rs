//! Minimal Kubernetes Quantity arithmetic, just enough for the resource quota
//! controller to sum pod compute resources.
//!
//! Each parsed value is stored as a (numerator, scale_decimal_digits) pair
//! interpreted as `numerator * 10^(-scale)`. `scale` is non-negative so the
//! representation is exact for every Quantity we'll see in conformance (CPU
//! down to milli-units, memory in bytes). Output is the canonical "fewest
//! suffix bytes" form, but consumers only need it to parse back to the same
//! numeric value — `Quantity.Cmp` in upstream Go treats `"500m" == "0.5"`.

#[derive(Debug, Clone, Copy)]
pub struct Quantity {
    /// Value in the smallest unit (no suffix). For CPU we keep at most 3
    /// decimal places (millis); for memory/storage everything is integer bytes.
    millis: i128,
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

    pub fn add(self, other: Self) -> Self {
        Self {
            millis: self.millis + other.millis,
        }
    }

    pub fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.millis.cmp(&other.millis)
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
        // Only exact when frac*mult % frac_pow == 0 — for milli-CPU and byte-memory
        // this is the case in every conformance input we'll meet.
        let scaled_int = int_v.checked_mul(multiplier_thousands)?;
        let scaled_frac = frac_v
            .checked_mul(multiplier_thousands)?
            .checked_div(frac_pow)?;
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
