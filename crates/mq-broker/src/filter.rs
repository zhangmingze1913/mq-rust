//! Minimal RocketMQ SQL92 property filter.
//!
//! Supported: comparison operators, `AND`/`OR`/`NOT`, parentheses, `IS [NOT]
//! NULL`, `[NOT] IN (...)`, `[NOT] BETWEEN ... AND ...`, string/number/bool
//! literals. Property values are compared numerically when both sides parse as
//! numbers, otherwise as strings.

use std::collections::HashMap;

pub fn matches_sql92(properties: &str, expr: &str) -> bool {
    let props = parse_properties(properties);
    let Ok(tokens) = tokenize(expr) else {
        return false;
    };
    let mut parser = Parser {
        tokens,
        pos: 0,
        props: &props,
    };
    parser.parse_or().unwrap_or(false)
}

fn parse_properties(properties: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in properties.split('\u{2}') {
        let mut it = pair.splitn(2, '\u{1}');
        if let (Some(k), Some(v)) = (it.next(), it.next()) {
            if !k.is_empty() {
                map.insert(k.to_string(), v.to_string());
            }
        }
    }
    map
}

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Ident(String),
    Str(String),
    Num(f64),
    Op(String),
    LParen,
    RParen,
    Comma,
    And,
    Or,
    Not,
    Is,
    Null,
    In,
    Between,
    True,
    False,
}

fn tokenize(input: &str) -> Result<Vec<Tok>, ()> {
    let chars: Vec<char> = input.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        match c {
            '(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            ',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            '\'' => {
                i += 1;
                let mut s = String::new();
                while i < chars.len() {
                    if chars[i] == '\'' {
                        if i + 1 < chars.len() && chars[i + 1] == '\'' {
                            s.push('\'');
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    s.push(chars[i]);
                    i += 1;
                }
                if i >= chars.len() {
                    return Err(());
                }
                i += 1;
                out.push(Tok::Str(s));
            }
            '=' | '>' | '<' | '!' => {
                let mut op = String::from(c);
                i += 1;
                if i < chars.len() && (chars[i] == '=' || (c == '<' && chars[i] == '>')) {
                    op.push(chars[i]);
                    i += 1;
                }
                out.push(Tok::Op(op));
            }
            '-' | '0'..='9' => {
                let start = i;
                i += 1;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let s: String = chars[start..i].iter().collect();
                out.push(Tok::Num(s.parse().map_err(|_| ())?));
            }
            _ if c.is_alphabetic() || c == '_' => {
                let start = i;
                i += 1;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                out.push(match word.to_ascii_uppercase().as_str() {
                    "AND" => Tok::And,
                    "OR" => Tok::Or,
                    "NOT" => Tok::Not,
                    "IS" => Tok::Is,
                    "NULL" => Tok::Null,
                    "IN" => Tok::In,
                    "BETWEEN" => Tok::Between,
                    "TRUE" => Tok::True,
                    "FALSE" => Tok::False,
                    _ => Tok::Ident(word),
                });
            }
            _ => return Err(()),
        }
    }
    Ok(out)
}

struct Parser<'a> {
    tokens: Vec<Tok>,
    pos: usize,
    props: &'a HashMap<String, String>,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.pos)
    }

    fn eat(&mut self, t: &Tok) -> bool {
        if self.peek() == Some(t) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn parse_or(&mut self) -> Result<bool, ()> {
        let mut value = self.parse_and()?;
        while self.eat(&Tok::Or) {
            let rhs = self.parse_and()?;
            value = value || rhs;
        }
        Ok(value)
    }

    fn parse_and(&mut self) -> Result<bool, ()> {
        let mut value = self.parse_not()?;
        while self.eat(&Tok::And) {
            let rhs = self.parse_not()?;
            value = value && rhs;
        }
        Ok(value)
    }

    fn parse_not(&mut self) -> Result<bool, ()> {
        if self.eat(&Tok::Not) {
            return Ok(!self.parse_not()?);
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<bool, ()> {
        if self.eat(&Tok::LParen) {
            let v = self.parse_or()?;
            if !self.eat(&Tok::RParen) {
                return Err(());
            }
            return Ok(v);
        }
        let left = self.parse_operand()?;
        if self.eat(&Tok::Is) {
            let negated = self.eat(&Tok::Not);
            if !self.eat(&Tok::Null) {
                return Err(());
            }
            let is_null = left.is_none();
            return Ok(if negated { !is_null } else { is_null });
        }
        let negated = self.eat(&Tok::Not);
        if self.eat(&Tok::In) {
            if !self.eat(&Tok::LParen) {
                return Err(());
            }
            let mut found = false;
            loop {
                let item = self.parse_operand()?;
                if compare_eq(left.as_ref(), item.as_ref()) {
                    found = true;
                }
                if self.eat(&Tok::Comma) {
                    continue;
                }
                if self.eat(&Tok::RParen) {
                    break;
                }
                return Err(());
            }
            return Ok(if negated { !found } else { found });
        }
        if self.eat(&Tok::Between) {
            let low = self.parse_operand()?;
            if !self.eat(&Tok::And) {
                return Err(());
            }
            let high = self.parse_operand()?;
            let within = compare_between(left.as_ref(), low.as_ref(), high.as_ref());
            return Ok(if negated { !within } else { within });
        }
        if negated {
            return Err(());
        }
        let Some(Tok::Op(op)) = self.peek().cloned() else {
            return Err(());
        };
        self.pos += 1;
        let right = self.parse_operand()?;
        Ok(compare_op(left.as_ref(), right.as_ref(), &op))
    }

    fn parse_operand(&mut self) -> Result<Option<String>, ()> {
        let tok = self.tokens.get(self.pos).cloned().ok_or(())?;
        self.pos += 1;
        match tok {
            Tok::Str(s) => Ok(Some(s)),
            Tok::Num(n) => Ok(Some(n.to_string())),
            Tok::True => Ok(Some("true".to_string())),
            Tok::False => Ok(Some("false".to_string())),
            Tok::Null => Ok(None),
            Tok::Ident(name) => Ok(self.props.get(&name).cloned()),
            _ => Err(()),
        }
    }
}

fn compare_eq(a: Option<&String>, b: Option<&String>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => {
            if let (Ok(x), Ok(y)) = (a.parse::<f64>(), b.parse::<f64>()) {
                (x - y).abs() < f64::EPSILON
            } else {
                a == b
            }
        }
        (None, None) => true,
        _ => false,
    }
}

fn compare_op(a: Option<&String>, b: Option<&String>, op: &str) -> bool {
    let (Some(a), Some(b)) = (a, b) else {
        return false;
    };
    if let (Ok(x), Ok(y)) = (a.parse::<f64>(), b.parse::<f64>()) {
        return match op {
            "=" => (x - y).abs() < f64::EPSILON,
            "<>" | "!=" => (x - y).abs() >= f64::EPSILON,
            ">" => x > y,
            ">=" => x >= y,
            "<" => x < y,
            "<=" => x <= y,
            _ => false,
        };
    }
    match op {
        "=" => a == b,
        "<>" | "!=" => a != b,
        ">" => a > b,
        ">=" => a >= b,
        "<" => a < b,
        "<=" => a <= b,
        _ => false,
    }
}

fn compare_between(a: Option<&String>, low: Option<&String>, high: Option<&String>) -> bool {
    let (Some(a), Some(low), Some(high)) = (a, low, high) else {
        return false;
    };
    if let (Ok(x), Ok(l), Ok(h)) = (a.parse::<f64>(), low.parse::<f64>(), high.parse::<f64>()) {
        return x >= l && x <= h;
    }
    a >= low && a <= high
}

#[cfg(test)]
mod tests {
    use super::matches_sql92;

    fn props() -> &'static str {
        "price\u{1}150\u{2}region\u{1}CN\u{2}status\u{1}PAID\u{2}qty\u{1}3"
    }

    #[test]
    fn numeric_comparison() {
        assert!(matches_sql92(props(), "price > 100"));
        assert!(!matches_sql92(props(), "price < 100"));
        assert!(matches_sql92(props(), "price BETWEEN 100 AND 200"));
        assert!(matches_sql92(props(), "qty IN (1, 3, 5)"));
    }

    #[test]
    fn string_and_boolean_logic() {
        assert!(matches_sql92(props(), "region = 'CN' AND status = 'PAID'"));
        assert!(matches_sql92(props(), "region = 'US' OR status = 'PAID'"));
        assert!(!matches_sql92(props(), "NOT (region = 'CN')"));
        assert!(matches_sql92(
            props(),
            "(region = 'CN' OR region = 'US') AND price >= 150"
        ));
    }

    #[test]
    fn null_and_not_in() {
        assert!(matches_sql92(props(), "missing IS NULL"));
        assert!(matches_sql92(props(), "region IS NOT NULL"));
        assert!(matches_sql92(props(), "qty NOT IN (1, 2)"));
        assert!(!matches_sql92(props(), "region NOT IN ('CN', 'US')"));
    }
}
