use proc_macro2::TokenStream;
use quote::{ToTokens, TokenStreamExt, quote};
use std::{borrow::Cow, cmp::min, collections::BTreeMap, ffi::CString, fmt::Write};
use syn::Path;

#[derive(Clone)]
/// Polymorphic iterator adapter returning items from either variant.
pub enum EitherIterator<A, B>
where
    A: Iterator,
    B: Iterator<Item = A::Item>,
{
    Left(A),
    Right(B),
}
impl<A, B> Iterator for EitherIterator<A, B>
where
    A: Iterator,
    B: Iterator<Item = A::Item>,
{
    type Item = A::Item;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            EitherIterator::Left(a) => a.next(),
            EitherIterator::Right(b) => b.next(),
        }
    }
}

/// Quote a `BTreeMap<K, V>` into tokens.
pub fn quote_btree_map<K: ToTokens, V: ToTokens>(value: &BTreeMap<K, V>) -> TokenStream {
    let mut tokens = TokenStream::new();
    for (k, v) in value {
        let ks = k.to_token_stream();
        let vs = v.to_token_stream();
        tokens.append_all(quote! {
            (#ks, #vs),
        });
    }
    quote! {
        ::std::collections::BTreeMap::from([
            #tokens
        ])
    }
}

/// Quote a `Cow<T>` preserving borrowed vs owned status for generated code.
pub fn quote_cow<T: ToOwned + ToTokens + ?Sized>(value: &Cow<T>) -> TokenStream
where
    <T as ToOwned>::Owned: ToTokens,
{
    match value {
        Cow::Borrowed(v) => quote! { ::std::borrow::Cow::Borrowed(#v) },
        Cow::Owned(v) => quote! { ::std::borrow::Cow::Borrowed(#v) },
    }
}

/// Quote an `Option<T>` into tokens.
pub fn quote_option<T: ToTokens>(value: &Option<T>) -> TokenStream {
    match value {
        None => quote! { None },
        Some(v) => quote! { Some(#v) },
    }
}

/// Determine if the trailing segments of a `syn::Path` match the expected identifiers.
pub fn matches_path(path: &Path, expect: &[&str]) -> bool {
    let len = min(path.segments.len(), expect.len());
    path.segments
        .iter()
        .rev()
        .take(len)
        .map(|v| &v.ident)
        .eq(expect.iter().rev().take(len))
}

/// Write an iterator of items separated by a delimiter into a string.
pub fn separated_by<T, F>(
    out: &mut String,
    values: impl IntoIterator<Item = T>,
    mut f: F,
    separator: &str,
) where
    F: FnMut(&mut String, T),
{
    let mut len = out.len();
    for v in values {
        if out.len() > len {
            out.push_str(separator);
        }
        len = out.len();
        f(out, v);
    }
}

/// Convenience wrapper converting into a `CString`, panicking on interior NUL.
pub fn as_c_string(str: impl Into<Vec<u8>>) -> CString {
    CString::new(
        str.into()
            .into_iter()
            .map(|b| if b == 0 { b'?' } else { b })
            .collect::<Vec<u8>>(),
    )
    .unwrap_or_default()
}

/// Consume a prefix of `input` while the predicate returns true, returning that slice.
pub fn consume_while<'s>(input: &mut &'s str, predicate: impl FnMut(&char) -> bool) -> &'s str {
    let len = input.chars().take_while(predicate).count();
    if len == 0 {
        return "";
    }
    let result = &input[..len];
    *input = &input[len..];
    result
}

pub fn extract_number<'s, const SIGNED: bool>(input: &mut &'s str) -> &'s str {
    let mut end = 0;
    let mut chars = input.chars().peekable();
    if SIGNED && matches!(chars.peek(), Some('+') | Some('-')) {
        chars.next();
        end += 1;
    }
    for _ in chars.take_while(char::is_ascii_digit) {
        end += 1;
    }
    let result = &input[..end];
    *input = &input[end..];
    result
}

pub fn print_timer(out: &mut String, quote: &str, h: i64, m: u8, s: u8, ns: u32) {
    let mut subsecond = ns;
    let mut width = 9;
    while width > 1 && subsecond % 10 == 0 {
        subsecond /= 10;
        width -= 1;
    }
    let _ = write!(
        out,
        "{quote}{h:02}:{m:02}:{s:02}.{subsecond:0width$}{quote}",
    );
}

#[macro_export]
macro_rules! number_to_month {
    ($month:expr, $throw:expr $(,)?) => {
        match $month {
            1 => Month::January,
            2 => Month::February,
            3 => Month::March,
            4 => Month::April,
            5 => Month::May,
            6 => Month::June,
            7 => Month::July,
            8 => Month::August,
            9 => Month::September,
            10 => Month::October,
            11 => Month::November,
            12 => Month::December,
            _ => $throw,
        }
    };
}

#[macro_export]
macro_rules! month_to_number {
    ($month:expr $(,)?) => {
        match $month {
            Month::January => 1,
            Month::February => 2,
            Month::March => 3,
            Month::April => 4,
            Month::May => 5,
            Month::June => 6,
            Month::July => 7,
            Month::August => 8,
            Month::September => 9,
            Month::October => 10,
            Month::November => 11,
            Month::December => 12,
        }
    };
}

#[macro_export]
/// Conditionally wrap a generated fragment in parentheses.
macro_rules! possibly_parenthesized {
    ($out:ident, $cond:expr, $v:expr) => {
        if $cond {
            $out.push('(');
            $v;
            $out.push(')');
        } else {
            $v;
        }
    };
}

#[macro_export]
/// Truncate long strings for logging and error messages purpose.
///
/// Returns a `format_args!` that yields at most 497 characters from the start
/// of the input followed by `...` when truncation occurred. Minimal overhead.
///
/// # Examples
/// ```rust
/// use tank_core::truncate_long;
/// let short = "SELECT 1";
/// assert_eq!(format!("{}", truncate_long!(short)), "SELECT 1\n");
/// let long = format!("SELECT {}", "X".repeat(600));
/// let logged = format!("{}", truncate_long!(long));
/// assert!(logged.starts_with("SELECT XXXXXX"));
/// assert!(logged.ends_with("...\n"));
/// ```
macro_rules! truncate_long {
    ($query:expr) => {
        format_args!(
            "{}{}",
            &$query[..::std::cmp::min($query.len(), 497)].trim(),
            if $query.len() > 497 { "...\n" } else { "" },
        )
    };
}

/// Sends the value through the channel and logs in case of error.
///
/// Parameters:
/// * `$tx`: sender channel
/// * `$value`: value to be sent
///
/// *Example*:
/// ```rust
/// send_value!(tx, Ok(QueryResult::Row(row)));
/// ```

#[macro_export]
macro_rules! send_value {
    ($tx:ident, $value:expr) => {{
        if let Err(e) = $tx.send($value) {
            log::error!("{e:#}");
        }
    }};
}

/// Incrementally accumulates tokens from a speculative parse stream until one
/// of the supplied parsers succeeds.
///
/// Returns `(accumulated_tokens, (parser1_option, parser2_option, ...))` with
/// exactly one `Some(T)`: the first successful parser.
#[doc(hidden)]
#[macro_export]
macro_rules! take_until {
    ($original:expr, $($parser:expr),+ $(,)?) => {{
        let macro_local_input = $original.fork();
        let mut macro_local_result = (
            TokenStream::new(),
            ($({
                let _ = $parser;
                None
            }),+),
        );
        loop {
            if macro_local_input.is_empty() {
                break;
            }
            let mut parsed = false;
            let produced = ($({
                let attempt = macro_local_input.fork();
                if let Ok(content) = ($parser)(&attempt) {
                    macro_local_input.advance_to(&attempt);
                    parsed = true;
                    Some(content)
                } else {
                    None
                }
            }),+);
            if parsed {
                macro_local_result.1 = produced;
                break;
            }
            macro_local_result.0.append(macro_local_input.parse::<TokenTree>()?);
        }
        $original.advance_to(&macro_local_input);
        macro_local_result
    }};
}

#[macro_export]
/// Implement the `Executor` trait for a transaction wrapper type by
/// delegating each operation to an underlying connection object.
///
/// This reduces boilerplate across driver implementations. The macro expands
/// into an `impl Executor for $transaction<'c>` with forwarding methods for
/// `prepare`, `run`, `fetch`, `execute`, and `append`.
///
/// Parameters:
/// * `$driver`: concrete driver type.
/// * `$transaction`: transaction wrapper type (generic over lifetime `'c`).
/// * `$connection`: field name on the transaction pointing to the connection.
///
/// # Examples
/// ```rust
/// use crate::{YourDBConnection, YourDBDriver};
/// use tank_core::{Error, Result, Transaction, impl_executor_transaction};
///
/// pub struct YourDBTransaction<'c> {
///     connection: &'c mut YourDBConnection,
/// }
///
/// impl_executor_transaction!(YourDBDriver, YourDBTransaction<'c>, connection);
///
/// impl<'c> Transaction<'c> for YourDBTransaction<'c> { ... }
/// ```
macro_rules! impl_executor_transaction {
    // Case 1: Lifetime is present (necessary for transactions)
    ($driver:ty, $transaction:ident $(< $lt:lifetime >)?, $connection:ident) => {
       impl $(<$lt>)? ::tank_core::Executor for $transaction $(<$lt>)? {
            type Driver = $driver;

            fn driver(&self) -> &Self::Driver {
                self.$connection.driver()
            }

            fn prepare(
                &mut self,
                query: String,
            ) -> impl Future<Output = ::tank_core::Result<::tank_core::Query<Self::Driver>>> + Send
            {
                self.$connection.prepare(query)
            }

            fn run<'s>(
                &'s mut self,
                query: impl ::tank_core::AsQuery<Self::Driver> + 's,
            ) -> impl ::tank_core::stream::Stream<
                Item = ::tank_core::Result<::tank_core::QueryResult>,
            > + Send {
                self.$connection.run(query)
            }

            fn fetch<'s>(
                &'s mut self,
                query: impl ::tank_core::AsQuery<Self::Driver> + 's,
            ) -> impl ::tank_core::stream::Stream<
                Item = ::tank_core::Result<::tank_core::RowLabeled>,
            > + Send
            + 's {
                self.$connection.fetch(query)
            }

            fn execute<'s>(
                &'s mut self,
                query: impl ::tank_core::AsQuery<Self::Driver> + 's,
            ) -> impl Future<Output = ::tank_core::Result<::tank_core::RowsAffected>> + Send {
                self.$connection.execute(query)
            }

            fn append<'a, E, It>(
                &mut self,
                entities: It,
            ) -> impl Future<Output = ::tank_core::Result<::tank_core::RowsAffected>> + Send
            where
                E: ::tank_core::Entity + 'a,
                It: IntoIterator<Item = &'a E> + Send,
            {
                self.$connection.append(entities)
            }
        }
    }
}
