use proc_macro2::{TokenStream, TokenTree};
use quote::{TokenStreamExt, quote};
use syn::{
    Expr, Ident, Path, Result, custom_keyword, parenthesized,
    parse::{Parse, ParseBuffer, ParseStream, discouraged::Speculative},
    parse2,
    token::Paren,
};
use tank_core::{JoinType, take_until};

pub(crate) struct JoinParsed(pub(crate) TokenStream);
struct JoinMemberParsed(pub(crate) TokenStream);

fn parse_join_rhs(original: ParseStream, join: JoinType, lhs: TokenStream) -> Result<TokenStream> {
    let input = original.fork();
    let (rhs, on, expr_type, chained_join) = {
        custom_keyword!(ON);
        let (rhs, (on, chained_join)) = take_until!(
            input,
            ParseBuffer::parse::<ON>,
            ParseBuffer::parse::<JoinType>,
        );
        let Ok(rhs) = parse2::<JoinMemberParsed>(rhs) else {
            return Err(input.error(
                "Expected to find a right hand side after the join, a table or a nested join",
            ));
        };
        let (expr, expr_type, chained_join) = if on.is_some() {
            let (expr, chained_join) = take_until!(input, ParseBuffer::parse::<JoinType>);
            let Ok(expr) = parse2::<Expr>(expr) else {
                return Err(input.error("Expected to find a valid expression after `ON`"));
            };
            (
                quote! { Some(::tank::expr!(#expr)) },
                quote! { _ },
                chained_join,
            )
        } else {
            (quote! { None }, quote! { () }, chained_join)
        };
        (rhs, expr, expr_type, chained_join)
    };
    let rhs = rhs.0;
    let result = quote! {
        ::tank::Join::<_, _, #expr_type> {
            join: #join,
            lhs: #lhs,
            rhs: #rhs,
            on: #on,
        }
    };
    original.advance_to(&input);
    if let Some(chained) = chained_join {
        parse_join_rhs(original, chained, result)
    } else {
        Ok(result)
    }
}

impl Parse for JoinParsed {
    fn parse(original: ParseStream) -> Result<Self> {
        let input = original.fork();

        // Unwrap eventual parentheses
        if original.peek(Paren) {
            let attempt = input.fork();
            let content;
            parenthesized!(content in attempt);
            if attempt.is_empty() {
                // The whole join can be parenthesized
                original.advance_to(&attempt);
                return Self::parse(&content);
            }
        }

        // Base case
        let (lhs, join) = take_until!(input, ParseBuffer::parse::<JoinType>);
        let Some(join) = join else {
            return Err(input.error("Expected to find join keywords in the input"));
        };
        let lhs = parse2::<JoinMemberParsed>(lhs)?.0;
        let parsed = parse_join_rhs(&input, join, lhs)?;

        original.advance_to(&input);
        Ok(Self(parsed))
    }
}

impl Parse for JoinMemberParsed {
    fn parse(input: ParseStream) -> Result<Self> {
        if let Ok(join) = input.parse::<JoinParsed>() {
            return Ok(Self(join.0));
        }
        let table: Path = if let Ok(table) = input.parse::<Path>() {
            table
        } else if let Ok(table) = input.parse::<Ident>() {
            table.into()
        } else {
            return Err(input.error("Expected a table or some nested join"));
        };
        if input.peek(Ident) {
            let alias = input.parse::<Ident>()?;
            Ok(Self(
                quote! { ::tank::DeclareTableRef(<#table as ::tank::Entity>::table().with_alias(stringify!(#alias).into())) },
            ))
        } else {
            Ok(Self(quote! { <#table as ::tank::Entity>::table() }))
        }
    }
}
