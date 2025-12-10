use proc_macro2::{Delimiter, Group, Spacing, TokenStream, TokenTree};
use quote::quote;
use std::{iter, mem};

pub fn flag_evaluated(input: TokenStream) -> TokenStream {
    fn do_flagging(input: TokenStream) -> TokenStream {
        let mut iter = input.into_iter().peekable();
        let mut cur = None;
        iter::from_fn(move || {
            loop {
                let prev = mem::take(&mut cur);
                if let Some(token) = iter.next() {
                    let next = iter.peek_mut();
                    cur = Some(token);
                    match (&prev, cur.as_ref().unwrap(), next) {
                        // #identifier
                        (_, TokenTree::Punct(p), Some(tt))
                            if p.as_char() == '#' && p.spacing() == Spacing::Alone =>
                        {
                            let wrapped: TokenStream = quote!(::tank::evaluated!(#tt)).into();
                            iter.next(); // Consume the following token
                            return Some(TokenTree::Group(Group::new(
                                Delimiter::None,
                                wrapped.into(),
                            )));
                        }

                        // Asterisk preceeded by '.' or ','
                        (Some(TokenTree::Punct(a)), TokenTree::Punct(b), _)
                            if matches!(a.as_char(), '.' | ',') && b.as_char() == '*' =>
                        {
                            return Some(TokenTree::Group(Group::new(
                                Delimiter::None,
                                quote!(::tank::asterisk!()),
                            )));
                        }

                        // Asterisk as the first character
                        (None, TokenTree::Punct(p), None) if p.as_char() == '*' => {
                            return Some(TokenTree::Group(Group::new(
                                Delimiter::None,
                                quote!(::tank::asterisk!()),
                            )));
                        }

                        // Question mark
                        (_, TokenTree::Punct(punct), _) if punct.as_char() == '?' => {
                            return Some(TokenTree::Group(Group::new(
                                Delimiter::None,
                                quote!(::tank::question_mark!()),
                            )));
                        }

                        // Nested
                        (_, TokenTree::Group(group), _) => {
                            let content = do_flagging(group.stream());
                            return Some(TokenTree::Group(Group::new(group.delimiter(), content)));
                        }

                        // NOT
                        (Some(..), TokenTree::Ident(cur), Some(TokenTree::Ident(next)))
                            if cur == "NOT"
                                && matches!(next.to_string().as_str(), "LIKE" | "IN") =>
                        {
                            // Do not emit the NOT, just skip it
                            continue;
                        }

                        _ => {}
                    }
                    return cur.clone();
                } else {
                    return None;
                }
            }
        })
        .collect()
    }
    do_flagging(input)
}
