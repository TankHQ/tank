use proc_macro2::{Delimiter, Group, Spacing, TokenStream, TokenTree};
use quote::quote;
use std::iter;

pub fn flag_evaluated(input: TokenStream) -> TokenStream {
    fn do_flagging(input: TokenStream) -> TokenStream {
        let mut iter = input.into_iter().peekable();
        let mut cur = None;
        iter::from_fn(move || {
            let prev = cur.clone();
            if let Some(token) = iter.next() {
                let next = iter.peek_mut();
                cur = Some(token.clone());
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

                    // IS
                    (_, TokenTree::Ident(ident), Some(rhs)) if ident == "IS" => {
                        *rhs = TokenTree::Group(Group::new(Delimiter::None, quote!(#rhs as IS)));
                        return Some(TokenTree::Group(Group::new(Delimiter::None, quote!(==))));
                    }

                    // IN
                    (Some(prev), TokenTree::Ident(ident), Some(rhs)) if ident == "IN" => {
                        *rhs = TokenTree::Group(Group::new(Delimiter::None, quote!(#rhs as IN)));
                        return Some(TokenTree::Group(Group::new(
                            Delimiter::None,
                            match prev {
                                TokenTree::Ident(v) if v == "NOT" => quote!(!=),
                                _ => quote!(==),
                            },
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
                    _ => {}
                }
                return Some(token);
            }
            None
        })
        .collect()
    }
    do_flagging(input)
}
