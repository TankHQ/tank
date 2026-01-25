use crate::{
    DynQuery, Expression, ExpressionMatcher, OpPrecedence,
    writer::{Context, SqlWriter},
};
use proc_macro2::TokenStream;
use quote::{ToTokens, TokenStreamExt, quote};

#[derive(Debug, Clone, Copy)]
pub enum Order {
    ASC,
    DESC,
}

impl ToTokens for Order {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.append_all(match self {
            Order::ASC => quote!(::tank::Order::ASC),
            Order::DESC => quote!(::tank::Order::DESC),
        });
    }
}

#[derive(Debug)]
pub struct Ordered<E: Expression> {
    pub expression: E,
    pub order: Order,
}

impl<E: Expression> OpPrecedence for Ordered<E> {
    fn precedence(&self, writer: &dyn SqlWriter) -> i32 {
        self.expression.precedence(writer)
    }
}

impl<E: Expression> Expression for Ordered<E> {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_expression_ordered(
            context,
            out,
            &Ordered {
                expression: &self.expression,
                order: self.order,
            },
        );
    }

    fn matches(&self, matcher: &mut dyn ExpressionMatcher) -> bool {
        matcher.match_ordered(&Ordered {
            expression: self as &dyn Expression,
            order: self.order,
        })
    }
}
