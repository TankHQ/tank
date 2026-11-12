use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{
    BinOp, Block, Expr, ExprGroup, ExprLit, ExprMacro, ExprPath, LitStr, Macro, Member, Pat, Path,
    Stmt, Type, TypePath, punctuated::Punctuated, spanned::Spanned, token::Comma,
};
use tank_core::{decode_type, matches_path};

fn unwrap_group(expr: &Expr) -> &Expr {
    match expr {
        Expr::Group(v) => &*v.expr,
        _ => expr,
    }
}

/// The single expression of a `CASE` branch body: `{ expr }`.
fn decode_branch(block: &Block) -> TokenStream {
    match block.stmts.as_slice() {
        [Stmt::Expr(expr, None)] => decode_expression(expr),
        _ => panic!(
            "A CASE branch must contain exactly one expression, found `{}`",
            block.to_token_stream()
        ),
    }
}

/// The values a `match` arm pattern compares against: literals, paths, `#variables` and `a | b`.
fn decode_patterns(pat: &Pat, out: &mut Vec<TokenStream>) {
    match pat {
        Pat::Or(v) => v.cases.iter().for_each(|case| decode_patterns(case, out)),
        Pat::Paren(v) => decode_patterns(&v.pat, out),
        Pat::Path(v) if v.path.is_ident("NULL") => {
            panic!("`NULL` never compares equal in a CASE, use `if value == NULL {{ .. }}` instead")
        }
        Pat::Lit(v) => out.push(decode_expression(&Expr::Lit(v.clone()))),
        Pat::Path(v) => out.push(decode_expression(&Expr::Path(v.clone()))),
        Pat::Macro(v) => out.push(decode_expression(&Expr::Macro(v.clone()))),
        _ => panic!(
            "Unsupported CASE pattern `{}`, expected literals, paths, `#variables` or `a | b`",
            pat.to_token_stream()
        ),
    }
}

fn case_tokens(
    value: Option<TokenStream>,
    when: Vec<(TokenStream, TokenStream)>,
    otherwise: Option<TokenStream>,
) -> TokenStream {
    let value = match value {
        Some(v) => quote!(Some(&#v as &dyn ::tank::Expression)),
        None => quote!(None),
    };
    let when = when.iter().map(|(when, then)| {
        quote!((&#when as &dyn ::tank::Expression, &#then as &dyn ::tank::Expression))
    });
    let otherwise = match otherwise {
        Some(v) => quote!(Some(&#v as &dyn ::tank::Expression)),
        None => quote!(None),
    };
    quote! {
        ::tank::Case {
            value: #value,
            when: &[#(#when),*],
            otherwise: #otherwise,
        }
    }
}

pub fn decode_expression(expr: &Expr) -> TokenStream {
    match expr {
        Expr::Binary(expr_binary) => {
            let lhs = expr_binary.left.as_ref();
            let mut rhs = expr_binary.right.as_ref();
            let op = expr_binary.op;
            let op = match expr_binary.op {
                BinOp::Add(..) => quote! { ::tank::BinaryOpType::Addition },
                BinOp::Sub(..) => quote! { ::tank::BinaryOpType::Subtraction },
                BinOp::Mul(..) => quote! { ::tank::BinaryOpType::Multiplication },
                BinOp::Div(..) => quote! { ::tank::BinaryOpType::Division },
                BinOp::Rem(..) => quote! { ::tank::BinaryOpType::Remainder },
                BinOp::And(..) => quote! { ::tank::BinaryOpType::And },
                BinOp::Or(..) => quote! { ::tank::BinaryOpType::Or },
                BinOp::BitAnd(..) => quote! { ::tank::BinaryOpType::BitwiseAnd },
                BinOp::BitOr(..) => quote! { ::tank::BinaryOpType::BitwiseOr },
                BinOp::Shl(..) => quote! { ::tank::BinaryOpType::ShiftLeft },
                BinOp::Shr(..) => quote! { ::tank::BinaryOpType::ShiftRight },
                BinOp::Eq(..) | BinOp::Ne(..) => {
                    let mut result = match op {
                        BinOp::Eq(..) => quote! { ::tank::BinaryOpType::Equal },
                        BinOp::Ne(..) => quote! { ::tank::BinaryOpType::NotEqual },
                        _ => unreachable!(),
                    };
                    if let Expr::Cast(cast) = unwrap_group(expr_binary.right.as_ref()) {
                        if let Type::Path(TypePath {
                            path: Path { segments, .. },
                            ..
                        }) = cast.ty.as_ref()
                        {
                            if segments.len() == 1 {
                                let identifier = &segments
                                    .last()
                                    .expect("The path has exactly one segment on this branch")
                                    .ident;
                                if identifier == "IN" {
                                    rhs = &cast.expr;
                                    result = match op {
                                        BinOp::Eq(..) => quote! { ::tank::BinaryOpType::In },
                                        BinOp::Ne(..) => quote! { ::tank::BinaryOpType::NotIn },
                                        _ => unreachable!(),
                                    }
                                } else if identifier == "IS" {
                                    rhs = &cast.expr;
                                    result = match op {
                                        BinOp::Eq(..) => quote! { ::tank::BinaryOpType::Is },
                                        BinOp::Ne(..) => quote! { ::tank::BinaryOpType::IsNot },
                                        _ => unreachable!(),
                                    }
                                } else if identifier == "LIKE" {
                                    rhs = &cast.expr;
                                    result = match op {
                                        BinOp::Eq(..) => quote! { ::tank::BinaryOpType::Like },
                                        BinOp::Ne(..) => quote! { ::tank::BinaryOpType::NotLike },
                                        _ => unreachable!(),
                                    }
                                } else if identifier == "REGEXP" {
                                    rhs = &cast.expr;
                                    result = match op {
                                        BinOp::Eq(..) => quote! { ::tank::BinaryOpType::Regexp },
                                        BinOp::Ne(..) => quote! { ::tank::BinaryOpType::NotRegexp },
                                        _ => unreachable!(),
                                    }
                                } else if identifier == "GLOB" {
                                    rhs = &cast.expr;
                                    result = match op {
                                        BinOp::Eq(..) => quote! { ::tank::BinaryOpType::Glob },
                                        BinOp::Ne(..) => quote! { ::tank::BinaryOpType::NotGlob },
                                        _ => unreachable!(),
                                    }
                                }
                            }
                        }
                    } else if let Expr::Path(ExprPath {
                        path: Path { segments, .. },
                        ..
                    }) = expr_binary.right.as_ref()
                    {
                        if segments.iter().map(|v| &v.ident).eq(["NULL"].iter()) {
                            result = match op {
                                BinOp::Eq(..) => quote! { ::tank::BinaryOpType::Is },
                                BinOp::Ne(..) => quote! { ::tank::BinaryOpType::IsNot },
                                _ => unreachable!(),
                            };
                        }
                    }
                    result
                }
                BinOp::Lt(..) => quote! { ::tank::BinaryOpType::Less },
                BinOp::Le(..) => quote! { ::tank::BinaryOpType::LessEqual },
                BinOp::Ge(..) => quote! { ::tank::BinaryOpType::GreaterEqual },
                BinOp::Gt(..) => quote! { ::tank::BinaryOpType::Greater },
                _ => panic!("Unexpected binary operator"),
            };
            let lhs = decode_expression(lhs);
            let rhs = decode_expression(rhs);
            quote! {
                ::tank::BinaryOp {
                    op: #op,
                    lhs: #lhs,
                    rhs: #rhs,
                }
            }
        }
        Expr::Index(v) => {
            let lhs = decode_expression(&v.expr);
            let rhs = decode_expression(&v.index);
            quote! {
                ::tank::BinaryOp {
                    op: ::tank::BinaryOpType::Indexing,
                    lhs: #lhs,
                    rhs: #rhs,
                }
            }
        }
        Expr::Cast(cast) => {
            let lhs = decode_expression(&cast.expr);
            let rhs = match cast.ty.as_ref() {
                Type::Path(TypePath { path, .. }) => decode_expression(&Expr::Path(ExprPath {
                    attrs: Default::default(),
                    qself: None,
                    path: path.clone(),
                })),
                _ => panic!(
                    "Unexpected cast type, cast can only be a type or the special keyworkds: `IS`, `LIKE`, `REGEXP`, `GLOB`"
                ),
            };
            quote! {
                ::tank::BinaryOp {
                    op: ::tank::BinaryOpType::Alias,
                    lhs: #lhs,
                    rhs: #rhs,
                }
            }
        }
        Expr::Unary(v) => {
            let op = match v.op {
                syn::UnOp::Not(..) => quote! { ::tank::UnaryOpType::Not },
                syn::UnOp::Neg(..) => quote! { ::tank::UnaryOpType::Negative },
                _ => panic!("Unsupported operator `{}`", v.op.to_token_stream()),
            };
            let arg = decode_expression(v.expr.as_ref());
            quote! {
                ::tank::UnaryOp {
                    op: #op,
                    arg: #arg,
                }
            }
        }
        Expr::Call(v) => {
            let f = v.func.as_ref();
            let Expr::Path(ExprPath { path, .. }) = f else {
                panic!(
                    "Function `{}` must be a path or some identifier",
                    v.into_token_stream(),
                );
            };
            if path.is_ident("CAST")
                && v.args.len() == 1
                && let Some(Expr::Cast(cast)) = v.args.first()
            {
                let lhs = decode_expression(&cast.expr);
                let rhs = match cast.ty.as_ref() {
                    Type::Path(..) => decode_type(&cast.ty).value.into_token_stream(),
                    _ => panic!(
                        "Unexpected cast type, cast can only be a Rust valid type (check tank::Value)"
                    ),
                };
                quote! {
                    ::tank::BinaryOp {
                        op: ::tank::BinaryOpType::Cast,
                        lhs: #lhs,
                        rhs: ::tank::Operand::Type(#rhs),
                    }
                }
            } else {
                let args = v.args.iter().map(|v| decode_expression(v));
                let path = path.into_token_stream().to_string();
                quote! { ::tank::Operand::Call(&#path, &[#(&#args),*]) }
            }
        }
        Expr::Lit(ExprLit { lit: v, .. }) => {
            let v = match v {
                syn::Lit::Bool(v) => quote! { ::tank::Operand::LitBool(#v) },
                syn::Lit::Int(v) => {
                    let value = v.base10_parse::<i128>().unwrap_or_else(|e| {
                        panic!("Integer literal is out of range for i128: {e}")
                    });
                    quote! { ::tank::Operand::LitInt(#value) }
                }
                syn::Lit::Float(v) => quote! { ::tank::Operand::LitFloat(#v as _) },
                syn::Lit::Str(v) => quote! { ::tank::Operand::LitStr(#v) },
                syn::Lit::Char(v) => {
                    let s = LitStr::new(&v.value().to_string(), v.span());
                    quote! { ::tank::Operand::LitStr(#s) }
                }
                _ => panic!(
                    "Unexpected value {:?} in a sql expression",
                    v.into_token_stream()
                ),
            };
            quote! { #v }
        }
        Expr::Macro(ExprMacro {
            mac: Macro { path, tokens, .. },
            ..
        }) => {
            if matches_path(path, &["tank", "evaluated"]) {
                quote! { ::tank::Operand::Variable(::tank::AsValue::as_value(#tokens)) }
            } else if matches_path(path, &["tank", "asterisk"]) {
                quote! { ::tank::Operand::Asterisk }
            } else if matches_path(path, &["tank", "question_mark"]) {
                quote! { ::tank::Operand::QuestionMark }
            } else if matches_path(path, &["tank", "current_timestamp_ms"]) {
                quote! { ::tank::Operand::CurrentTimestampMs }
            } else {
                quote! { #path!(#tokens) }
            }
        }
        // `if a { x } else if b { y } else { z }` is `CASE WHEN a THEN x WHEN b THEN y ELSE z END`.
        Expr::If(_) => {
            let mut when = Vec::new();
            let mut current = expr;
            let otherwise = loop {
                match current {
                    Expr::If(v) => {
                        when.push((decode_expression(&v.cond), decode_branch(&v.then_branch)));
                        match &v.else_branch {
                            Some((_, branch)) => current = branch,
                            None => break None,
                        }
                    }
                    Expr::Block(v) => break Some(decode_branch(&v.block)),
                    _ => break Some(decode_expression(current)),
                }
            };
            case_tokens(None, when, otherwise)
        }
        // `match v { 1 => x, 2 | 3 => y, _ => z }` is `CASE v WHEN 1 THEN x WHEN 2 THEN y WHEN 3 THEN y ELSE z END`.
        Expr::Match(v) => {
            let mut when = Vec::new();
            let mut otherwise = None;
            for (i, arm) in v.arms.iter().enumerate() {
                if arm.guard.is_some() {
                    panic!("CASE arms cannot have guards, use `if .. else` for conditions");
                }
                let then = decode_expression(&arm.body);
                if let Pat::Wild(..) = arm.pat {
                    if i + 1 != v.arms.len() {
                        panic!("The `_` arm of a CASE must be the last one");
                    }
                    otherwise = Some(then);
                    continue;
                }
                let mut values = Vec::new();
                decode_patterns(&arm.pat, &mut values);
                when.extend(values.into_iter().map(|value| (value, then.clone())));
            }
            case_tokens(Some(decode_expression(&v.expr)), when, otherwise)
        }
        Expr::Block(v) => decode_branch(&v.block),
        Expr::MethodCall(_) => todo!("Expr::MethodCall"),
        Expr::Paren(v) => decode_expression(&v.expr),
        Expr::Path(ExprPath { path, .. }) => {
            if path.segments.len() > 1 {
                quote! { #path }
            } else {
                let ident = path
                    .get_ident()
                    .expect("The path is expected to be identifier");
                if ident == "NULL" {
                    quote!(::tank::Operand::Null)
                } else {
                    let v = LitStr::new(&path.to_token_stream().to_string(), path.span());
                    quote!(::tank::Operand::LitIdent(#v))
                }
            }
        }
        Expr::Field(v) => {
            let mut current = expr;
            let mut segments = Vec::new();
            let do_panic = || {
                panic!(
                    "Unexpected field expression {}, it should dot separated names like: namespace.table.column",
                    v.to_token_stream().to_string()
                )
            };
            loop {
                match current {
                    Expr::Field(field) => {
                        match &field.member {
                            Member::Named(ident) => segments.push(ident.to_string()),
                            _ => do_panic(),
                        }
                        current = &field.base;
                    }
                    Expr::Path(path) => {
                        for segment in path.path.segments.iter().rev() {
                            segments.push(segment.ident.to_string());
                        }
                        break;
                    }
                    _ => do_panic(),
                }
            }
            let segments = segments.into_iter().rev().collect::<Vec<_>>();
            quote! { ::tank::Operand::LitField(&[#(#segments),*]) }
        }
        Expr::Array(v) => {
            let v = v
                .elems
                .iter()
                .map(|v| decode_expression(v))
                .collect::<Punctuated<_, Comma>>();
            quote! { ::tank::Operand::LitList(&[#v]) }
        }
        Expr::Tuple(v) => {
            let v = v
                .elems
                .iter()
                .map(|v| decode_expression(v))
                .collect::<Punctuated<_, Comma>>();
            quote! { ::tank::Operand::LitTuple(&[#v]) }
        }
        Expr::Group(ExprGroup { expr, .. }) => decode_expression(&expr),
        _ => panic!(
            "Unexpected expression `{}`",
            expr.to_token_stream().to_string()
        ),
    }
}
