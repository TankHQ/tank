use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{
    BinOp, Expr, ExprGroup, ExprLit, ExprMacro, ExprPath, LitStr, Macro, Member, Path, Type,
    TypePath, punctuated::Punctuated, spanned::Spanned, token::Comma,
};
use tank_core::decode_type;

fn unwrap_group(expr: &Expr) -> &Expr {
    match expr {
        Expr::Group(v) => &*v.expr,
        _ => expr,
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
                                        BinOp::Ne(..) => {
                                            quote! { ::tank::BinaryOpType::NotRegexp }
                                        }
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
                    Type::Path(..) => decode_type(&cast.ty).0.value.into_token_stream(),
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
                syn::Lit::Str(v) => quote! { ::tank::Operand::LitStr(#v) },
                syn::Lit::Char(v) => quote! { ::tank::Operand::LitStr(#v) },
                syn::Lit::Int(v) => quote! { ::tank::Operand::LitInt(#v) },
                syn::Lit::Float(v) => quote! { ::tank::Operand::LitFloat(#v) },
                syn::Lit::Bool(v) => quote! { ::tank::Operand::LitBool(#v) },
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
            if path
                .segments
                .iter()
                .map(|v| v.ident.to_string())
                .eq(["tank", "evaluated"].into_iter())
            {
                quote! { ::tank::Operand::Variable(::tank::AsValue::as_value(#tokens)) }
            } else if path
                .segments
                .iter()
                .map(|v| v.ident.to_string())
                .eq(["tank", "asterisk"].into_iter())
            {
                quote! { ::tank::Operand::Asterisk }
            } else if path.segments.iter().map(|v| v.ident.to_string()).eq([
                "tank",
                "question_mark",
            ]
            .into_iter())
            {
                quote! { ::tank::Operand::QuestionMark }
            } else {
                quote! { #path!(#tokens) }
            }
        }
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
            quote! { ::tank::Operand::LitArray(&[#v]) }
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
