use crate::{CheckPassive, TypeDecoded, Value, matches_path};
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use std::mem;
use syn::{
    Expr, ExprLit, GenericArgument, Lit, PathArguments, Type, TypeArray, TypePath, TypeSlice,
};

#[doc(hidden)]
pub fn decode_type(ty: &Type) -> (TypeDecoded, Option<CheckPassive>) {
    let mut nullable = false;
    let mut filter_passive = None;
    let data_type = 'data_type: {
        if let Type::Path(TypePath { path, .. }) = ty {
            let ident = path.get_ident();
            if let Some(ident) = ident {
                if ident == "bool" {
                    break 'data_type Value::Boolean(None);
                } else if ident == "i8" {
                    break 'data_type Value::Int8(None);
                } else if ident == "i16" {
                    break 'data_type Value::Int16(None);
                } else if ident == "i32" {
                    break 'data_type Value::Int32(None);
                } else if ident == "i64" {
                    break 'data_type Value::Int64(None);
                } else if ident == "i128" {
                    break 'data_type Value::Int128(None);
                } else if ident == "u8" {
                    break 'data_type Value::UInt8(None);
                } else if ident == "u16" {
                    break 'data_type Value::UInt16(None);
                } else if ident == "u32" {
                    break 'data_type Value::UInt32(None);
                } else if ident == "u64" {
                    break 'data_type Value::UInt64(None);
                } else if ident == "u128" {
                    break 'data_type Value::UInt128(None);
                } else if ident == "isize" {
                    break 'data_type if mem::size_of::<isize>() == mem::size_of::<i32>() {
                        Value::Int32(None)
                    } else {
                        Value::Int64(None)
                    };
                } else if ident == "usize" {
                    break 'data_type if mem::size_of::<usize>() == mem::size_of::<u32>() {
                        Value::UInt32(None)
                    } else {
                        Value::UInt64(None)
                    };
                } else if ident == "f32" {
                    break 'data_type Value::Float32(None);
                } else if ident == "f64" {
                    break 'data_type Value::Float64(None);
                } else if ident == "char" {
                    break 'data_type Value::Char(None);
                } else if ident == "str" {
                    break 'data_type Value::Varchar(None);
                }
            }
            if matches_path(path, &["std", "string", "String"]) {
                break 'data_type Value::Varchar(None);
            } else if matches_path(path, &["rust_decimal", "Decimal"]) {
                break 'data_type Value::Decimal(None, 0, 0);
            } else if matches_path(path, &["tank", "FixedDecimal"]) {
                let PathArguments::AngleBracketed(arguments) = &path
                    .segments
                    .last()
                    .expect("FixedDecimal must have two generic values")
                    .arguments
                else {
                    panic!("`{}` must have 2 generic arguments", path.to_token_stream());
                };
                let ws = arguments
                    .args
                    .iter()
                    .take(2)
                    .map(|arg| match arg {
                        GenericArgument::Const(Expr::Lit(ExprLit {
                            lit: Lit::Int(v), ..
                        })) => v.base10_digits().parse::<u8>().expect("Must be a integer"),
                        _ => panic!(),
                    })
                    .collect::<Vec<_>>();
                break 'data_type Value::Decimal(
                    None,
                    *ws.first().expect("Doesn't have width param"),
                    *ws.last().expect("Doesn't have size param"),
                );
            } else if matches_path(path, &["time", "Time"]) {
                break 'data_type Value::Time(None);
            } else if matches_path(path, &["time", "Date"]) {
                break 'data_type Value::Date(None);
            } else if matches_path(path, &["time", "PrimitiveDateTime"]) {
                break 'data_type Value::Timestamp(None);
            } else if matches_path(path, &["time", "OffsetDateTime"])
                || matches_path(path, &["chrono", "DateTime"])
            {
                break 'data_type Value::TimestampWithTimezone(None);
            } else if matches_path(path, &["std", "time", "Duration"])
                || matches_path(path, &["tank", "Interval"])
            {
                break 'data_type Value::Interval(None);
            } else if matches_path(path, &["uuid", "Uuid"]) {
                break 'data_type Value::Uuid(None);
            } else {
                let is_passive = matches_path(path, &["tank", "Passive"]);
                let is_option = matches_path(path, &["std", "option", "Option"]);
                let is_cow = matches_path(path, &["std", "borrow", "Cow"]);
                let is_list = matches_path(path, &["std", "vec", "Vec"])
                    || matches_path(path, &["std", "collections", "VecDeque"])
                    || matches_path(path, &["std", "collections", "LinkedList"]);
                let is_map = matches_path(path, &["std", "collections", "HashMap"])
                    || matches_path(path, &["std", "collections", "BTreeMap"]);
                let is_wrapper = is_option
                    || is_passive
                    || is_cow
                    || matches_path(path, &["std", "boxed", "Box"])
                    || matches_path(path, &["std", "cell", "Cell"])
                    || matches_path(path, &["std", "cell", "RefCell"])
                    || matches_path(path, &["std", "rc", "Rc"])
                    || matches_path(path, &["std", "sync", "Arc"])
                    || matches_path(path, &["std", "sync", "RwLock"]);
                if is_wrapper || is_list || is_map {
                    match &path
                        .segments
                        .last()
                        .expect("Path must be non empty")
                        .arguments
                    {
                        PathArguments::AngleBracketed(bracketed) => {
                            let mut args = bracketed.args.iter();
                            let mut generic_type = args
                                .next()
                                .expect("Must have at least one generic argument");
                            if is_cow {
                                generic_type = args
                                    .next()
                                    .expect("Must have at least two generic argument");
                            }
                            if let GenericArgument::Type(generic_type) = generic_type {
                                let first_type = decode_type(&generic_type);
                                if first_type.1.is_some() {
                                    filter_passive = first_type.1;
                                }
                                let first_type = first_type.0;
                                if is_wrapper {
                                    nullable = if is_option {
                                        true
                                    } else {
                                        nullable || first_type.nullable
                                    };
                                    if is_passive {
                                        filter_passive = Some(Box::new(|v: TokenStream| {
                                            quote!(matches!(#v, ::tank::Passive::Set(..)))
                                        }));
                                    } else if is_wrapper && filter_passive.is_some() {
                                        let passive = filter_passive.take();
                                        filter_passive = Some(Box::new(move |v: TokenStream| {
                                            passive.as_ref().expect(
                                                "The value `filter_passive` is checked to be some",
                                            )(
                                                quote!(*#v)
                                            )
                                        }))
                                    }
                                    break 'data_type first_type.value;
                                } else if is_list {
                                    break 'data_type Value::List(None, Box::new(first_type.value));
                                } else if is_map {
                                    let panic_msg = &format!(
                                        "Type `{}` must have two generic arguments at lease",
                                        path.to_token_stream().to_string()
                                    );
                                    let GenericArgument::Type(second_type) =
                                        bracketed.args.get(1).expect(panic_msg)
                                    else {
                                        panic!("{}", panic_msg);
                                    };
                                    let second_type = decode_type(second_type).0;
                                    break 'data_type Value::Map(
                                        None,
                                        Box::new(first_type.value),
                                        Box::new(second_type.value),
                                    );
                                }
                            } else {
                                panic!(
                                    "{} must have a type as the first generic argument",
                                    path.to_token_stream()
                                )
                            }
                        }
                        _ => panic!("`{}` must have a generic argument", path.to_token_stream()),
                    }
                }
            }
            panic!("Unknown type `{}`", path.to_token_stream());
        } else if let Type::Array(TypeArray {
            elem,
            len:
                Expr::Lit(ExprLit {
                    lit: Lit::Int(len, ..),
                    ..
                }),
            ..
        }) = ty
        {
            break 'data_type Value::Array(
                None,
                Box::new(decode_type(&*elem).0.value),
                len.base10_parse().expect(&format!(
                    "Expected a integer literal array length in `{}`",
                    ty.to_token_stream().to_string()
                )),
            );
        } else if let Type::Slice(TypeSlice { elem, .. }) = ty {
            let element_type = decode_type(&*elem).0;
            if matches!(
                element_type,
                TypeDecoded {
                    value: Value::UInt8(..),
                    nullable: false,
                    passive: false,
                }
            ) {
                break 'data_type Value::Blob(None);
            }
            break 'data_type Value::List(None, Box::new(decode_type(&*elem).0.value));
        } else {
            panic!("Unexpected type `{}`", ty.to_token_stream().to_string())
        }
    };
    (
        TypeDecoded {
            value: data_type,
            nullable,
            passive: filter_passive.is_some(),
        },
        filter_passive,
    )
}
