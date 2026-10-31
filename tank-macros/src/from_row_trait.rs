use crate::TableMetadata;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, LitByteStr, spanned::Spanned};

pub(crate) fn from_row_trait(table: &TableMetadata) -> (Ident, TokenStream) {
    let item = &table.item;
    let struct_name = &item.ident;
    let trait_name = Ident::new(&format!("{struct_name}FromRowTrait"), item.span());
    let factory_name = Ident::new(&format!("{struct_name}FromRowFactory"), item.span());
    let columns_len = table.columns.len();
    let unique_names = (1..columns_len).all(|i| {
        table.columns[..i]
            .iter()
            .all(|c| c.name != table.columns[i].name)
    });
    let idents = table.columns.iter().map(|c| &c.ident).collect::<Vec<_>>();
    let tys = table.columns.iter().map(|c| &c.ty);
    let names = table.columns.iter().map(|c| &c.name).collect::<Vec<_>>();
    let name_lits = table
        .columns
        .iter()
        .map(|c| LitByteStr::new(c.name.as_bytes(), item.span()))
        .collect::<Vec<_>>();
    let remaining = item
        .fields
        .iter()
        .filter_map(|f| f.ident.as_ref())
        .filter(|id| !idents.contains(id))
        .collect::<Vec<_>>();
    let convs = table
        .columns
        .iter()
        .map(|c| {
            let (ty, conv) = (&c.ty, c.conversion_type.as_ref().unwrap_or(&c.ty));
            quote!(::std::convert::Into::<#ty>::into(
                <#conv as ::tank::AsValue>::try_from_value(__v__)?
            ))
        })
        .collect::<Vec<_>>();
    let indices = (0..columns_len).collect::<Vec<_>>();
    let is_ordered = quote! {
        #unique_names
            && row.len() >= #columns_len
            && matches!(row.names(), [#(#idents),*] if true #(&& #idents.as_bytes() == #name_lits)*)
    };
    let fallback = |assigns: TokenStream| {
        quote! {
            ::std::hint::cold_path();
            for (__i__, __n__) in row.names().iter().enumerate() {
                let Some(__v__) = row.value(__i__) else { break };
                #[allow(unreachable_patterns)]
                match __n__.as_bytes() {
                    #assigns
                    _ => drop(__v__),
                }
            }
        }
    };
    let fallback_default = fallback(quote!(#(#name_lits => result.#idents = #convs,)*));
    let fallback_holder = fallback(quote!(#(#name_lits => #idents = Some(#convs),)*));
    (
        factory_name.clone(),
        quote! {
            trait #trait_name {
                fn make_error(name: &str) -> ::tank::Error;
                fn from_row(row: impl ::tank::Row) -> ::tank::Result<#struct_name>;
            }
            struct #factory_name<T>(std::marker::PhantomData<T>);
            impl<T: Default + Into<#struct_name>> #factory_name<T> {
                #[inline(always)]
                fn from_row(row: impl ::tank::Row) -> ::tank::Result<#struct_name> {
                    let mut result = T::default().into();
                    if #is_ordered {
                        #(
                            let Some(__v__) = row.value(#indices) else {
                                return Ok(result);
                            };
                            result.#idents = #convs;
                        )*
                        return Ok(result);
                    }
                    #fallback_default
                    Ok(result)
                }
            }
            impl<T> #trait_name for #factory_name<T> {
                #[cold]
                #[inline(never)]
                fn make_error(name: &str) -> ::tank::Error {
                    ::tank::anyhow!(
                        "Column `{name}` does not exist in the row provided (implement `Default` for {} or get all the columns)",
                        stringify!(#struct_name),
                    )
                }
                #[inline(always)]
                fn from_row(row: impl ::tank::Row) -> ::tank::Result<#struct_name> {
                    if #is_ordered {
                        return Ok(#struct_name {
                            #(
                                #idents: {
                                    let Some(__v__) = row.value(#indices) else {
                                        return Err(Self::make_error(#names));
                                    };
                                    #convs
                                },
                            )*
                            #(#remaining: Default::default(),)*
                        });
                    }
                    #(let mut #idents: Option<#tys> = None;)*
                    #fallback_holder
                    Ok(#struct_name {
                        #(#idents: #idents.ok_or_else(|| Self::make_error(#names))?,)*
                        #(#remaining: Default::default(),)*
                    })
                }
            }
        },
    )
}
