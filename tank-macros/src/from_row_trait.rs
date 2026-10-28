use crate::{ColumnMetadata, TableMetadata};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, spanned::Spanned};

pub(crate) fn from_row_trait(table: &TableMetadata) -> (Ident, TokenStream) {
    let item = &table.item;
    let struct_name = &item.ident;
    let trait_name = Ident::new(&format!("{struct_name}FromRowTrait"), item.span());
    let factory_name = Ident::new(&format!("{struct_name}FromRowFactory"), item.span());
    let fields_holder_declarations = table.columns.iter().map(|ColumnMetadata { ident, ty, .. }| {
        quote!(let mut #ident: Option<#ty> = None;)
    });
    let arms = |wrap: fn(&Ident, TokenStream) -> TokenStream| {
        table.columns.iter().map(
            move |ColumnMetadata {
                      ident,
                      name,
                      ty,
                      conversion_type,
                      ..
                  }| {
                let name = name.as_bytes();
                let conversion_type = conversion_type.as_ref().unwrap_or(ty);
                let assign = wrap(
                    ident,
                    quote!(::std::convert::Into::<#ty>::into(
                        <#conversion_type as ::tank::AsValue>::try_from_value(__v__)?
                    )),
                );
                quote! {
                    [#(#name),*] => {
                        #assign;
                    }
                }
            },
        )
    };
    let field_assignment_default = arms(|field, expr| quote!(result.#field = #expr));
    let field_assignment_holder = arms(|field, expr| quote!(#field = Some(#expr)));
    let create_result = table.columns.iter().map(|ColumnMetadata { name, ident, .. }| {
        quote!(#ident: #ident.ok_or_else(|| __make_error__(#name))?)
    });
    let remaining = item
        .fields
        .iter()
        .filter(|field| {
            table
                .columns
                .iter()
                .all(|c| c.ident != *field.ident.as_ref().unwrap())
        })
        .map(|field| {
            let ident = field.ident.as_ref().unwrap();
            quote!(#ident: Default::default())
        });
    (
        factory_name.clone(),
        quote! {
            trait #trait_name {
                fn from_row(
                    row: impl ::tank::Row,
                ) -> ::tank::Result<#struct_name>;
            }
            struct #factory_name<T>(std::marker::PhantomData<T>);
            impl<T: Default + Into<#struct_name>> #factory_name<T> {
                // Called when T has Default Trait
                #[inline(always)]
                fn from_row(
                    row: impl ::tank::Row,
                ) -> ::tank::Result<#struct_name> {
                    let mut result = T::default().into();
                    for (__i__, __n__) in row.names().iter().enumerate() {
                        let Some(__v__) = row.value(__i__) else { break };
                        #[allow(unreachable_patterns)]
                        match __n__.as_bytes() {
                            #(#field_assignment_default)*
                            _ => {},
                        }
                    }
                    Ok(result)
                }
            }
            impl<T> #trait_name for #factory_name<T> {
                // Called when T doesn't have default trait
                #[inline(always)]
                fn from_row(
                    row: impl ::tank::Row,
                ) -> ::tank::Result<#struct_name> {
                    #(#fields_holder_declarations)*
                    let __make_error__ = |name: &str| ::tank::anyhow!(
                        "Column `{name}` does not exist in the row provided (implement `Default` for {} or get all the columns)",
                        stringify!(#struct_name),
                    );
                    for (__i__, __n__) in row.names().iter().enumerate() {
                        let Some(__v__) = row.value(__i__) else { break };
                        #[allow(unreachable_patterns)]
                        match __n__.as_bytes() {
                            #(#field_assignment_holder)*
                            _ => {},
                        }
                    }
                    Ok(#struct_name {
                        #(#create_result,)*
                        #(#remaining,)*
                    })
                }
            }
        },
    )
}
