use crate::{TableMetadata, encode_column_ref::encode_column_ref};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, spanned::Spanned};

pub(crate) fn column_trait(table: &TableMetadata) -> (Ident, TokenStream) {
    let struct_name = &table.item.ident;
    let trait_name = Ident::new(&format!("{}ColumnTrait", struct_name), table.item.span());
    let columns: Vec<_> = table
        .columns
        .iter()
        .map(|column| {
            (
                column.ident.clone(),
                encode_column_ref(column, table.name.to_string(), table.schema.to_string()),
            )
        })
        .collect();
    let columns_fields_declarations = columns.iter().map(|(name, _)| {
        quote! {
            #[allow(non_upper_case_globals)]
            const #name: ::tank::ColumnRef;
        }
    });
    let columns_fields_definitions = columns.iter().map(|(name, column_ref)| {
        quote! {
            const #name: ::tank::ColumnRef = #column_ref;
        }
    });
    (
        trait_name.clone(),
        quote! {
            pub trait #trait_name {
                #(#columns_fields_declarations)*
            }
            impl #trait_name for #struct_name {
                #(#columns_fields_definitions)*
            }
        },
    )
}
