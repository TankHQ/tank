use crate::decode_column::ColumnMetadata;
use proc_macro2::TokenStream;
use quote::quote;
use tank_core::{future::Either, quote_btree_map, quote_option};

pub fn encode_column_def(metadata: &ColumnMetadata, column_ref: TokenStream) -> TokenStream {
    let column_type = quote_btree_map(&metadata.column_type);
    let value = &metadata.value;
    let nullable = &metadata.nullable;
    let default = metadata
        .default
        .as_ref()
        .map_or(quote!(Default::default()), |v| quote!(#v.into()));
    let primary_key = &metadata.primary_key;
    let references = if let Some(Either::Left(tokens)) = &metadata.references {
        let tokens = tokens.clone();
        quote!(Some(#tokens))
    } else if let Some(Either::Right((table, column))) = &metadata.references {
        let (schema, table) = match table.rsplit_once('.') {
            Some((schema, table)) => (schema.to_string(), table.to_string()),
            _ => (String::new(), table.to_string()),
        };
        quote! {
            Some(::tank::ColumnRef {
                name: #column,
                table: #table,
                schema: #schema,
            })
        }
    } else {
        quote!(None)
    };
    let on_delete = quote_option(&metadata.on_delete);
    let on_update = quote_option(&metadata.on_update);
    let unique = &metadata.unique;
    let passive = &metadata.passive;
    let comment = &metadata.comment;
    quote! {
        ::tank::ColumnDef {
            column_ref: #column_ref,
            column_type: #column_type,
            value: #value,
            nullable: #nullable,
            default: #default,
            primary_key: #primary_key,
            references: #references,
            on_delete: #on_delete,
            on_update: #on_update,
            unique: #unique,
            passive: #passive,
            comment: #comment,
        }
    }
}
