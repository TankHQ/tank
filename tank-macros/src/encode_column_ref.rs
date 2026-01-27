use crate::decode_column::ColumnMetadata;
use proc_macro2::TokenStream;
use quote::quote;

pub fn encode_column_ref(metadata: &ColumnMetadata, table: String, schema: String) -> TokenStream {
    let name = &metadata.name;
    quote! {
        ::tank::ColumnRef {
            name: ::std::borrow::Cow::Borrowed(#name),
            table: ::std::borrow::Cow::Borrowed(#table),
            schema: ::std::borrow::Cow::Borrowed(#schema),
        }
    }
}
