use crate::TableMetadata;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, spanned::Spanned};

/// Converts fields to values, borrowing those whose reference implements `AsValue`.
pub(crate) fn field_value_trait(table: &TableMetadata) -> (Ident, TokenStream) {
    let item = &table.item;
    let struct_name = &item.ident;
    let trait_name = Ident::new(&format!("{struct_name}FieldValueTrait"), item.span());
    let factory_name = Ident::new(&format!("{struct_name}FieldValue"), item.span());
    (
        factory_name.clone(),
        quote! {
            trait #trait_name<T> {
                fn value(field: &T) -> ::tank::Value<'_>;
            }
            struct #factory_name<T>(::std::marker::PhantomData<T>);
            impl<T> #factory_name<T>
            where
                for<'a> &'a T: ::tank::AsValue,
            {
                #[inline(always)]
                fn value(field: &T) -> ::tank::Value<'_> {
                    ::tank::AsValue::as_value(field)
                }
            }
            impl<T: ::tank::AsValue + Clone> #trait_name<T> for #factory_name<T> {
                #[inline(always)]
                fn value(field: &T) -> ::tank::Value<'_> {
                    ::tank::AsValue::as_value(field.clone())
                }
            }
        },
    )
}
