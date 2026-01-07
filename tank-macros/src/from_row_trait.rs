use crate::TableMetadata;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, Type, spanned::Spanned};

pub(crate) fn from_row_trait(table: &TableMetadata) -> (Ident, TokenStream) {
    let item = &table.item;
    let struct_name = &item.ident;
    let trait_name = Ident::new(&format!("{struct_name}FromRowTrait"), item.span());
    let factory_name = Ident::new(&format!("{struct_name}FromRowFactory"), item.span());
    let fields_holder_declarations = table.columns.iter().map(|c| {
        let ident = &c.ident;
        let ty = &c.ty;
        quote! {
            let mut #ident: Option<#ty> = None;
        }
    });
    type AssignmentFn = dyn Fn(&Ident, &Type) -> TokenStream;
    type ProducerFn = Box<dyn Fn(&AssignmentFn) -> TokenStream>;
    let field_assignment = table
        .columns
        .iter()
        .map(|c| (c.ident.clone(), c.name.to_string(), c.ty.clone()))
        .map(|(ident, name, ty)| {
            Box::new(move |assign: &AssignmentFn| {
                let assign = assign(&ident, &ty);
                quote! {
                    if __n__ == #name {
                        #assign;
                    }
                }
            }) as ProducerFn
        })
        .reduce(|acc, cur| {
            Box::new(move |assign: &AssignmentFn| {
                let acc = acc(assign);
                let cur = cur(assign);
                quote!(#acc else #cur)
            }) as ProducerFn
        })
        .unwrap_or(Box::new(|_| TokenStream::new()));
    let create_result = table.columns.iter().map(|c| {
        let column = &c.name;
        let ident = &c.ident;
        quote! {
            #ident: #ident.ok_or(__make_error__(#column))?
        }
    });
    let remaining = item
        .fields
        .iter()
        .filter(|field| {
            table
                .columns
                .iter()
                .find(|c| c.ident == *field.ident.as_ref().unwrap())
                .is_none()
        })
        .map(|field| {
            let ident = field.ident.as_ref().unwrap();
            quote!(#ident: Default::default())
        });
    let create_result = quote! {
        #struct_name {
            #(#create_result,)*
            #(#remaining,)*
        }
    };
    let field_assignment_default = field_assignment(
        &|field, ty| quote!(result.#field = <#ty as ::tank::AsValue>::try_from_value(__v__)?),
    );
    let field_assignment_holder = field_assignment(
        &|field, ty| quote!(#field = Some(<#ty as ::tank::AsValue>::try_from_value(__v__)?)),
    );
    (
        factory_name.clone(),
        quote! {
            trait #trait_name {
                fn from_row(row: ::tank::RowLabeled) -> ::tank::Result<#struct_name>;
            }
            struct #factory_name<T>(std::marker::PhantomData<T>);
            impl<T: Default + Into<#struct_name>> #factory_name<T> {
                // Called when T has Default Trait
                fn from_row(row: ::tank::RowLabeled) -> ::tank::Result<#struct_name> {
                    let mut result = T::default().into();
                    for (__n__, __v__) in ::std::iter::zip(row.labels.iter(), row.values.into_iter())
                    {
                        #field_assignment_default
                    }
                    Ok(result)
                }
            }
            impl<T> #trait_name for #factory_name<T> {
                // Called when T doesn't have default trait
                fn from_row(row: ::tank::RowLabeled) -> ::tank::Result<#struct_name> {
                    #(#fields_holder_declarations)*
                    for (__n__, __v__) in ::std::iter::zip(row.labels.iter(), row.values.into_iter())
                    {
                        #field_assignment_holder
                    }
                    let __make_error__ = |name: &str| ::tank::Error::msg(format!(
                        "Column `{name}` does not exist in the row provided (implement `Default` for {} or get all the columns)",
                        stringify!(#struct_name),
                    ));
                    Ok(#create_result)
                }
            }
        },
    )
}
