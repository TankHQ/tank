use crate::QueryMetadata;
use bson::Document;
use std::fmt::{self, Write};

#[derive(Clone, Debug)]
pub enum QueryBuffer {
    String(String),
    Json(Document),
}

impl QueryBuffer {
    pub fn len(&self) -> usize {
        match self {
            QueryBuffer::String(v) => v.len(),
            QueryBuffer::Json(..) => 0,
        }
    }
    pub fn cast_string(&mut self) -> &mut String {
        if !matches!(self, QueryBuffer::String(..)) {
            *self = QueryBuffer::String(Default::default());
        }
        let QueryBuffer::String(string) = self else {
            unreachable!()
        };
        string
    }
    pub fn cast_json(&mut self) -> &mut Document {
        if !matches!(self, QueryBuffer::Json(..)) {
            *self = QueryBuffer::Json(Default::default());
        }
        let QueryBuffer::Json(document) = self else {
            unreachable!()
        };
        document
    }
}

impl Default for QueryBuffer {
    fn default() -> Self {
        QueryBuffer::String(Default::default())
    }
}

#[derive(Default, Clone, Debug)]
pub struct RawQuery {
    pub(crate) value: QueryBuffer,
    pub(crate) metadata: QueryMetadata,
}

impl RawQuery {
    pub fn new(value: String) -> Self {
        Self {
            value: QueryBuffer::String(value),
            metadata: Default::default(),
        }
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(String::with_capacity(capacity))
    }
    pub fn buffer(&mut self) -> &mut String {
        self.value.cast_string()
    }
    pub fn as_str(&self) -> &str {
        match &self.value {
            QueryBuffer::String(v) => v,
            QueryBuffer::Json(..) => "",
        }
    }
    pub fn push_str(&mut self, s: &str) {
        self.value.cast_string().push_str(s);
    }
    pub fn push(&mut self, c: char) {
        self.value.cast_string().push(c);
    }
    pub fn len(&self) -> usize {
        self.value.len()
    }
    pub fn is_empty(&self) -> bool {
        match &self.value {
            QueryBuffer::String(v) => v.is_empty(),
            QueryBuffer::Json(..) => true,
        }
    }
    pub fn metadata(&self) -> &QueryMetadata {
        &self.metadata
    }
    pub fn metadata_mut(&mut self) -> &mut QueryMetadata {
        &mut self.metadata
    }
}

impl Write for RawQuery {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.push_str(s);
        Ok(())
    }
    fn write_char(&mut self, c: char) -> fmt::Result {
        self.push(c);
        Ok(())
    }
}
