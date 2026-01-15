use crate::{QueryMetadata, truncate_long};
use std::fmt::{self, Display, Write};

#[derive(Default, Clone, Debug)]
pub struct RawQuery {
    pub(crate) value: String,
    pub(crate) metadata: QueryMetadata,
}

impl RawQuery {
    pub fn new(value: String) -> Self {
        Self {
            value,
            metadata: Default::default(),
        }
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(String::with_capacity(capacity))
    }
    pub fn buffer(&mut self) -> &mut String {
        &mut self.value
    }
    pub fn as_str(&self) -> &str {
        &self.value
    }
    pub fn push_str(&mut self, s: &str) {
        self.value.push_str(s);
    }
    pub fn push(&mut self, c: char) {
        self.value.push(c);
    }
    pub fn len(&self) -> usize {
        self.value.len()
    }
    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
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

impl Display for RawQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", truncate_long!(self.value))
    }
}
