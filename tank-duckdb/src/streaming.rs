use crate::{DuckDBConnection, DuckDBDriver, ValueWrap, cbox::CBox};
use anyhow::anyhow;
use libduckdb_sys::*;
use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::{CStr, CString, c_void},
    fmt::{self, Debug, Formatter},
    mem,
    pin::Pin,
    str,
    sync::Arc,
    task::{Context, Poll},
};
use tank_core::{
    Error, ErrorContext, Interval, Query, QueryResult, Result, Row, RowLabels, RowOwned,
    RowsAffected, Value, ValueOwned, decimal_from_scaled, error_message_from_ptr, stream::Stream,
};
use time::PrimitiveDateTime;
use uuid::Uuid;

pub(crate) type RowSchema = Arc<(RowLabels, Box<[(ValueOwned, DUCKDB_TYPE)]>)>;
pub(crate) type Batch = (Box<[VectorNode]>, RowSchema, CBox<duckdb_data_chunk>);

pub(crate) struct VectorNode {
    id: DUCKDB_TYPE,
    width: u8,
    scale: u8,
    data: *const c_void,
    validity: *mut u64,
    dict: Box<[Box<str>]>,
    children: Box<[VectorNode]>,
}

// The pointers are owned by the chunk the whole batch shares.
unsafe impl Send for VectorNode {}
unsafe impl Sync for VectorNode {}

impl VectorNode {
    fn from_vector(vector: duckdb_vector, kind: DUCKDB_TYPE, value_t: &Value<'_>) -> Result<Self> {
        unsafe {
            let mut dict = Box::new([]) as Box<[Box<str>]>;
            let id = if kind == DUCKDB_TYPE_DUCKDB_TYPE_ENUM {
                let logical_type = CBox(duckdb_vector_get_column_type(vector));
                dict = (0..duckdb_enum_dictionary_size(*logical_type))
                    .map(|i| {
                        let value = CBox(duckdb_enum_dictionary_value(*logical_type, i as _));
                        if value.is_null() {
                            return Err(anyhow!("Missing DuckDB ENUM dictionary value"));
                        }
                        Ok(CStr::from_ptr(*value).to_str()?.into())
                    })
                    .collect::<Result<Box<[_]>>>()?;
                duckdb_enum_internal_type(*logical_type)
            } else {
                kind
            };
            let (width, scale) = match *value_t {
                Value::Decimal(_, w, s) => (w, s),
                _ => (0, 0),
            };
            let child_node = |child, elem_t: &Value<'_>| {
                let child_id = if matches!(
                    elem_t,
                    Value::Time(..) | Value::Timestamp(..) | Value::Unknown(..)
                ) {
                    duckdb_get_type_id(*CBox(duckdb_vector_get_column_type(child)))
                } else {
                    DUCKDB_TYPE_DUCKDB_TYPE_INVALID
                };
                Self::from_vector(child, child_id, elem_t)
            };
            let children = match value_t {
                Value::List(_, t) => Box::new([child_node(
                    duckdb_list_vector_get_child(vector),
                    &t.element,
                )?]),
                Value::Array(_, t) => Box::new([child_node(
                    duckdb_array_vector_get_child(vector),
                    &t.element,
                )?]),
                Value::Struct(_, t) => t
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(i, (_, field_t))| {
                        child_node(duckdb_struct_vector_get_child(vector, i as _), field_t)
                    })
                    .collect::<Result<Box<[_]>>>()?,
                Value::Map(_, t) => {
                    let entries = duckdb_list_vector_get_child(vector);
                    Box::new([
                        child_node(duckdb_struct_vector_get_child(entries, 0), &t.key)?,
                        child_node(duckdb_struct_vector_get_child(entries, 1), &t.value)?,
                    ])
                }
                _ => Box::new([]),
            };
            Ok(Self {
                id,
                width,
                scale,
                data: duckdb_vector_get_data(vector),
                validity: duckdb_vector_get_validity(vector),
                dict,
                children,
            })
        }
    }

    #[inline(always)]
    fn is_valid(&self, row: usize) -> bool {
        self.validity.is_null() || unsafe { (*self.validity.add(row >> 6) >> (row & 63)) & 1 != 0 }
    }

    #[inline(never)]
    fn extract<'a>(&'a self, row: usize, value_t: &Value<'a>) -> Result<Value<'a>> {
        unsafe {
            let data = self.data;
            if !self.is_valid(row)
                || (data.is_null() && !matches!(value_t, Value::Array(..) | Value::Struct(..)))
            {
                return Ok(if matches!(value_t, Value::Unknown(..)) {
                    Value::Varchar(None)
                } else {
                    value_t.clone()
                });
            }
            Ok(match value_t {
                Value::Null => Value::Null,
                Value::Boolean(..) => Value::Boolean(Some(*(data as *const bool).add(row))),
                Value::Int8(..) => Value::Int8(Some(*(data as *const i8).add(row))),
                Value::Int16(..) => Value::Int16(Some(*(data as *const i16).add(row))),
                Value::Int32(..) => Value::Int32(Some(*(data as *const i32).add(row))),
                Value::Int64(..) => Value::Int64(Some(*(data as *const i64).add(row))),
                Value::Int128(..) => Value::Int128(Some(
                    ValueWrap::to_i128(&*(data as *const duckdb_hugeint).add(row)).into(),
                )),
                Value::UInt8(..) => Value::UInt8(Some(*(data as *const u8).add(row))),
                Value::UInt16(..) => Value::UInt16(Some(*(data as *const u16).add(row))),
                Value::UInt32(..) => Value::UInt32(Some(*(data as *const u32).add(row))),
                Value::UInt64(..) => Value::UInt64(Some(*(data as *const u64).add(row))),
                Value::UInt128(..) => {
                    let v = &*(data as *const duckdb_uhugeint).add(row);
                    Value::UInt128(Some(((v.upper as u128) << 64 | v.lower as u128).into()))
                }
                Value::Float32(..) => Value::Float32(Some(*(data as *const f32).add(row))),
                Value::Float64(..) => Value::Float64(Some(*(data as *const f64).add(row))),
                Value::Decimal(_, width, scale) => {
                    let num = match *width {
                        ..=4 => *(data as *const i16).add(row) as i128,
                        ..=9 => *(data as *const i32).add(row) as i128,
                        ..=18 => *(data as *const i64).add(row) as i128,
                        _ => ValueWrap::to_i128(&*(data as *const duckdb_hugeint).add(row)),
                    };
                    Value::Decimal(
                        Some(decimal_from_scaled(num, *scale as u32)?),
                        *width,
                        *scale,
                    )
                }
                Value::Date(..) => Value::Date(Some(ValueWrap::to_date(
                    (*(data as *const duckdb_date).add(row)).days,
                )?)),
                Value::Time(..) => Value::Time(Some(ValueWrap::to_time_ns(match self.id {
                    DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS => *(data as *const i64).add(row),
                    DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => {
                        ((*(data as *const u64).add(row) >> 24) as i64).saturating_mul(1_000)
                    }
                    _ => (*(data as *const i64).add(row)).saturating_mul(1_000),
                })?)),
                Value::Timestamp(..) => {
                    let raw = *(data as *const i64).add(row);
                    Value::Timestamp(Some(match self.id {
                        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => {
                            ValueWrap::to_timestamp(raw.saturating_mul(1_000_000))?
                        }
                        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => {
                            ValueWrap::to_timestamp(raw.saturating_mul(1_000))?
                        }
                        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => {
                            let days = i32::try_from(raw.div_euclid(86_400_000_000_000))
                                .context("While extracting a timestamp")?;
                            PrimitiveDateTime::new(
                                ValueWrap::to_date(days)?,
                                ValueWrap::to_time_ns(raw.rem_euclid(86_400_000_000_000))?,
                            )
                        }
                        _ => ValueWrap::to_timestamp(raw)?,
                    }))
                }
                Value::Interval(..) => {
                    let v = *(data as *const duckdb_interval).add(row);
                    Value::Interval(Some(Interval::new(v.months, v.days, v.micros * 1_000)))
                }
                Value::Varchar(..) => {
                    Value::Varchar(Some(Cow::Borrowed(str::from_utf8_unchecked(
                        ValueWrap::bytes_slice(&*(data as *const duckdb_string_t).add(row)),
                    ))))
                }
                Value::Unknown(..) => {
                    let idx = match self.id {
                        DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => *(data as *const u8).add(row) as usize,
                        DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => {
                            *(data as *const u16).add(row) as usize
                        }
                        _ => *(data as *const u32).add(row) as usize,
                    };
                    Value::Varchar(Some(Cow::Borrowed(
                        self.dict.get(idx).map(|v| v.as_ref()).unwrap_or_default(),
                    )))
                }
                Value::Blob(..) => Value::Blob(Some(Cow::Borrowed(ValueWrap::bytes_slice(
                    &*(data as *const duckdb_string_t).add(row),
                )))),
                Value::Uuid(..) => {
                    let v = &*(data as *const duckdb_hugeint).add(row);
                    Value::Uuid(Some(Uuid::from_u64_pair(
                        v.upper as u64 ^ (1 << 63),
                        v.lower,
                    )))
                }
                Value::List(_, t) | Value::Array(_, t) => {
                    let (begin, len, ctor): (_, _, fn(_, _) -> _) = match value_t {
                        Value::List(..) => {
                            let e = *(data as *const duckdb_list_entry).add(row);
                            (e.offset as usize, e.length as usize, Value::List)
                        }
                        _ => (row * t.len as usize, t.len as usize, Value::Array),
                    };
                    let child = self
                        .children
                        .first()
                        .context("Missing DuckDB child vector")?;
                    ctor(
                        Some(
                            (begin..begin + len)
                                .map(|i| child.extract(i, &t.element))
                                .collect::<Result<Box<[_]>>>()?,
                        ),
                        t.clone(),
                    )
                }
                Value::Struct(_, struct_type) => Value::Struct(
                    Some(
                        struct_type
                            .fields
                            .iter()
                            .zip(&self.children)
                            .map(|((name, field_t), child)| {
                                Ok((name.clone(), child.extract(row, field_t)?))
                            })
                            .collect::<Result<Box<[_]>>>()?,
                    ),
                    struct_type.clone(),
                ),
                Value::Map(_, map_type) => {
                    let e = *(data as *const duckdb_list_entry).add(row);
                    let [keys, values] = &*self.children else {
                        return Err(anyhow!("Missing DuckDB MAP child vectors"));
                    };
                    Value::Map(
                        Some(Box::new(
                            (e.offset as usize..(e.offset + e.length) as usize)
                                .map(|i| {
                                    Ok((
                                        keys.extract(i, &map_type.key)?,
                                        values.extract(i, &map_type.value)?,
                                    ))
                                })
                                .collect::<Result<HashMap<_, _>>>()?,
                        )),
                        map_type.clone(),
                    )
                }
                _ => return Err(anyhow!("Unsupported DuckDB column type: {value_t:?}")),
            })
        }
    }
}

/// A native row view borrowing a DuckDB data chunk shared with its siblings.
pub struct DuckDBRow {
    batch: Arc<Batch>,
    row: usize,
}

impl Debug for DuckDBRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDBRow")
            .field("labels", &(self.batch.1).0)
            .field("len", &self.len())
            .finish()
    }
}

impl DuckDBRow {
    /// Slow path for nulls, temporal variants, and nested kinds.
    #[cold]
    #[inline(never)]
    fn decode<'a>(&'a self, vector: &'a VectorNode, index: usize) -> Option<Value<'a>> {
        unsafe {
            vector
                .extract(self.row, &(self.batch.1).1.get_unchecked(index).0)
                .ok()
        }
    }
}

impl Row for DuckDBRow {
    #[inline(always)]
    fn labels(&self) -> &[String] {
        &(self.batch.1).0
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.batch.0.len()
    }

    #[inline(always)]
    fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
        if index >= self.batch.0.len() {
            return None;
        }
        let row = self.row;
        unsafe {
            let vector = self.batch.0.get_unchecked(index);
            let data = vector.data;
            if !data.is_null() && vector.is_valid(row) {
                match vector.id {
                    DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => {
                        return Some(Cow::Owned(Value::Boolean(Some(
                            *(data as *const bool).add(row),
                        ))));
                    }
                    DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => {
                        return Some(Cow::Owned(Value::Int32(Some(
                            *(data as *const i32).add(row),
                        ))));
                    }
                    DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => {
                        return Some(Cow::Owned(Value::Int64(Some(
                            *(data as *const i64).add(row),
                        ))));
                    }
                    DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => {
                        return Some(Cow::Owned(Value::Float64(Some(
                            *(data as *const f64).add(row),
                        ))));
                    }
                    DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => {
                        let (width, scale) = (vector.width, vector.scale);
                        if let Ok(scaled) = decimal_from_scaled(
                            match width {
                                ..=4 => *(data as *const i16).add(row) as i128,
                                ..=9 => *(data as *const i32).add(row) as i128,
                                ..=18 => *(data as *const i64).add(row) as i128,
                                _ => ValueWrap::to_i128(&*(data as *const duckdb_hugeint).add(row)),
                            },
                            scale as u32,
                        ) {
                            return Some(Cow::Owned(Value::Decimal(Some(scaled), width, scale)));
                        }
                    }
                    DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => {
                        return Some(Cow::Owned(Value::Varchar(Some(Cow::Borrowed(
                            str::from_utf8_unchecked(ValueWrap::bytes_slice(
                                &*(data as *const duckdb_string_t).add(row),
                            )),
                        )))));
                    }
                    DUCKDB_TYPE_DUCKDB_TYPE_DATE => {
                        if let Ok(date) =
                            ValueWrap::to_date((*(data as *const duckdb_date).add(row)).days)
                        {
                            return Some(Cow::Owned(Value::Date(Some(date))));
                        }
                    }
                    DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => {
                        if let Ok(timestamp) = ValueWrap::to_timestamp(
                            (*(data as *const duckdb_timestamp).add(row)).micros,
                        ) {
                            return Some(Cow::Owned(Value::Timestamp(Some(timestamp))));
                        }
                    }
                    _ => {}
                }
            }
            match self.decode(vector, index) {
                Some(v) => Some(Cow::Owned(v)),
                None => Some(Cow::Owned(Value::Null)),
            }
        }
    }

    fn into_owned(self) -> RowOwned {
        RowOwned::new(
            (self.batch.1).0.clone(),
            (0..self.len())
                .map(|i| {
                    unsafe { self.value(i).unwrap_unchecked() }
                        .into_owned()
                        .into_owned()
                })
                .collect(),
        )
    }
}

pub struct DuckDBStream<'s, Q: AsMut<Query<DuckDBDriver>>> {
    // Drop dependent native resources before the query and connection borrow.
    batch: Option<Arc<Batch>>,
    row: usize,
    size: usize,
    result: Option<(CBox<duckdb_result>, RowSchema)>,
    prepared: Option<CBox<duckdb_prepared_statement>>,
    extracted: Option<CBox<duckdb_extracted_statements>>,
    query: Q,
    connection: &'s mut DuckDBConnection,
    count: u64,
    index: u64,
    finished: bool,
}

impl<'s, Q: AsMut<Query<DuckDBDriver>>> DuckDBStream<'s, Q> {
    #[inline(always)]
    pub(crate) fn new(connection: &'s mut DuckDBConnection, query: Q) -> Self {
        Self {
            batch: None,
            row: 0,
            size: 0,
            result: None,
            prepared: None,
            extracted: None,
            query,
            connection,
            count: 1,
            index: 0,
            finished: false,
        }
    }

    fn finish(&mut self) -> Result<()> {
        self.batch = None;
        self.row = 0;
        self.size = 0;
        self.result = None;
        self.prepared = None;
        self.extracted = None;
        if !self.finished {
            self.finished = true;
            self.query.as_mut().clear_bindings()?;
        }
        Ok(())
    }

    #[cold]
    #[inline(never)]
    fn fail(&mut self, error: Error) -> Error {
        let context = format!("While running the query:\n{}", self.query.as_mut());
        if let Err(cleanup) = self.finish() {
            log::error!("While cleaning up DuckDB execution: {cleanup:#}");
        }
        error.context(context)
    }

    #[cold]
    #[inline(never)]
    fn advance(&mut self) -> Result<Option<RowsAffected>> {
        unsafe {
            loop {
                if let Some((result, schema)) = &mut self.result {
                    let chunk = CBox(duckdb_fetch_chunk(**result));
                    if chunk.is_null() {
                        let error = duckdb_result_error(&mut **result);
                        if !error.is_null() {
                            return Err(Error::msg(
                                CStr::from_ptr(error).to_string_lossy().into_owned(),
                            ));
                        }
                        self.result = None;
                        self.prepared = None;
                        continue;
                    }
                    let size = duckdb_data_chunk_get_size(*chunk) as usize;
                    if size == 0 {
                        continue;
                    }
                    let make_node = |(i, (value_t, kind)): (usize, &(ValueOwned, DUCKDB_TYPE))| {
                        VectorNode::from_vector(
                            duckdb_data_chunk_get_vector(*chunk, i as _),
                            *kind,
                            value_t,
                        )
                    };
                    if let Some(batch) = self.batch.as_mut().and_then(Arc::get_mut)
                        && batch.0.len() == schema.1.len()
                    {
                        if !Arc::ptr_eq(&batch.1, schema) {
                            batch.1 = schema.clone();
                        }
                        for (dst, item) in batch.0.iter_mut().zip(schema.1.iter().enumerate()) {
                            *dst = make_node(item)?;
                        }
                        batch.2 = chunk;
                    } else {
                        let mut vectors = Vec::with_capacity(schema.1.len());
                        for item in schema.1.iter().enumerate() {
                            vectors.push(make_node(item)?);
                        }
                        self.batch = Some(Arc::new((
                            vectors.into_boxed_slice(),
                            schema.clone(),
                            chunk,
                        )));
                    }
                    self.row = 0;
                    self.size = size;
                    return Ok(None);
                }
                if !self.finished
                    && self.extracted.is_none()
                    && let Query::Raw(raw) = self.query.as_mut()
                {
                    let sql = CString::new(raw.0.as_bytes())?;
                    let mut extracted = CBox::null();
                    self.count = duckdb_extract_statements(
                        *self.connection.connection,
                        sql.as_ptr(),
                        &mut *extracted,
                    );
                    let error = duckdb_extract_statements_error(*extracted);
                    if !error.is_null() {
                        return Err(Error::msg(
                            CStr::from_ptr(error).to_string_lossy().into_owned(),
                        ));
                    }
                    self.extracted = Some(extracted);
                }
                if self.finished || self.index == self.count {
                    self.finish()?;
                    return Ok(None);
                }
                let statement = if let Some(extracted) = &self.extracted {
                    let mut prepared = CBox::null();
                    if duckdb_prepare_extracted_statement(
                        *self.connection.connection,
                        **extracted,
                        self.index,
                        &mut *prepared,
                    ) != duckdb_state_DuckDBSuccess
                    {
                        return Err(Error::msg(
                            error_message_from_ptr(&duckdb_prepare_error(*prepared)).into_owned(),
                        ));
                    }
                    **self.prepared.insert(prepared)
                } else {
                    match self.query.as_mut() {
                        Query::Prepared(query) => *query.statement,
                        Query::Raw(_) => unreachable!(),
                    }
                };
                self.index += 1;
                let mut result = CBox(mem::zeroed());
                if duckdb_execute_prepared_streaming(statement, &mut *result)
                    != duckdb_state_DuckDBSuccess
                {
                    return Err(Error::msg(
                        error_message_from_ptr(&duckdb_result_error(&mut *result)).into_owned(),
                    ));
                }
                if duckdb_result_return_type(*result)
                    == duckdb_result_type_DUCKDB_RESULT_TYPE_QUERY_RESULT
                {
                    let schema = if let Query::Prepared(query) = self.query.as_mut()
                        && let Some(schema) = &query.schema
                    {
                        schema.clone()
                    } else {
                        let cols = duckdb_column_count(&mut *result) as usize;
                        let names = (0..cols)
                            .map(|i| {
                                let ptr = duckdb_column_name(&mut *result, i as _);
                                if ptr.is_null() {
                                    String::new()
                                } else {
                                    CStr::from_ptr(ptr).to_string_lossy().into_owned()
                                }
                            })
                            .collect::<RowLabels>();
                        let mut columns = Vec::with_capacity(cols);
                        for i in 0..cols {
                            let logical_type =
                                CBox(duckdb_column_logical_type(&mut *result, i as _));
                            if logical_type.is_null() {
                                return Err(anyhow!("Missing DuckDB column type at index {i}"));
                            }
                            let kind = duckdb_get_type_id(*logical_type);
                            columns.push((ValueWrap::try_from(*logical_type)?.into(), kind));
                        }
                        let schema = Arc::new((names, columns.into_boxed_slice()));
                        if let Query::Prepared(query) = self.query.as_mut() {
                            query.schema = Some(schema.clone());
                        }
                        schema
                    };
                    self.result = Some((result, schema));
                } else {
                    let affected = RowsAffected {
                        rows_affected: Some(duckdb_rows_changed(&mut *result)),
                        ..Default::default()
                    };
                    drop(result);
                    self.prepared = None;
                    return Ok(Some(affected));
                }
            }
        }
    }
}

impl<Q: AsMut<Query<DuckDBDriver>>> Stream for DuckDBStream<'_, Q> {
    type Item = Result<QueryResult<DuckDBRow>>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            if this.row < this.size {
                let row = this.row;
                this.row = row + 1;
                return Poll::Ready(Some(Ok(QueryResult::Row(DuckDBRow {
                    batch: unsafe { this.batch.as_ref().unwrap_unchecked() }.clone(),
                    row,
                }))));
            }
            match this.advance() {
                Ok(Some(affected)) => {
                    return Poll::Ready(Some(Ok(QueryResult::Affected(affected))));
                }
                Ok(None) if this.row < this.size => {}
                Ok(None) => return Poll::Ready(None),
                Err(error) => return Poll::Ready(Some(Err(this.fail(error)))),
            }
        }
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.size - self.row;
        (remaining, (remaining > 0).then_some(remaining))
    }
}

impl<Q: AsMut<Query<DuckDBDriver>>> Drop for DuckDBStream<'_, Q> {
    fn drop(&mut self) {
        if let Err(e) = self.finish() {
            log::error!("{:#}", e.context("While dropping DuckDB execution"));
        }
    }
}
