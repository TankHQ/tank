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

pub(crate) type Batch = (Box<[VectorNode]>, RowLabels, CBox<duckdb_data_chunk>);

pub(crate) struct VectorNode {
    vector: duckdb_vector,
    data: *const c_void,
    validity: *mut u64,
    id: DUCKDB_TYPE,
    value_t: ValueOwned,
}

// The pointers are owned by the chunk the whole batch shares.
unsafe impl Send for VectorNode {}
unsafe impl Sync for VectorNode {}

impl VectorNode {
    #[inline(always)]
    fn new(vector: duckdb_vector, id: DUCKDB_TYPE, value_t: ValueOwned) -> Self {
        unsafe {
            Self {
                vector,
                data: duckdb_vector_get_data(vector),
                validity: duckdb_vector_get_validity(vector),
                id,
                value_t,
            }
        }
    }

    fn from_child(vector: duckdb_vector, value_t: &Value<'_>) -> Self {
        let id = if matches!(value_t, Value::Time(..) | Value::Timestamp(..)) {
            unsafe { duckdb_get_type_id(*CBox(duckdb_vector_get_column_type(vector))) }
        } else {
            DUCKDB_TYPE_DUCKDB_TYPE_INVALID
        };
        Self::new(vector, id, Value::Null)
    }

    #[inline(always)]
    fn is_valid(&self, row: usize) -> bool {
        self.validity.is_null() || unsafe { (*self.validity.add(row >> 6) >> (row & 63)) & 1 != 0 }
    }

    #[inline(never)]
    fn extract<'a>(&self, row: usize, value_t: &'a Value<'a>) -> Result<Value<'a>> {
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
                    let logical_type = CBox(duckdb_vector_get_column_type(self.vector));
                    let idx = match duckdb_enum_internal_type(*logical_type) {
                        DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => *(data as *const u8).add(row) as u64,
                        DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => *(data as *const u16).add(row) as u64,
                        _ => *(data as *const u32).add(row) as u64,
                    };
                    let val = CBox(duckdb_enum_dictionary_value(*logical_type, idx));
                    if val.is_null() {
                        return Err(anyhow!("Missing DuckDB ENUM dictionary value"));
                    }
                    Value::Varchar(Some(Cow::Owned(CStr::from_ptr(*val).to_str()?.to_owned())))
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
                    let (child, begin, len, ctor): (_, _, _, fn(_, _) -> _) = match value_t {
                        Value::List(..) => {
                            let e = *(data as *const duckdb_list_entry).add(row);
                            (
                                duckdb_list_vector_get_child(self.vector),
                                e.offset as usize,
                                e.length as usize,
                                Value::List,
                            )
                        }
                        _ => (
                            duckdb_array_vector_get_child(self.vector),
                            row * t.len as usize,
                            t.len as usize,
                            Value::Array,
                        ),
                    };
                    let node = Self::from_child(child, &t.element);
                    ctor(
                        Some(
                            (begin..begin + len)
                                .map(|i| node.extract(i, &t.element))
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
                            .enumerate()
                            .map(|(i, (name, field_t))| {
                                let child = duckdb_struct_vector_get_child(self.vector, i as _);
                                Ok((
                                    name.clone(),
                                    Self::from_child(child, field_t).extract(row, field_t)?,
                                ))
                            })
                            .collect::<Result<Box<[_]>>>()?,
                    ),
                    struct_type.clone(),
                ),
                Value::Map(_, map_type) => {
                    let e = *(data as *const duckdb_list_entry).add(row);
                    let entries = duckdb_list_vector_get_child(self.vector);
                    let keys = Self::from_child(
                        duckdb_struct_vector_get_child(entries, 0),
                        &map_type.key,
                    );
                    let values = Self::from_child(
                        duckdb_struct_vector_get_child(entries, 1),
                        &map_type.value,
                    );
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
            .field("labels", &self.batch.1)
            .field("len", &self.len())
            .finish()
    }
}

impl DuckDBRow {
    /// Slow path for nulls, temporal variants, and nested kinds.
    #[cold]
    #[inline(never)]
    fn decode<'a>(&'a self, vector: &'a VectorNode) -> Option<Value<'a>> {
        vector.extract(self.row, &vector.value_t).ok()
    }
}

impl Row for DuckDBRow {
    #[inline(always)]
    fn labels(&self) -> &[String] {
        &self.batch.1
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
                        if let Value::Decimal(_, width, scale) = vector.value_t
                            && let Ok(scaled) = decimal_from_scaled(
                                match width {
                                    ..=4 => *(data as *const i16).add(row) as i128,
                                    ..=9 => *(data as *const i32).add(row) as i128,
                                    ..=18 => *(data as *const i64).add(row) as i128,
                                    _ => ValueWrap::to_i128(
                                        &*(data as *const duckdb_hugeint).add(row),
                                    ),
                                },
                                scale as u32,
                            )
                        {
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
            match self.decode(vector) {
                Some(v) => Some(Cow::Owned(v)),
                None => Some(Cow::Owned(Value::Null)),
            }
        }
    }

    fn into_owned(self) -> RowOwned {
        RowOwned::new(
            self.batch.1.clone(),
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
    result: Option<CBox<duckdb_result>>,
    extracted: Option<CBox<duckdb_extracted_statements>>,
    query: Q,
    connection: &'s mut DuckDBConnection,
    count: u64,
    index: u64,
}

impl<'s, Q: AsMut<Query<DuckDBDriver>>> DuckDBStream<'s, Q> {
    #[inline(always)]
    pub(crate) fn new(connection: &'s mut DuckDBConnection, query: Q) -> Self {
        Self {
            batch: None,
            row: 0,
            size: 0,
            result: None,
            extracted: None,
            query,
            connection,
            count: 1,
            index: 0,
        }
    }

    fn finish(&mut self) -> Result<()> {
        self.batch = None;
        self.row = 0;
        self.size = 0;
        self.result = None;
        self.extracted = None;
        if self.count != 0 {
            self.count = 0;
            self.index = 0;
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
                if let Some(result) = &mut self.result {
                    let chunk = CBox(duckdb_fetch_chunk(**result));
                    if chunk.is_null() {
                        let error = duckdb_result_error(&mut **result);
                        if !error.is_null() {
                            return Err(Error::msg(
                                CStr::from_ptr(error).to_string_lossy().into_owned(),
                            ));
                        }
                        self.result = None;
                        continue;
                    }
                    let size = duckdb_data_chunk_get_size(*chunk) as usize;
                    if size == 0 {
                        continue;
                    }
                    let cols = duckdb_column_count(&mut **result) as usize;
                    let labels = (0..cols)
                        .map(|i| {
                            let ptr = duckdb_column_name(&mut **result, i as _);
                            if ptr.is_null() {
                                String::new()
                            } else {
                                CStr::from_ptr(ptr).to_string_lossy().into_owned()
                            }
                        })
                        .collect();
                    let vectors = (0..cols)
                        .map(|i| {
                            let logical_type =
                                CBox(duckdb_column_logical_type(&mut **result, i as _));
                            if logical_type.is_null() {
                                return Err(anyhow!("Missing DuckDB column type at index {i}"));
                            }
                            Ok(VectorNode::new(
                                duckdb_data_chunk_get_vector(*chunk, i as _),
                                duckdb_get_type_id(*logical_type),
                                ValueWrap::try_from(*logical_type)?.into(),
                            ))
                        })
                        .collect::<Result<Box<[_]>>>()?;
                    self.batch = Some(Arc::new((vectors, labels, chunk)));
                    self.row = 0;
                    self.size = size;
                    return Ok(None);
                }
                if self.count != 0
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
                if self.index == self.count {
                    self.finish()?;
                    return Ok(None);
                }
                let mut extracted_stmt = CBox::null();
                let statement = if let Some(extracted) = &self.extracted {
                    if duckdb_prepare_extracted_statement(
                        *self.connection.connection,
                        **extracted,
                        self.index,
                        &mut *extracted_stmt,
                    ) != duckdb_state_DuckDBSuccess
                    {
                        return Err(Error::msg(
                            error_message_from_ptr(&duckdb_prepare_error(*extracted_stmt))
                                .into_owned(),
                        ));
                    }
                    *extracted_stmt
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
                    != duckdb_result_type_DUCKDB_RESULT_TYPE_QUERY_RESULT
                {
                    return Ok(Some(RowsAffected {
                        rows_affected: Some(duckdb_rows_changed(&mut *result)),
                        ..Default::default()
                    }));
                }
                self.result = Some(result);
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
