use crate::{DuckDBConnection, DuckDBDriver, ValueWrap, cbox::CBox};
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
    RowsAffected, Value, error_message_from_ptr, log_error, stream::Stream,
};
use time::PrimitiveDateTime;
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Column {
    vector: duckdb_vector,
    data: *const c_void,
    validity: *mut u64,
    id: DUCKDB_TYPE,
    width: u8,
    scale: u8,
}

impl Column {
    #[inline(always)]
    unsafe fn from_vector(vector: duckdb_vector) -> Self {
        unsafe {
            let logical_type = CBox(duckdb_vector_get_column_type(vector));
            let id = duckdb_get_type_id(*logical_type);
            let (width, scale) = if id == DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL {
                (
                    duckdb_decimal_width(*logical_type),
                    duckdb_decimal_scale(*logical_type),
                )
            } else {
                (0, 0)
            };
            Self {
                vector,
                data: duckdb_vector_get_data(vector) as *const c_void,
                validity: duckdb_vector_get_validity(vector),
                id,
                width,
                scale,
            }
        }
    }

    #[inline(always)]
    unsafe fn is_valid(&self, row: usize) -> bool {
        self.validity.is_null() || unsafe { (*self.validity.add(row >> 6) >> (row & 63)) & 1 != 0 }
    }

    unsafe fn empty_value(&self) -> Option<Value<'static>> {
        unsafe {
            Some(
                ValueWrap::try_from(*CBox(duckdb_vector_get_column_type(self.vector)))
                    .ok()?
                    .into(),
            )
        }
    }

    #[inline(always)]
    unsafe fn value<'a>(&self, row: usize) -> Option<Cow<'a, Value<'a>>> {
        unsafe {
            let data = self.data;
            if !data.is_null() && self.is_valid(row) {
                match self.id {
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
                        if let Some(scaled) = ValueWrap::to_decimal(
                            match self.width {
                                ..=4 => *(data as *const i16).add(row) as i128,
                                ..=9 => *(data as *const i32).add(row) as i128,
                                ..=18 => *(data as *const i64).add(row) as i128,
                                _ => ValueWrap::to_i128(&*(data as *const duckdb_hugeint).add(row)),
                            },
                            self.scale,
                        ) {
                            return Some(Cow::Owned(Value::Decimal(
                                Some(scaled),
                                self.width,
                                self.scale,
                            )));
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
                        if let Some(date) =
                            ValueWrap::to_date((*(data as *const duckdb_date).add(row)).days)
                        {
                            return Some(Cow::Owned(Value::Date(Some(date))));
                        }
                    }
                    DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => {
                        if let Some(timestamp) = ValueWrap::to_timestamp(
                            (*(data as *const duckdb_timestamp).add(row)).micros,
                        ) {
                            return Some(Cow::Owned(Value::Timestamp(Some(timestamp))));
                        }
                    }
                    _ => {}
                }
            }
            match self.decode(row) {
                Some(v) => Some(Cow::Owned(v)),
                None => Some(Cow::Owned(Value::Null)),
            }
        }
    }

    #[inline(never)]
    unsafe fn decode<'a>(&self, row: usize) -> Option<Value<'a>> {
        unsafe {
            let data = self.data;
            let id = self.id;
            if !self.is_valid(row)
                || (data.is_null()
                    && !matches!(
                        id,
                        DUCKDB_TYPE_DUCKDB_TYPE_ARRAY | DUCKDB_TYPE_DUCKDB_TYPE_STRUCT
                    ))
            {
                return self.empty_value();
            }
            Some(match id {
                DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL => Value::Null,
                DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => Value::Int8(Some(*(data as *const i8).add(row))),
                DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => {
                    Value::Int16(Some(*(data as *const i16).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => Value::Int128(Some(
                    ValueWrap::to_i128(&*(data as *const duckdb_hugeint).add(row)).into(),
                )),
                DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => {
                    Value::UInt8(Some(*(data as *const u8).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => {
                    Value::UInt16(Some(*(data as *const u16).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => {
                    Value::UInt32(Some(*(data as *const u32).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => {
                    Value::UInt64(Some(*(data as *const u64).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT => {
                    let v = &*(data as *const duckdb_uhugeint).add(row);
                    Value::UInt128(Some(((v.upper as u128) << 64 | v.lower as u128).into()))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => {
                    Value::Float32(Some(*(data as *const f32).add(row)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_TIME
                | DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS
                | DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => {
                    Value::Time(Some(ValueWrap::to_time_ns(match id {
                        DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS => *(data as *const i64).add(row),
                        DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => {
                            ((*(data as *const u64).add(row) >> 24) as i64).saturating_mul(1_000)
                        }
                        _ => (*(data as *const i64).add(row)).saturating_mul(1_000),
                    })?))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => {
                    Value::Timestamp(Some(ValueWrap::to_timestamp(
                        *(data as *const i64).add(row),
                    )?))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => Value::Timestamp(Some(
                    ValueWrap::to_timestamp((*(data as *const i64).add(row)).saturating_mul(1_000_000))?,
                )),
                DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => Value::Timestamp(Some(
                    ValueWrap::to_timestamp((*(data as *const i64).add(row)).saturating_mul(1_000))?,
                )),
                DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => {
                    let raw = *(data as *const i64).add(row);
                    let days = i32::try_from(raw.div_euclid(86_400_000_000_000)).ok()?;
                    Value::Timestamp(Some(PrimitiveDateTime::new(
                        ValueWrap::to_date(days)?,
                        ValueWrap::to_time_ns(raw.rem_euclid(86_400_000_000_000))?,
                    )))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => {
                    let v = *(data as *const duckdb_interval).add(row);
                    Value::Interval(Some(Interval::new(v.months, v.days, v.micros * 1_000)))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_ENUM => {
                    let logical_type = CBox(duckdb_vector_get_column_type(self.vector));
                    let idx = match duckdb_enum_internal_type(*logical_type) {
                        DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => *(data as *const u8).add(row) as u64,
                        DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => *(data as *const u16).add(row) as u64,
                        _ => *(data as *const u32).add(row) as u64,
                    };
                    let val = CBox(duckdb_enum_dictionary_value(*logical_type, idx));
                    if val.is_null() {
                        return None;
                    }
                    Value::Varchar(Some(Cow::Owned(
                        CStr::from_ptr(*val).to_str().ok()?.to_owned(),
                    )))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_BLOB => Value::Blob(Some(Cow::Borrowed(
                    ValueWrap::bytes_slice(&*(data as *const duckdb_string_t).add(row)),
                ))),
                DUCKDB_TYPE_DUCKDB_TYPE_UUID => {
                    let v = &*(data as *const duckdb_hugeint).add(row);
                    Value::Uuid(Some(Uuid::from_u64_pair(
                        v.upper as u64 ^ (1 << 63),
                        v.lower,
                    )))
                }
                DUCKDB_TYPE_DUCKDB_TYPE_LIST | DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => {
                    let (Value::List(_, t) | Value::Array(_, t)) = self.empty_value()? else {
                        return None;
                    };
                    let (child, begin, len, ctor): (_, _, _, fn(_, _) -> _) =
                        if id == DUCKDB_TYPE_DUCKDB_TYPE_LIST {
                            let e = *(data as *const duckdb_list_entry).add(row);
                            (
                                duckdb_list_vector_get_child(self.vector),
                                e.offset as usize,
                                e.length as usize,
                                Value::List,
                            )
                        } else {
                            (
                                duckdb_array_vector_get_child(self.vector),
                                row * t.len as usize,
                                t.len as usize,
                                Value::Array,
                            )
                        };
                    let col = Self::from_vector(child);
                    ctor(
                        Some(
                            (begin..begin + len)
                                .map(|i| col.value(i).map(Cow::into_owned))
                                .collect::<Option<Box<[_]>>>()?,
                        ),
                        t,
                    )
                }
                DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => {
                    let Value::Struct(_, struct_type) = self.empty_value()? else {
                        return None;
                    };
                    Value::Struct(
                        Some(
                            struct_type
                                .fields
                                .iter()
                                .enumerate()
                                .map(|(i, (name, _))| {
                                    let child = duckdb_struct_vector_get_child(self.vector, i as _);
                                    Some((
                                        name.clone(),
                                        Self::from_vector(child).value(row)?.into_owned(),
                                    ))
                                })
                                .collect::<Option<Box<[_]>>>()?,
                        ),
                        struct_type,
                    )
                }
                DUCKDB_TYPE_DUCKDB_TYPE_MAP => {
                    let Value::Map(_, map_type) = self.empty_value()? else {
                        return None;
                    };
                    let e = *(data as *const duckdb_list_entry).add(row);
                    let entries = duckdb_list_vector_get_child(self.vector);
                    let keys = Self::from_vector(duckdb_struct_vector_get_child(entries, 0));
                    let values = Self::from_vector(duckdb_struct_vector_get_child(entries, 1));
                    Value::Map(
                        Some(Box::new(
                            (e.offset as usize..(e.offset + e.length) as usize)
                                .map(|i| {
                                    Some((
                                        keys.value(i)?.into_owned(),
                                        values.value(i)?.into_owned(),
                                    ))
                                })
                                .collect::<Option<HashMap<_, _>>>()?,
                        )),
                        map_type,
                    )
                }
                _ => return None,
            })
        }
    }
}

// Caching each column's `data` and `validity` pointers per chunk avoids three C FFI calls per cell in `DuckDBRow::value`.
#[derive(Debug)]
pub(crate) struct Batch {
    chunk: CBox<duckdb_data_chunk>,
    labels: RowLabels,
    columns: Vec<Column>,
}

unsafe impl Send for Batch {}
unsafe impl Sync for Batch {}

/// A native row view borrowing a DuckDB data chunk shared with its siblings.
pub struct DuckDBRow {
    batch: Arc<Batch>,
    row: usize,
}

impl Debug for DuckDBRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDBRow")
            .field("labels", &self.batch.labels)
            .field("len", &self.len())
            .finish()
    }
}

impl Row for DuckDBRow {
    #[inline(always)]
    fn labels(&self) -> &[String] {
        &self.batch.labels
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.batch.columns.len()
    }

    #[inline(always)]
    fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
        unsafe { self.batch.columns.get(index)?.value(self.row) }
    }

    fn into_owned(self) -> RowOwned {
        RowOwned::new(
            self.batch.labels.clone(),
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
            batch: connection.batch.take(),
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
        self.row = 0;
        self.size = 0;
        self.result = None;
        self.extracted = None;
        if let Some(mut batch) = self.batch.take()
            && let Some(inner) = Arc::get_mut(&mut batch)
        {
            inner.chunk = CBox::null();
            self.connection.batch = Some(batch);
        }
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
        let _ = self
            .finish()
            .context("While cleaning up DuckDB execution")
            .inspect_err(log_error);
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
                        self.row = 0;
                        self.size = 0;
                        self.result = None;
                        continue;
                    }
                    let size = duckdb_data_chunk_get_size(*chunk) as usize;
                    if size == 0 {
                        continue;
                    }
                    let batch = match self.batch.as_mut().and_then(Arc::get_mut) {
                        Some(batch) => batch,
                        None => {
                            let batch = self.batch.insert(Arc::new(Batch {
                                chunk: CBox::null(),
                                labels: Arc::from([]),
                                columns: Vec::new(),
                            }));
                            Arc::get_mut(batch).unwrap_unchecked()
                        }
                    };
                    if self.size != 0 {
                        for (i, col) in batch.columns.iter_mut().enumerate() {
                            let vector = duckdb_data_chunk_get_vector(*chunk, i as _);
                            col.vector = vector;
                            col.data = duckdb_vector_get_data(vector) as *const c_void;
                            col.validity = duckdb_vector_get_validity(vector);
                        }
                    } else {
                        let cols = duckdb_column_count(&mut **result) as usize;
                        if batch.labels.len() != cols
                            || !(0..cols).all(|i| {
                                let ptr = duckdb_column_name(&mut **result, i as _);
                                !ptr.is_null()
                                    && batch.labels[i].as_bytes() == CStr::from_ptr(ptr).to_bytes()
                            })
                        {
                            batch.labels = (0..cols)
                                .map(|i| {
                                    let ptr = duckdb_column_name(&mut **result, i as _);
                                    if ptr.is_null() {
                                        String::new()
                                    } else {
                                        CStr::from_ptr(ptr).to_string_lossy().into_owned()
                                    }
                                })
                                .collect();
                        }
                        batch.columns.clear();
                        batch.columns.extend((0..cols).map(|i| {
                            Column::from_vector(duckdb_data_chunk_get_vector(*chunk, i as _))
                        }));
                    }
                    batch.chunk = chunk;
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
        let _ = self
            .finish()
            .context("While dropping DuckDB execution")
            .inspect_err(log_error);
    }
}
