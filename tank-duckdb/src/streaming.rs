use crate::{
    DuckDBConnection, DuckDBDriver, cbox::CBox, duckdb_bytes_to_slice, duckdb_date_to_date,
    duckdb_hugeint_to_i128, duckdb_interval_to_interval, duckdb_time_ns_to_time,
    duckdb_time_to_time, duckdb_uhugeint_to_u128, nanos_to_primitive_date_time,
    uuid_from_duckdb_hugeint,
};
use anyhow::anyhow;
use libduckdb_sys::*;
use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::{CStr, CString, c_void},
    fmt::{self, Debug, Formatter},
    mem,
    ptr, str,
    sync::Arc,
};
use tank_core::{
    Error, Query, QueryResult, Result, Row, RowLabels, RowOwned, RowsAffected, Value, ValueOwned,
    decimal_from_scaled, error_message_from_ptr,
};

struct VectorNode {
    id: DUCKDB_TYPE,
    data: *const c_void,
    validity: *mut u64,
    dict: Box<[Box<str>]>,
    children: Box<[VectorNode]>,
}

// The pointers are owned by the chunk the whole batch shares.
unsafe impl Send for VectorNode {}
unsafe impl Sync for VectorNode {}

impl VectorNode {
    fn from_vector(vector: duckdb_vector, value_t: &Value<'_>) -> Result<Self> {
        unsafe {
            let mut dict = Box::new([]) as Box<[Box<str>]>;
            let id = match value_t {
                Value::Boolean(..) => DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
                Value::Int32(..) => DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
                Value::Int64(..) => DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
                Value::Float64(..) => DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
                Value::Varchar(..) => DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
                // These need the physical type to pick the unit or the dictionary.
                Value::Time(..) | Value::Timestamp(..) | Value::Unknown(..) => {
                    let logical_type = CBox::new(
                        duckdb_vector_get_column_type(vector),
                        |mut p| duckdb_destroy_logical_type(&mut p),
                    );
                    let id = duckdb_get_type_id(*logical_type);
                    if id == DUCKDB_TYPE_DUCKDB_TYPE_ENUM {
                        let count = duckdb_enum_dictionary_size(*logical_type);
                        dict = (0..count)
                            .map(|i| {
                                let value = duckdb_enum_dictionary_value(*logical_type, i as _);
                                if value.is_null() {
                                    return Err(anyhow!("Missing DuckDB ENUM dictionary value"));
                                }
                                let text = CStr::from_ptr(value).to_str()?.into();
                                duckdb_free(value as _);
                                Ok(text)
                            })
                            .collect::<Result<Box<[_]>>>()?;
                        duckdb_enum_internal_type(*logical_type)
                    } else {
                        id
                    }
                }
                _ => DUCKDB_TYPE_DUCKDB_TYPE_INVALID,
            };
            let children = match value_t {
                Value::List(_, t) => Box::new([Self::from_vector(
                    duckdb_list_vector_get_child(vector),
                    &t.element,
                )?]),
                Value::Array(_, t) => Box::new([Self::from_vector(
                    duckdb_array_vector_get_child(vector),
                    &t.element,
                )?]),
                Value::Struct(_, t) => t
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(i, (_, field_t))| {
                        Self::from_vector(duckdb_struct_vector_get_child(vector, i as _), field_t)
                    })
                    .collect::<Result<Box<[_]>>>()?,
                Value::Map(_, t) => {
                    let entries = duckdb_list_vector_get_child(vector);
                    Box::new([
                        Self::from_vector(duckdb_struct_vector_get_child(entries, 0), &t.key)?,
                        Self::from_vector(duckdb_struct_vector_get_child(entries, 1), &t.value)?,
                    ])
                }
                _ => Box::new([]),
            };
            Ok(Self {
                id,
                data: duckdb_vector_get_data(vector),
                validity: duckdb_vector_get_validity(vector),
                dict,
                children,
            })
        }
    }

    #[inline(always)]
    fn is_valid(&self, row: usize) -> bool {
        self.validity.is_null() || unsafe { duckdb_validity_row_is_valid(self.validity, row as _) }
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
                    duckdb_hugeint_to_i128(&*(data as *const duckdb_hugeint).add(row)).into(),
                )),
                Value::UInt8(..) => Value::UInt8(Some(*(data as *const u8).add(row))),
                Value::UInt16(..) => Value::UInt16(Some(*(data as *const u16).add(row))),
                Value::UInt32(..) => Value::UInt32(Some(*(data as *const u32).add(row))),
                Value::UInt64(..) => Value::UInt64(Some(*(data as *const u64).add(row))),
                Value::UInt128(..) => Value::UInt128(Some(
                    duckdb_uhugeint_to_u128(&*(data as *const duckdb_uhugeint).add(row)).into(),
                )),
                Value::Float32(..) => Value::Float32(Some(*(data as *const f32).add(row))),
                Value::Float64(..) => Value::Float64(Some(*(data as *const f64).add(row))),
                Value::Decimal(_, width, scale) => {
                    let num = if *width <= 4 {
                        *(data as *const i16).add(row) as i128
                    } else if *width <= 9 {
                        *(data as *const i32).add(row) as i128
                    } else if *width <= 18 {
                        *(data as *const i64).add(row) as i128
                    } else {
                        duckdb_hugeint_to_i128(&*(data as *const duckdb_hugeint).add(row))
                    };
                    Value::Decimal(
                        Some(decimal_from_scaled(num, *scale as u32)?),
                        *width,
                        *scale,
                    )
                }
                Value::Date(..) => Value::Date(Some(duckdb_date_to_date(
                    (*(data as *const duckdb_date).add(row)).days,
                )?)),
                Value::Time(..) => Value::Time(Some(match self.id {
                    DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS => {
                        duckdb_time_ns_to_time(*(data as *const i64).add(row))?
                    }
                    DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => {
                        duckdb_time_to_time((*(data as *const u64).add(row) >> 24) as i64)?
                    }
                    _ => duckdb_time_to_time(*(data as *const i64).add(row))?,
                })),
                Value::Timestamp(..) => {
                    let raw = *(data as *const i64).add(row) as i128;
                    Value::Timestamp(Some(nanos_to_primitive_date_time(match self.id {
                        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => raw * 1_000_000_000,
                        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => raw * 1_000_000,
                        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => raw,
                        _ => raw * 1_000,
                    })?))
                }
                Value::Interval(..) => Value::Interval(Some(duckdb_interval_to_interval(
                    *(data as *const duckdb_interval).add(row),
                ))),
                Value::Varchar(..) => {
                    Value::Varchar(Some(Cow::Borrowed(str::from_utf8_unchecked(
                        duckdb_bytes_to_slice(&*(data as *const duckdb_string_t).add(row)),
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
                        self.dict
                            .get(idx)
                            .map(|v| v.as_ref())
                            .unwrap_or_default(),
                    )))
                }
                Value::Blob(..) => Value::Blob(Some(Cow::Borrowed(duckdb_bytes_to_slice(
                    &*(data as *const duckdb_string_t).add(row),
                )))),
                Value::Uuid(..) => Value::Uuid(Some(uuid_from_duckdb_hugeint(
                    &*(data as *const duckdb_hugeint).add(row),
                ))),
                Value::List(_, t) | Value::Array(_, t) => {
                    let (begin, len) = if matches!(value_t, Value::List(..)) {
                        let e = *(data as *const duckdb_list_entry).add(row);
                        (e.offset as usize, e.length as usize)
                    } else {
                        (row * t.len as usize, t.len as usize)
                    };
                    let child = self
                        .children
                        .first()
                        .ok_or_else(|| anyhow!("Missing DuckDB child vector"))?;
                    let values = (begin..begin + len)
                        .map(|i| child.extract(i, &t.element))
                        .collect::<Result<Box<[_]>>>()?;
                    if matches!(value_t, Value::List(..)) {
                        Value::List(Some(values), t.clone())
                    } else {
                        Value::Array(Some(values), t.clone())
                    }
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

/// Column names and logical types shared by every row of a result.
#[derive(Clone, Debug)]
pub(crate) struct RowSchema {
    pub(crate) names: RowLabels,
    pub(crate) types: Arc<[ValueOwned]>,
}

impl RowSchema {
    unsafe fn read(result: &mut duckdb_result) -> Result<Self> {
        unsafe {
            let cols = duckdb_column_count(result);
            let mut names = Vec::with_capacity(cols as usize);
            let mut types = Vec::with_capacity(cols as usize);
            for i in 0..cols {
                let name = duckdb_column_name(result, i);
                names.push(if name.is_null() {
                    String::new()
                } else {
                    CStr::from_ptr(name).to_str()?.to_owned()
                });
                let logical_type = CBox::new(duckdb_column_logical_type(result, i), |mut p| {
                    duckdb_destroy_logical_type(&mut p)
                });
                if logical_type.is_null() {
                    return Err(anyhow!("Missing DuckDB column type at index {i}"));
                }
                types.push(crate::extract::type_to_value(*logical_type)?);
            }
            Ok(Self {
                names: names.into(),
                types: types.into(),
            })
        }
    }
}

struct Batch {
    vectors: Box<[VectorNode]>,
    schema: RowSchema,
    // Keeps the buffers every vector points into alive.
    _chunk: CBox<duckdb_data_chunk>,
}

/// A native row view borrowing a DuckDB data chunk shared with its siblings.
pub struct DuckDBRow {
    batch: Arc<Batch>,
    row: usize,
}

impl Debug for DuckDBRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDBRow")
            .field("labels", &self.batch.schema.names)
            .field("len", &self.len())
            .finish()
    }
}

impl Row for DuckDBRow {
    #[inline(always)]
    fn labels(&self) -> &[String] {
        &self.batch.schema.names
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.batch.schema.types.len()
    }

    #[inline(always)]
    fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
        if index >= self.batch.vectors.len() {
            return None;
        }
        let row = self.row;
        unsafe {
            let vector = self.batch.vectors.get_unchecked(index);
            let data = vector.data;
            if vector.is_valid(row) && !data.is_null() {
                // Columns decoded on every row avoid the generic path.
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
                    DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => {
                        return Some(Cow::Owned(Value::Varchar(Some(Cow::Borrowed(
                            str::from_utf8_unchecked(duckdb_bytes_to_slice(
                                &*(data as *const duckdb_string_t).add(row),
                            )),
                        )))));
                    }
                    _ => {}
                }
            }
            match vector.extract(row, self.batch.schema.types.get_unchecked(index)) {
                Ok(value) => Some(Cow::Owned(value)),
                Err(_) => Some(Cow::Owned(Value::Null)),
            }
        }
    }

    fn into_owned(self) -> RowOwned {
        RowOwned::new(
            self.batch.schema.names.clone(),
            self.values().map(|v| v.into_owned().into_owned()).collect(),
        )
    }
}

/// The streaming result of one statement.
struct Rows {
    result: CBox<duckdb_result>,
    streaming: bool,
    schema: RowSchema,
    batch: Option<(DuckDBRow, usize)>,
}

impl Rows {
    fn new(result: CBox<duckdb_result>, schema: RowSchema) -> Self {
        let streaming = unsafe { duckdb_result_is_streaming(*result) };
        Self {
            result,
            streaming,
            schema,
            batch: None,
        }
    }
}

impl Rows {
    fn next_row(&mut self) -> Result<Option<DuckDBRow>> {
        loop {
            if let Some((current, size)) = &mut self.batch {
                let row = current.row;
                // Dates can hold values `time::Date` cannot represent, report that per row.
                for (vector, value_t) in current.batch.vectors.iter().zip(self.schema.types.iter()) {
                    if matches!(value_t, Value::Date(..)) && vector.is_valid(row) {
                        unsafe {
                            duckdb_date_to_date((*(vector.data as *const duckdb_date).add(row)).days)?;
                        }
                    }
                }
                let item = if row + 1 == *size {
                    match self.batch.take() {
                        Some((row, _)) => row,
                        None => return Ok(None),
                    }
                } else {
                    current.row = row + 1;
                    DuckDBRow {
                        batch: current.batch.clone(),
                        row,
                    }
                };
                return Ok(Some(item));
            }
            let chunk = CBox::new(
                unsafe {
                    if self.streaming {
                        duckdb_stream_fetch_chunk(*self.result)
                    } else {
                        duckdb_fetch_chunk(*self.result)
                    }
                },
                |mut chunk| unsafe { duckdb_destroy_data_chunk(&mut chunk) },
            );
            if chunk.is_null() {
                let error = unsafe { duckdb_result_error(&mut *self.result) };
                return if error.is_null() {
                    Ok(None)
                } else {
                    Err(Error::msg(
                        unsafe { CStr::from_ptr(error) }.to_string_lossy().into_owned(),
                    ))
                };
            }
            let size = unsafe { duckdb_data_chunk_get_size(*chunk) } as usize;
            if size == 0 {
                continue;
            }
            let vectors = self
                .schema
                .types
                .iter()
                .enumerate()
                .map(|(i, value_t)| unsafe {
                    VectorNode::from_vector(
                        duckdb_data_chunk_get_vector(*chunk, i as _),
                        value_t,
                    )
                })
                .collect::<Result<Box<[_]>>>();
            let vectors = vectors?;
            let batch = Arc::new(Batch {
                vectors,
                schema: self.schema.clone(),
                _chunk: chunk,
            });
            self.batch = Some((
                DuckDBRow {
                    batch: batch.clone(),
                    row: 0,
                },
                size,
            ));
        }
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.batch {
            Some((row, size)) => (size - row.row, Some(size - row.row)),
            None => (0, None),
        }
    }
}

impl Iterator for Rows {
    type Item = Result<DuckDBRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_row().transpose()
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        Rows::size_hint(self)
    }
}

struct Execution<'s, Q: AsMut<Query<DuckDBDriver>>> {
    // Drop dependent native resources before the query and connection borrow.
    rows: Option<Rows>,
    prepared: Option<CBox<duckdb_prepared_statement>>,
    extracted: Option<CBox<duckdb_extracted_statements>>,
    query: Q,
    connection: &'s mut DuckDBConnection,
    context: String,
    count: u64,
    index: u64,
    finished: bool,
}

enum Statement {
    Rows,
    Affected(RowsAffected),
}

impl<Q: AsMut<Query<DuckDBDriver>>> Execution<'_, Q> {
    fn finish(&mut self) -> Result<()> {
        self.rows = None;
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
        if let Err(cleanup) = self.finish() {
            log::error!("While cleaning up DuckDB execution: {cleanup:#}");
        }
        error.context(self.context.clone())
    }

    fn next_row(&mut self) -> Result<Option<DuckDBRow>> {
        let Some(rows) = self.rows.as_mut() else {
            return Ok(None);
        };
        match rows.next() {
            Some(Ok(row)) => Ok(Some(row)),
            Some(Err(error)) => Err(error),
            None => {
                self.rows = None;
                self.prepared = None;
                Ok(None)
            }
        }
    }

    fn next_statement(&mut self) -> Result<Option<Statement>> {
        if self.finished {
            return Ok(None);
        }
        // An outer advance invalidates the previous row iterator, but does not hide errors
        // in unread rows. Extract and discard one row at a time before executing more SQL.
        while self.next_row()?.is_some() {}
        if self.index == self.count {
            self.finish()?;
            return Ok(None);
        }
        unsafe {
            let statement = if let Some(extracted) = &self.extracted {
                let mut prepared =
                    CBox::new(ptr::null_mut(), |mut p| duckdb_destroy_prepare(&mut p));
                let rc = duckdb_prepare_extracted_statement(
                    *self.connection.connection,
                    **extracted,
                    self.index,
                    &mut *prepared,
                );
                if rc != duckdb_state_DuckDBSuccess {
                    return Err(Error::msg(
                        error_message_from_ptr(&duckdb_prepare_error(*prepared)).into_owned(),
                    ));
                }
                let statement = *prepared;
                self.prepared = Some(prepared);
                statement
            } else {
                match self.query.as_mut() {
                    Query::Prepared(query) => query.statement(),
                    Query::Raw(_) => unreachable!(),
                }
            };
            self.index += 1;
            let mut result = CBox::new(mem::zeroed(), |mut r| duckdb_destroy_result(&mut r));
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
                // A prepared statement keeps the schema of its first execution.
                let cached = match self.query.as_mut() {
                    Query::Prepared(query) => query.schema.clone(),
                    Query::Raw(_) => None,
                };
                let schema = match cached {
                    Some(schema) => schema,
                    None => {
                        let schema = RowSchema::read(&mut result)?;
                        if let Query::Prepared(query) = self.query.as_mut() {
                            query.schema = Some(schema.clone());
                        }
                        schema
                    }
                };
                self.rows = Some(Rows::new(result, schema));
                Ok(Some(Statement::Rows))
            } else {
                let affected = RowsAffected {
                    rows_affected: Some(duckdb_rows_changed(&mut *result)),
                    ..Default::default()
                };
                // The native result must die before its temporary prepared statement.
                drop(result);
                self.prepared = None;
                Ok(Some(Statement::Affected(affected)))
            }
        }
    }
}

impl<Q: AsMut<Query<DuckDBDriver>>> Drop for Execution<'_, Q> {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            log::error!("While dropping DuckDB execution: {error:#}");
        }
    }
}

pub struct Statements<'s, Q: AsMut<Query<DuckDBDriver>>> {
    execution: Execution<'s, Q>,
}

impl<'s, Q: AsMut<Query<DuckDBDriver>>> Statements<'s, Q> {
    pub(crate) fn new(connection: &'s mut DuckDBConnection, mut query: Q) -> Result<Self> {
        let context = format!("While running the query:\n{}", query.as_mut());
        let mut count = 1;
        let extracted = if let Query::Raw(raw) = query.as_mut() {
            let sql = CString::new(raw.0.as_bytes())
                .map_err(|error| Error::new(error).context(context.clone()))?;
            let mut extracted = CBox::new(ptr::null_mut(), |mut p| unsafe {
                duckdb_destroy_extracted(&mut p)
            });
            unsafe {
                count =
                    duckdb_extract_statements(*connection.connection, sql.as_ptr(), &mut *extracted);
                let error = duckdb_extract_statements_error(*extracted);
                if !error.is_null() {
                    return Err(
                        Error::msg(CStr::from_ptr(error).to_string_lossy().into_owned())
                            .context(context),
                    );
                }
            }
            Some(extracted)
        } else {
            None
        };
        Ok(Self {
            execution: Execution {
                rows: None,
                prepared: None,
                extracted,
                query,
                connection,
                context,
                count,
                index: 0,
                finished: false,
            },
        })
    }
}

impl<Q: AsMut<Query<DuckDBDriver>>> Iterator for Statements<'_, Q> {
    type Item = Result<QueryResult<DuckDBRow>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.execution.next_row() {
                Ok(Some(row)) => return Some(Ok(QueryResult::Row(row))),
                Ok(None) => {}
                Err(error) => return Some(Err(self.execution.fail(error))),
            }
            match self.execution.next_statement() {
                Ok(Some(Statement::Rows)) => {}
                Ok(Some(Statement::Affected(affected))) => {
                    return Some(Ok(QueryResult::Affected(affected)));
                }
                Ok(None) => return None,
                Err(error) => return Some(Err(self.execution.fail(error))),
            }
        }
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.execution.rows {
            Some(rows) => rows.size_hint(),
            None => (0, None),
        }
    }
}
