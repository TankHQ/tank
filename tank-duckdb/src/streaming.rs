use crate::{
    DuckDBConnection, DuckDBDriver, ValueWrap,
    cbox::CBox,
    check_rc, duckdb_bytes_to_slice, duckdb_date_to_date, duckdb_hugeint_to_i128,
    duckdb_hugeint_to_uuid, duckdb_interval_to_interval, duckdb_str_to_str, duckdb_time_ns_to_time,
    duckdb_time_to_time, duckdb_uhugeint_to_u128, nanos_to_primitive_date_time,
};
use anyhow::anyhow;
use libduckdb_sys::v2::{
    DUCKDB_V2_LOGICAL_TYPE_ID::*, DUCKDB_V2_RESULT_STEP_STATUS::*, DUCKDB_V2_RESULT_TYPE::*, *,
};
use std::{
    borrow::Cow,
    cmp::max,
    collections::HashMap,
    ffi::{CString, c_void},
    fmt::{self, Debug, Formatter},
    mem,
    pin::Pin,
    ptr,
    sync::Arc,
    task::{Context, Poll},
};
use tank_core::{
    Error, ErrorContext, Prepared, Query, QueryResult, Result, Row, RowLabels, RowOwned,
    RowsAffected, Value, ValueOwned, decimal_from_scaled, stream::Stream,
};

struct VectorNode {
    vector: duckdb_v2_vector_handle,
    id: DUCKDB_V2_LOGICAL_TYPE_ID,
    data: *const c_void,
    validity: *const u64,
    children: Box<[VectorNode]>,
}

unsafe impl Send for VectorNode {}
unsafe impl Sync for VectorNode {}

impl VectorNode {
    #[inline(always)]
    fn flat_view(vector: duckdb_v2_vector_handle) -> Result<duckdb_v2_vector_view> {
        unsafe {
            let mut view: duckdb_v2_vector_view = mem::zeroed();
            if duckdb_v2_vector_get_view(vector, &mut view, ptr::null_mut())
                != DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_NONE
                || !view.sel.is_null()
            {
                check_rc(|err| duckdb_v2_vector_flatten(vector, err))?;
                check_rc(|err| duckdb_v2_vector_get_view(vector, &mut view, err))?;
            }
            Ok(view)
        }
    }

    fn from_vector(vector: duckdb_v2_vector_handle, ty: &ValueOwned) -> Result<Self> {
        unsafe {
            let view = Self::flat_view(vector)?;
            let id = if matches!(ty, Value::Time(..) | Value::Timestamp(..)) {
                let mut vec_ty = CBox::new(ptr::null_mut(), |mut p| {
                    duckdb_v2_logical_type_destroy(&mut p);
                });
                let mut id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
                check_rc(|err| duckdb_v2_vector_get_logical_type(vector, &mut *vec_ty, err))?;
                check_rc(|err| duckdb_v2_logical_type_get_id(*vec_ty, &mut id, err))?;
                id
            } else {
                DUCKDB_V2_LOGICAL_TYPE_ID_INVALID
            };
            let child_vec = |v, i| {
                let mut c = ptr::null_mut();
                check_rc(|err| duckdb_v2_vector_get_child(v, i, &mut c, err)).map(|_| c)
            };
            let children = match ty {
                Value::List(_, t) | Value::Array(_, t) => {
                    Box::new([Self::from_vector(child_vec(vector, 0)?, &t.element)?])
                }
                Value::Struct(_, t) => t
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(i, (_, f_ty))| Self::from_vector(child_vec(vector, i as idx_t)?, f_ty))
                    .collect::<Result<Box<[_]>>>()?,
                Value::Map(_, t) => {
                    let entries = child_vec(vector, 0)?;
                    Self::flat_view(entries)?;
                    Box::new([
                        Self::from_vector(child_vec(entries, 0)?, &t.key)?,
                        Self::from_vector(child_vec(entries, 1)?, &t.value)?,
                    ])
                }
                _ => Box::new([]),
            };
            Ok(Self {
                vector,
                id,
                data: view.data,
                validity: view.validity,
                children,
            })
        }
    }

    #[inline(always)]
    fn is_valid(&self, row: usize) -> bool {
        self.validity.is_null()
            || ((unsafe { *self.validity.add(row >> 6) } >> (row & 63)) & 1) != 0
    }

    #[inline(never)]
    fn extract<'a>(&'a self, row: usize, ty: &'a ValueOwned) -> Result<Value<'a>> {
        unsafe {
            let data = self.data;
            if !self.is_valid(row)
                || (data.is_null() && !matches!(ty, Value::Array(..) | Value::Struct(..)))
            {
                return Ok(if matches!(ty, Value::Unknown(..)) {
                    Value::Varchar(None)
                } else {
                    ty.clone()
                });
            }
            Ok(match ty {
                Value::Null => Value::Null,
                Value::Boolean(..) => Value::Boolean(Some(*(data as *const bool).add(row))),
                Value::Int8(..) => Value::Int8(Some(*(data as *const i8).add(row))),
                Value::Int16(..) => Value::Int16(Some(*(data as *const i16).add(row))),
                Value::Int32(..) => Value::Int32(Some(*(data as *const i32).add(row))),
                Value::Int64(..) => Value::Int64(Some(*(data as *const i64).add(row))),
                Value::Int128(..) => Value::Int128(Some(
                    duckdb_hugeint_to_i128(&*(data as *const duckdb_v2_hugeint_t).add(row)).into(),
                )),
                Value::UInt8(..) => Value::UInt8(Some(*(data as *const u8).add(row))),
                Value::UInt16(..) => Value::UInt16(Some(*(data as *const u16).add(row))),
                Value::UInt32(..) => Value::UInt32(Some(*(data as *const u32).add(row))),
                Value::UInt64(..) => Value::UInt64(Some(*(data as *const u64).add(row))),
                Value::UInt128(..) => Value::UInt128(Some(
                    duckdb_uhugeint_to_u128(&*(data as *const duckdb_v2_uhugeint_t).add(row))
                        .into(),
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
                        duckdb_hugeint_to_i128(&*(data as *const duckdb_v2_hugeint_t).add(row))
                    };
                    Value::Decimal(Some(decimal_from_scaled(num, *scale as u32)?), *width, *scale)
                }
                Value::Date(..) => {
                    Value::Date(Some(duckdb_date_to_date(*(data as *const i32).add(row))?))
                }
                Value::Time(..) => Value::Time(Some(match self.id {
                    DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS => {
                        duckdb_time_ns_to_time(*(data as *const i64).add(row))?
                    }
                    DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ => {
                        duckdb_time_to_time((*(data as *const u64).add(row) >> 24) as i64)?
                    }
                    _ => duckdb_time_to_time(*(data as *const i64).add(row))?,
                })),
                Value::Timestamp(..) => {
                    let raw = *(data as *const i64).add(row) as i128;
                    Value::Timestamp(Some(nanos_to_primitive_date_time(match self.id {
                        DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC => raw * 1_000_000_000,
                        DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS => raw * 1_000_000,
                        DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS
                        | DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS => raw,
                        _ => raw * 1_000,
                    })?))
                }
                Value::Interval(..) => Value::Interval(Some(duckdb_interval_to_interval(
                    *(data as *const duckdb_v2_interval_t).add(row),
                ))),
                Value::Varchar(..) => Value::Varchar(Some(Cow::Borrowed(
                    std::str::from_utf8_unchecked(duckdb_bytes_to_slice(
                        &*(data as *const duckdb_v2_bytes).add(row),
                    )),
                ))),
                Value::Unknown(..) => {
                    let mut val = ValueWrap::new_value_box();
                    let mut s = duckdb_v2_str::default();
                    check_rc(|err| {
                        duckdb_v2_vector_get_value(self.vector, row as idx_t, &mut *val, err)
                    })?;
                    check_rc(|err| duckdb_v2_value_get_varchar(*val, &mut s, err))?;
                    Value::Varchar(Some(duckdb_str_to_str(s).to_owned().into()))
                }
                Value::Blob(..) => Value::Blob(Some(Cow::Borrowed(duckdb_bytes_to_slice(
                    &*(data as *const duckdb_v2_bytes).add(row),
                )))),
                Value::Uuid(..) => Value::Uuid(Some(duckdb_hugeint_to_uuid(
                    &*(data as *const duckdb_v2_hugeint_t).add(row),
                ))),
                Value::List(_, t) | Value::Array(_, t) => {
                    let (begin, len) = if matches!(ty, Value::List(..)) {
                        let e = *(data as *const duckdb_v2_list_entry).add(row);
                        (e.offset as usize, e.length as usize)
                    } else {
                        (row * t.len as usize, t.len as usize)
                    };
                    let child = self.children.first().context("Missing DuckDB child vector")?;
                    let values = (begin..begin + len)
                        .map(|i| child.extract(i, &t.element))
                        .collect::<Result<Box<[_]>>>()?;
                    if matches!(ty, Value::List(..)) {
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
                            .map(|((name, f_ty), child)| {
                                Ok((name.clone(), child.extract(row, f_ty)?))
                            })
                            .collect::<Result<Box<[_]>>>()?,
                    ),
                    struct_type.clone(),
                ),
                Value::Map(_, map_type) => {
                    let e = *(data as *const duckdb_v2_list_entry).add(row);
                    let [keys, vals] = &*self.children else {
                        return Err(anyhow!("Missing key/value child vectors for DuckDB MAP"));
                    };
                    Value::Map(
                        Some(Box::new(
                            (e.offset as usize..(e.offset + e.length) as usize)
                                .map(|i| {
                                    Ok((
                                        keys.extract(i, &map_type.key)?,
                                        vals.extract(i, &map_type.value)?,
                                    ))
                                })
                                .collect::<Result<HashMap<_, _>>>()?,
                        )),
                        map_type.clone(),
                    )
                }
                _ => return Err(anyhow!("Unsupported DuckDB column type: {ty:?}")),
            })
        }
    }
}

/// A native row view borrowing a DuckDB data chunk and shared column metadata.
pub struct DuckDBRow {
    batch: Arc<(
        Box<[VectorNode]>,
        Arc<[ValueOwned]>,
        RowLabels,
        CBox<duckdb_v2_data_chunk_handle>,
    )>,
    row: usize,
}

impl Debug for DuckDBRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDBRow")
            .field("labels", &self.batch.2)
            .field("len", &self.len())
            .finish()
    }
}

impl Row for DuckDBRow {
    #[inline(always)]
    fn labels(&self) -> &[String] {
        &self.batch.2
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.batch.1.len()
    }

    #[inline(always)]
    fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
        let vec_node = self.batch.0.get(index)?;
        let ty = self.batch.1.get(index)?;
        let row = self.row;
        unsafe {
            let data = vec_node.data;
            if !vec_node.is_valid(row)
                || (data.is_null() && !matches!(ty, Value::Array(..) | Value::Struct(..)))
            {
                return Some(if matches!(ty, Value::Unknown(..)) {
                    Cow::Owned(Value::Varchar(None))
                } else {
                    Cow::Borrowed(ty)
                });
            }
            match ty {
                Value::Boolean(..) => Some(Value::Boolean(Some(*(data as *const bool).add(row)))),
                Value::Int32(..) => Some(Value::Int32(Some(*(data as *const i32).add(row)))),
                Value::Int64(..) => Some(Value::Int64(Some(*(data as *const i64).add(row)))),
                Value::Float64(..) => Some(Value::Float64(Some(*(data as *const f64).add(row)))),
                Value::Varchar(..) => Some(Value::Varchar(Some(Cow::Borrowed(
                    std::str::from_utf8_unchecked(duckdb_bytes_to_slice(
                        &*(data as *const duckdb_v2_bytes).add(row),
                    )),
                )))),
                _ => vec_node.extract(row, ty).ok(),
            }
            .map(Cow::Owned)
        }
    }

    fn into_owned(self) -> RowOwned {
        RowOwned::new(
            self.batch.2.clone(),
            self.values().map(|v| v.into_owned().into_owned()).collect(),
        )
    }
}

pub struct Statements<'s, Q: AsMut<Query<DuckDBDriver>>> {
    // Drop dependent native resources before the query and connection borrow.
    batch: Option<(DuckDBRow, usize)>,
    pub(crate) result: Option<CBox<duckdb_v2_result_handle>>,
    schema: Option<(RowLabels, Arc<[ValueOwned]>)>,
    rows_changed: Option<u64>,
    iterator: Option<CBox<duckdb_v2_statement_iterator_handle>>,
    query: Q,
    connection: &'s mut DuckDBConnection,
    finished: bool,
}

impl<'s, Q: AsMut<Query<DuckDBDriver>>> Statements<'s, Q> {
    const STEP_BUDGET: usize = 256;

    pub(crate) fn new(connection: &'s mut DuckDBConnection, mut query: Q) -> Result<Self> {
        let iterator = if let Query::Raw(raw) = query.as_mut() {
            let sql = CString::new(raw.0.as_bytes())
                .with_context(|| format!("While running the query:\n{}", raw.0))?;
            let mut iter = CBox::new(ptr::null_mut(), |mut p| unsafe {
                duckdb_v2_statement_iterator_destroy(&mut p);
            });
            unsafe {
                check_rc(|err| {
                    duckdb_v2_parse_sql(*connection.connection, sql.as_ptr(), &mut *iter, err)
                })
                .with_context(|| format!("While running the query:\n{}", raw.0))?;
            }
            Some(iter)
        } else {
            None
        };
        Ok(Self {
            batch: None,
            result: None,
            schema: None,
            rows_changed: None,
            iterator,
            query,
            connection,
            finished: false,
        })
    }

    fn finish(&mut self) -> Result<()> {
        self.batch = None;
        self.result = None;
        self.schema = None;
        self.iterator = None;
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
}

impl<Q: AsMut<Query<DuckDBDriver>>> Drop for Statements<'_, Q> {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            log::error!("While dropping DuckDB execution: {error:#}");
        }
    }
}

impl<'s, Q: AsMut<Query<DuckDBDriver>>> Stream for Statements<'s, Q> {
    type Item = Result<QueryResult<DuckDBRow>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let mut steps = 0;
        let res: Result<Poll<Option<QueryResult<DuckDBRow>>>> = (|| loop {
            if this.finished {
                return Ok(Poll::Ready(None));
            }
            if let Some((current, size)) = &mut this.batch {
                let row = current.row;
                for (v, ty) in current.batch.0.iter().zip(current.batch.1.iter()) {
                    if matches!(
                        ty,
                        Value::Date(..)
                            | Value::Time(..)
                            | Value::Timestamp(..)
                            | Value::Unknown(..)
                            | Value::List(..)
                            | Value::Array(..)
                            | Value::Struct(..)
                            | Value::Map(..)
                            | Value::Decimal(_, 29.., _)
                            | Value::Decimal(_, _, 29..)
                    ) {
                        v.extract(row, ty)?;
                    }
                }
                let item = if row + 1 == *size {
                    this.batch.take().context("Missing active chunk batch")?.0
                } else {
                    current.row = row + 1;
                    DuckDBRow {
                        batch: current.batch.clone(),
                        row,
                    }
                };
                return Ok(Poll::Ready(Some(QueryResult::Row(item))));
            }
            if let Some(result) = &this.result {
                if steps >= Self::STEP_BUDGET {
                    cx.waker().wake_by_ref();
                    return Ok(Poll::Pending);
                }
                let mut raw_chunk = ptr::null_mut();
                let mut status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
                unsafe {
                    check_rc(|err| {
                        duckdb_v2_result_step(**result, &mut raw_chunk, &mut status, err)
                    })?;
                }
                match status {
                    DUCKDB_V2_RESULT_STEP_STATUS_WAITING => steps += 1,
                    DUCKDB_V2_RESULT_STEP_STATUS_CHUNK if !raw_chunk.is_null() => unsafe {
                        let chunk = CBox::new(raw_chunk, |mut c| {
                            duckdb_v2_data_chunk_destroy(&mut c);
                        });
                        let mut size = 0;
                        check_rc(|err| duckdb_v2_data_chunk_get_size(*chunk, &mut size, err))?;
                        if let Some((names, types)) = &this.schema {
                            if size > 0 {
                                let vectors = types
                                    .iter()
                                    .enumerate()
                                    .map(|(col, ty)| {
                                        let mut vec = ptr::null_mut();
                                        check_rc(|err| {
                                            duckdb_v2_data_chunk_get_vector(
                                                *chunk,
                                                col as idx_t,
                                                &mut vec,
                                                err,
                                            )
                                        })?;
                                        VectorNode::from_vector(vec, ty)
                                    })
                                    .collect::<Result<Box<[_]>>>()?;
                                this.batch = Some((
                                    DuckDBRow {
                                        batch: Arc::new((
                                            vectors,
                                            types.clone(),
                                            names.clone(),
                                            chunk,
                                        )),
                                        row: 0,
                                    },
                                    size as usize,
                                ));
                            }
                        } else if this.rows_changed.is_none() {
                            let mut vec = ptr::null_mut();
                            this.rows_changed = Some(if size > 0 {
                                check_rc(|err| {
                                    duckdb_v2_data_chunk_get_vector(*chunk, 0, &mut vec, err)
                                })?;
                                let view = VectorNode::flat_view(vec)?;
                                if view.data.is_null() {
                                    0
                                } else {
                                    max(*(view.data as *const i64), 0) as u64
                                }
                            } else {
                                0
                            });
                        }
                    },
                    DUCKDB_V2_RESULT_STEP_STATUS_CHUNK => {}
                    DUCKDB_V2_RESULT_STEP_STATUS_FINISHED => {
                        let is_query = this.schema.take().is_some();
                        this.result = None;
                        if this.iterator.is_none() {
                            this.finish()?;
                        }
                        if !is_query {
                            return Ok(Poll::Ready(Some(QueryResult::Affected(RowsAffected {
                                rows_affected: Some(this.rows_changed.unwrap_or(0)),
                                last_affected_id: None,
                            }))));
                        }
                    }
                    _ => return Err(anyhow!("DuckDB query was interrupted")),
                }
                continue;
            }
            unsafe {
                let mut result = CBox::new(ptr::null_mut(), |mut r| {
                    duckdb_v2_result_destroy(&mut r);
                });
                let prepared_schema = if let Some(iterator) = &this.iterator {
                    let mut stmt = CBox::new(ptr::null_mut(), |mut s| {
                        duckdb_v2_sql_statement_destroy(&mut s);
                    });
                    check_rc(|err| {
                        duckdb_v2_statement_iterator_next(**iterator, &mut *stmt, err)
                    })?;
                    if stmt.is_null() {
                        this.finish()?;
                        return Ok(Poll::Ready(None));
                    }
                    check_rc(|err| {
                        duckdb_v2_statement_execute(
                            *this.connection.connection,
                            *stmt,
                            ptr::null(),
                            ptr::null(),
                            0,
                            &mut *result,
                            err,
                        )
                    })?;
                    None
                } else {
                    let Query::Prepared(prepared) = this.query.as_mut() else {
                        return Err(anyhow!("Expected a prepared query"));
                    };
                    if prepared.is_empty() {
                        this.finish()?;
                        return Ok(Poll::Ready(None));
                    }
                    let params = prepared.take_params(this.connection.factory)?;
                    let raw_params = params.iter().map(|v| **v).collect::<Vec<_>>();
                    check_rc(|err| {
                        duckdb_v2_prepared_statement_execute(
                            prepared.statement(),
                            ptr::null(),
                            if raw_params.is_empty() {
                                ptr::null()
                            } else {
                                raw_params.as_ptr()
                            },
                            raw_params.len() as idx_t,
                            &mut *result,
                            err,
                        )
                    })?;
                    Some(&mut prepared.schema)
                };
                let mut result_type = DUCKDB_V2_RESULT_TYPE_NOTHING;
                check_rc(|err| duckdb_v2_result_get_result_type(*result, &mut result_type, err))?;
                match result_type {
                    DUCKDB_V2_RESULT_TYPE_QUERY_RESULT => {
                        this.schema = Some(if let Some(Some(cached)) = prepared_schema.as_deref() {
                            cached.clone()
                        } else {
                            let mut schema = CBox::new(ptr::null_mut(), |mut s| {
                                duckdb_v2_schema_destroy(&mut s);
                            });
                            check_rc(|err| {
                                duckdb_v2_result_get_schema(*result, &mut *schema, err)
                            })?;
                            let mut cols = 0;
                            check_rc(|err| duckdb_v2_schema_get_count(*schema, &mut cols, err))?;
                            let (raw_names, types): (Vec<_>, Vec<_>) = (0..cols)
                                .map(|i| {
                                    let mut name = duckdb_v2_identifier_t::default();
                                    let mut ty = ptr::null_mut();
                                    check_rc(|err| {
                                        duckdb_v2_schema_get_field(
                                            *schema, i, &mut name, &mut ty, err,
                                        )
                                    })?;
                                    Ok((duckdb_str_to_str(name), ValueWrap::try_from(ty)?.into()))
                                })
                                .collect::<Result<Vec<_>>>()?
                                .into_iter()
                                .unzip();
                            if this.connection.labels.len() != raw_names.len()
                                || !this
                                    .connection
                                    .labels
                                    .iter()
                                    .zip(&raw_names)
                                    .all(|(a, b)| a == b)
                            {
                                this.connection.labels =
                                    raw_names.into_iter().map(str::to_owned).collect();
                            }
                            let pair = (this.connection.labels.clone(), types.into());
                            if let Some(cache) = prepared_schema {
                                *cache = Some(pair.clone());
                            }
                            pair
                        });
                    }
                    DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS => this.rows_changed = None,
                    _ => this.rows_changed = Some(0),
                }
                this.result = Some(result);
            }
        })();
        match res {
            Ok(poll) => poll.map(|opt| opt.map(Ok)),
            Err(e) => Poll::Ready(Some(Err(this.fail(e)))),
        }
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.batch
            .as_ref()
            .map_or((0, None), |(r, size)| (size - r.row, None))
    }
}
