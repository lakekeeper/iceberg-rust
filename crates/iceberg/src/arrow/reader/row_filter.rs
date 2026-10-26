// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Predicate-driven row filtering for `ArrowReader`: constructing Arrow `RowFilter`s
//! from Iceberg predicates, row-group selection based on column statistics, and
//! row-selection via the Parquet page index. Also includes byte-range row-group
//! filtering used for file splitting.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{ArrowError, DataType, SchemaRef as ArrowSchemaRef, TimeUnit};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicate, ArrowPredicateFn, RowSelection};
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescriptor;

use super::{ArrowReader, PredicateConverter};
use crate::arrow::caching_delete_file_loader::{EqDeleteKey, EqDeleteSet};
use crate::arrow::{arrow_primitive_to_literal, arrow_type_to_type, type_to_arrow_type};
use crate::error::Result;
use crate::expr::BoundPredicate;
use crate::expr::visitors::bound_predicate_visitor::visit;
use crate::expr::visitors::page_index_evaluator::PageIndexEvaluator;
use crate::expr::visitors::row_group_metrics_evaluator::RowGroupMetricsEvaluator;
use crate::spec::{Datum, Literal, PrimitiveLiteral, PrimitiveType, Schema, Type};
use crate::{Error, ErrorKind};

/// One equality-delete key column of a decoded batch
struct KeyColumn {
    literals: Vec<Option<Literal>>,
    /// Iceberg type of the column in the table schema.
    primitive: PrimitiveType,
    /// Rows whose key has no exact value in the table type, such as a nanosecond timestamp with
    /// sub-microsecond digits in a microsecond column. Such a key equals no delete key.
    inexact: Option<Vec<bool>>,
}

impl ArrowReader {
    /// Builds the Arrow row-filter predicate for a bound scan predicate.
    pub(super) fn build_scan_predicate(
        predicates: &BoundPredicate,
        parquet_schema: &SchemaDescriptor,
        iceberg_field_ids: &HashSet<i32>,
        field_id_map: &HashMap<i32, usize>,
    ) -> Result<Box<dyn ArrowPredicate>> {
        // Collect Parquet column indices from field ids.
        // If the field id is not found in Parquet schema, it will be ignored due to schema evolution.
        let mut column_indices = iceberg_field_ids
            .iter()
            .filter_map(|field_id| field_id_map.get(field_id).cloned())
            .collect::<Vec<_>>();
        column_indices.sort();

        // The converter that converts `BoundPredicates` to `ArrowPredicates`
        let mut converter = PredicateConverter {
            parquet_schema,
            column_map: field_id_map,
            column_indices: &column_indices,
        };

        // After collecting required leaf column indices used in the predicate,
        // creates the projection mask for the Arrow predicates.
        let projection_mask = ProjectionMask::leaves(parquet_schema, column_indices.clone());
        let predicate_func = visit(&mut converter, predicates)?;
        Ok(Box::new(ArrowPredicateFn::new(
            projection_mask,
            predicate_func,
        )))
    }

    /// Builds one Arrow row-filter predicate per equality-delete set. The predicate is based
    /// on a hash-set lookup (see `EqDeleteSet`). It keeps a row unless its key tuple is present
    /// in that set. A row is deleted when it matches any set (the predicates are AND-ed by the `RowFilter`).
    pub(super) fn build_equality_delete_predicates(
        sets: &[Arc<EqDeleteSet>],
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
        use_position_fallback: bool,
        absent_key_values: &HashMap<i32, Datum>,
    ) -> Result<Vec<Box<dyn ArrowPredicate>>> {
        let field_id_map =
            Self::resolve_field_id_map(parquet_schema, arrow_schema, use_position_fallback)?;

        let mut predicates: Vec<Box<dyn ArrowPredicate>> = Vec::new();
        for set in sets {
            if set.is_empty() {
                continue;
            }

            // Parquet leaf index for each key column, in `fields` order; a column dropped
            // from this file by schema evolution has no entry.
            let leaf_indices: Vec<Option<usize>> = set
                .fields
                .iter()
                .map(|(_, id, _)| field_id_map.get(id).copied())
                .collect();

            // Every key column may be absent from this file, leaving `column_indices`
            // empty. `ProjectionMask::leaves(schema, [])` still yields a zero-column batch
            // with the correct row count, so the probe returns one boolean per row.
            let mut column_indices: Vec<usize> = leaf_indices.iter().flatten().copied().collect();
            column_indices.sort_unstable();
            column_indices.dedup();
            let projection_mask = ProjectionMask::leaves(parquet_schema, column_indices.clone());

            // Position of each key column within the projected batch (parquet-rs presents the
            // masked leaves in ascending leaf-index order).
            let batch_positions: Vec<Option<usize>> = leaf_indices
                .iter()
                .map(|leaf| leaf.and_then(|idx| column_indices.binary_search(&idx).ok()))
                .collect();

            // A key column absent from this file probes as the value projection fills in for
            // it (`absent_key_values`), as Java's reader does, and as null when there is none.
            let absent_values: Vec<Option<Datum>> = set
                .fields
                .iter()
                .map(|(_, id, ty)| {
                    absent_key_values
                        .get(id)
                        .map(|value| match (value.literal(), ty) {
                            // Delete keys are read back through Arrow, which types a uuid as
                            // fixed[16].
                            (
                                PrimitiveLiteral::UInt128(uuid),
                                Type::Primitive(PrimitiveType::Fixed(16)),
                            ) => Datum::fixed(uuid.to_be_bytes()),
                            _ => value.clone(),
                        })
                })
                .collect();
            let target_types: Vec<Type> = set.fields.iter().map(|(_, _, ty)| ty.clone()).collect();
            let num_cols = set.fields.len();
            let set = set.clone();

            let predicate_func =
                move |batch: RecordBatch| -> std::result::Result<BooleanArray, ArrowError> {
                    let num_rows = batch.num_rows();

                    let mut columns: Vec<Option<KeyColumn>> = Vec::with_capacity(num_cols);
                    for (i, target_type) in target_types.iter().enumerate() {
                        // A column absent from this file (schema evolution)
                        let Some(pos) = batch_positions[i] else {
                            columns.push(None);
                            continue;
                        };
                        let mut array = Arc::clone(batch.column(pos));
                        let source_type = arrow_type_to_type(array.data_type())
                            .map_err(|e| ArrowError::ComputeError(e.to_string()))?;
                        // Promotion to the table type keeps these comparable with the parsed
                        // delete keys under schema evolution.
                        let mut inexact = None;
                        if source_type != *target_type {
                            if let (
                                DataType::Timestamp(TimeUnit::Nanosecond, _),
                                Type::Primitive(
                                    PrimitiveType::Timestamp | PrimitiveType::Timestamptz,
                                ),
                            ) = (array.data_type(), target_type)
                            {
                                inexact = Some(
                                    array
                                        .as_primitive::<TimestampNanosecondType>()
                                        .iter()
                                        .map(|ns| ns.is_some_and(|ns| ns % 1000 != 0))
                                        .collect(),
                                );
                            }
                            let target_arrow_type = type_to_arrow_type(target_type)
                                .map_err(|e| ArrowError::ComputeError(e.to_string()))?;
                            array = cast_with_options(&array, &target_arrow_type, &CastOptions {
                                safe: false,
                                ..CastOptions::default()
                            })?;
                        }
                        let primitive = target_type
                            .as_primitive_type()
                            .ok_or_else(|| {
                                ArrowError::ComputeError(
                                    "equality delete key column is not a primitive type"
                                        .to_string(),
                                )
                            })?
                            .clone();
                        columns.push(Some(KeyColumn {
                            literals: arrow_primitive_to_literal(&array, target_type)
                                .map_err(|e| ArrowError::ComputeError(e.to_string()))?,
                            primitive,
                            inexact,
                        }));
                    }

                    // One hash lookup per row.
                    let mut keep = Vec::with_capacity(num_rows);
                    // Absent columns keep their value for the whole batch; present ones are set
                    // per row.
                    let mut probe = EqDeleteKey(absent_values.clone());
                    for row in 0..num_rows {
                        let mut unmatchable = false;
                        for (i, column) in columns.iter_mut().enumerate() {
                            let Some(column) = column else {
                                continue;
                            };
                            unmatchable |= column.inexact.as_ref().is_some_and(|rows| rows[row]);
                            // we can `take` because each cell is probed once.
                            probe.0[i] = match column.literals[row].take() {
                                Some(Literal::Primitive(primitive)) => {
                                    Some(Datum::new(column.primitive.clone(), primitive))
                                }
                                Some(other) => {
                                    return Err(ArrowError::ComputeError(format!(
                                        "equality delete key column {i} is not a primitive \
                                         literal: {other:?}"
                                    )));
                                }
                                None => None,
                            };
                        }
                        keep.push(unmatchable || !set.keys.contains(&probe));
                    }
                    Ok(BooleanArray::from(keep))
                };

            predicates.push(Box::new(ArrowPredicateFn::new(
                projection_mask,
                predicate_func,
            )));
        }

        Ok(predicates)
    }

    pub(super) fn get_selected_row_group_indices(
        predicate: &BoundPredicate,
        parquet_metadata: &Arc<ParquetMetaData>,
        field_id_map: &HashMap<i32, usize>,
        snapshot_schema: &Schema,
        candidate_row_groups: &[usize],
    ) -> Result<Vec<usize>> {
        let row_groups_metadata = parquet_metadata.row_groups();
        let mut results = Vec::with_capacity(candidate_row_groups.len());

        for &idx in candidate_row_groups {
            let row_group_metadata = row_groups_metadata.get(idx).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "row group index {idx} out of range ({} row groups)",
                        row_groups_metadata.len()
                    ),
                )
            })?;
            if RowGroupMetricsEvaluator::eval(
                predicate,
                row_group_metadata,
                field_id_map,
                snapshot_schema,
            )? {
                results.push(idx);
            }
        }

        Ok(results)
    }

    /// Computes a [`RowSelection`] by evaluating the filter predicate against
    /// the Parquet page index (column index + offset index).
    ///
    /// Returns `Ok(None)` when the Parquet file lacks column or offset index
    /// metadata (common with older files written before page indexes became
    /// standard). In that case page-level pruning is simply skipped; row-group
    /// filtering and the Arrow row filter still apply the predicate.
    ///
    /// `Ok(Some(empty))` case means that all rows were filtered by the predicate - returning zero rows
    pub(super) fn get_row_selection_for_filter_predicate(
        predicate: &BoundPredicate,
        parquet_metadata: &Arc<ParquetMetaData>,
        selected_row_groups: &Option<Vec<usize>>,
        field_id_map: &HashMap<i32, usize>,
        snapshot_schema: &Schema,
    ) -> Result<Option<RowSelection>> {
        let Some(column_index) = parquet_metadata.column_index() else {
            tracing::debug!("ColumnIndex was absent while reading this file");
            return Ok(None);
        };

        let Some(offset_index) = parquet_metadata.offset_index() else {
            tracing::debug!("OffsetIndex was absent while reading this file");
            return Ok(None);
        };

        // If all row groups were filtered out, return an empty RowSelection (select no rows)
        //
        if let Some(selected_row_groups) = selected_row_groups
            && selected_row_groups.is_empty()
        {
            return Ok(Some(RowSelection::from(Vec::new())));
        }

        let mut selected_row_groups_idx = 0;

        let page_index = column_index
            .iter()
            .enumerate()
            .zip(offset_index)
            .zip(parquet_metadata.row_groups());

        let mut results = Vec::new();
        for (((idx, column_index), offset_index), row_group_metadata) in page_index {
            if let Some(selected_row_groups) = selected_row_groups {
                // skip row groups that aren't present in selected_row_groups
                if idx == selected_row_groups[selected_row_groups_idx] {
                    selected_row_groups_idx += 1;
                } else {
                    continue;
                }
            }

            let selections_for_page = PageIndexEvaluator::eval(
                predicate,
                column_index,
                offset_index,
                row_group_metadata,
                field_id_map,
                snapshot_schema,
            )?;

            results.push(selections_for_page);

            if let Some(selected_row_groups) = selected_row_groups
                && selected_row_groups_idx == selected_row_groups.len()
            {
                break;
            }
        }

        Ok(Some(
            results.into_iter().flatten().collect::<Vec<_>>().into(),
        ))
    }

    /// Filters row groups by byte range to support Iceberg's file splitting.
    ///
    /// Engines split a data file into multiple scan tasks, each covering a byte range
    /// `[start, start+length)`. Normally Iceberg planning aligns these splits to row group
    /// boundaries using the data file's `split_offsets` metadata, so a task's range never
    /// bisects a row group. But when `split_offsets` is missing (e.g. a manually written or
    /// non-conforming file), planning falls back to tiling the file at the requested split
    /// size, and a task's range can land in the middle of a row group.
    ///
    /// A row group must be read by exactly one task, otherwise its rows are duplicated. We
    /// assign ownership by the row group's midpoint: a task owns a row group only if its range
    /// contains that midpoint. Because the tasks tile the file contiguously and disjointly,
    /// each midpoint falls in exactly one task. This matches parquet-mr's `BlockMetaData`
    /// midpoint semantics. For a whole-file task (`start=0, length=fileSize`, as iceberg-rust's
    /// own planner emits) every midpoint lies in range, so all row groups are selected.
    pub(super) fn filter_row_groups_by_byte_range(
        parquet_metadata: &Arc<ParquetMetaData>,
        start: u64,
        length: u64,
    ) -> Result<Vec<usize>> {
        let row_groups = parquet_metadata.row_groups();
        let mut selected = Vec::new();
        let end = start + length;

        // Row groups are stored sequentially after the 4-byte magic header.
        let mut current_byte_offset = 4u64;

        for (idx, row_group) in row_groups.iter().enumerate() {
            let row_group_size = row_group.compressed_size() as u64;
            let row_group_midpoint = current_byte_offset + row_group_size / 2;

            // Half-open ownership: a midpoint on a task boundary belongs to the upper task,
            // so exactly one task ever claims a given row group.
            if start <= row_group_midpoint && row_group_midpoint < end {
                selected.push(idx);
            }

            current_byte_offset += row_group_size;
        }

        Ok(selected)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int64Type, TimestampMicrosecondType};
    use arrow_array::{
        ArrayRef, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
        LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use futures::TryStreamExt;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::metadata::{FileMetaData, ParquetMetaData, ParquetMetaDataBuilder};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;
    use tempfile::TempDir;

    use crate::Runtime;
    use crate::arrow::{ArrowReader, ArrowReaderBuilder};
    use crate::expr::{Bind, BoundPredicate, Predicate, Reference};
    use crate::io::FileIO;
    use crate::scan::{FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream};
    use crate::spec::{
        DataContentType, DataFileFormat, Datum, Literal, NestedField, PartitionSpec, PrimitiveType,
        Schema, SchemaRef, Struct, Transform, Type,
    };

    async fn test_perform_read(
        predicate: Predicate,
        schema: SchemaRef,
        table_location: String,
        reader: ArrowReader,
    ) -> Vec<Option<String>> {
        let tasks = {
            let task = FileScanTask::builder()
                .with_file_size_in_bytes(
                    std::fs::metadata(format!("{table_location}/1.parquet"))
                        .unwrap()
                        .len(),
                )
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{table_location}/1.parquet"))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1])
                .with_predicate(Some(predicate.bind(schema, true).unwrap()))
                .with_case_sensitive(false)
                .build()
                .unwrap();
            Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream
        };

        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        result[0].columns()[0]
            .as_string_opt::<i32>()
            .unwrap()
            .iter()
            .map(|v| v.map(ToOwned::to_owned))
            .collect::<Vec<_>>()
    }

    fn setup_kleene_logic(
        data_for_col_a: Vec<Option<String>>,
        col_a_type: DataType,
    ) -> (FileIO, SchemaRef, String, TempDir) {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "a", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", col_a_type.clone(), true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();

        let file_io = FileIO::new_with_fs();

        let col = match col_a_type {
            DataType::Utf8 => Arc::new(StringArray::from(data_for_col_a)) as ArrayRef,
            DataType::LargeUtf8 => Arc::new(LargeStringArray::from(data_for_col_a)) as ArrayRef,
            _ => panic!("unexpected col_a_type"),
        };

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col]).unwrap();

        // Write the Parquet files
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer =
            ArrowWriter::try_new(file, to_write.schema(), Some(props.clone())).unwrap();

        writer.write(&to_write).expect("Writing batch");

        // writer must be closed to write footer
        writer.close().unwrap();

        (file_io, schema, table_location, tmp_dir)
    }

    #[tokio::test]
    async fn test_kleene_logic_or_behaviour() {
        // a IS NULL OR a = 'foo'
        let predicate = Reference::new("a")
            .is_null()
            .or(Reference::new("a").equal_to(Datum::string("foo")));

        // Table data: [NULL, "foo", "bar"]
        let data_for_col_a = vec![None, Some("foo".to_string()), Some("bar".to_string())];

        // Expected: [NULL, "foo"].
        let expected = vec![None, Some("foo".to_string())];

        let (file_io, schema, table_location, _temp_dir) =
            setup_kleene_logic(data_for_col_a, DataType::Utf8);
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let result_data = test_perform_read(predicate, schema, table_location, reader).await;

        assert_eq!(result_data, expected);
    }

    #[tokio::test]
    async fn test_kleene_logic_and_behaviour() {
        // a IS NOT NULL AND a != 'foo'
        let predicate = Reference::new("a")
            .is_not_null()
            .and(Reference::new("a").not_equal_to(Datum::string("foo")));

        // Table data: [NULL, "foo", "bar"]
        let data_for_col_a = vec![None, Some("foo".to_string()), Some("bar".to_string())];

        // Expected: ["bar"].
        let expected = vec![Some("bar".to_string())];

        let (file_io, schema, table_location, _temp_dir) =
            setup_kleene_logic(data_for_col_a, DataType::Utf8);
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        let result_data = test_perform_read(predicate, schema, table_location, reader).await;

        assert_eq!(result_data, expected);
    }

    #[tokio::test]
    async fn test_predicate_cast_literal() {
        let predicates = vec![
            // a == 'foo'
            (Reference::new("a").equal_to(Datum::string("foo")), vec![
                Some("foo".to_string()),
            ]),
            // a != 'foo'
            (
                Reference::new("a").not_equal_to(Datum::string("foo")),
                vec![Some("bar".to_string())],
            ),
            // STARTS_WITH(a, 'foo')
            (Reference::new("a").starts_with(Datum::string("f")), vec![
                Some("foo".to_string()),
            ]),
            // NOT STARTS_WITH(a, 'foo')
            (
                Reference::new("a").not_starts_with(Datum::string("f")),
                vec![Some("bar".to_string())],
            ),
            // a < 'foo'
            (Reference::new("a").less_than(Datum::string("foo")), vec![
                Some("bar".to_string()),
            ]),
            // a <= 'foo'
            (
                Reference::new("a").less_than_or_equal_to(Datum::string("foo")),
                vec![Some("foo".to_string()), Some("bar".to_string())],
            ),
            // a > 'foo'
            (
                Reference::new("a").greater_than(Datum::string("bar")),
                vec![Some("foo".to_string())],
            ),
            // a >= 'foo'
            (
                Reference::new("a").greater_than_or_equal_to(Datum::string("foo")),
                vec![Some("foo".to_string())],
            ),
            // a IN ('foo', 'bar')
            (
                Reference::new("a").is_in([Datum::string("foo"), Datum::string("baz")]),
                vec![Some("foo".to_string())],
            ),
            // a NOT IN ('foo', 'bar')
            (
                Reference::new("a").is_not_in([Datum::string("foo"), Datum::string("baz")]),
                vec![Some("bar".to_string())],
            ),
        ];

        // Table data: ["foo", "bar"]
        let data_for_col_a = vec![Some("foo".to_string()), Some("bar".to_string())];

        let (file_io, schema, table_location, _temp_dir) =
            setup_kleene_logic(data_for_col_a, DataType::LargeUtf8);
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        for (predicate, expected) in predicates {
            println!("testing predicate {predicate}");
            let result_data = test_perform_read(
                predicate.clone(),
                schema.clone(),
                table_location.clone(),
                reader.clone(),
            )
            .await;

            assert_eq!(result_data, expected, "predicate={predicate}");
        }
    }

    /// Verifies that file splits respect byte ranges and only read specific row groups.
    #[tokio::test]
    async fn test_file_splits_respect_byte_ranges() {
        use arrow_array::Int32Array;
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/multi_row_group.parquet");

        // Force each batch into its own row group for testing byte range filtering.
        let batch1 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Int32Array::from(
            (0..100).collect::<Vec<i32>>(),
        ))])
        .unwrap();
        let batch2 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Int32Array::from(
            (100..200).collect::<Vec<i32>>(),
        ))])
        .unwrap();
        let batch3 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Int32Array::from(
            (200..300).collect::<Vec<i32>>(),
        ))])
        .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_row_count(Some(100))
            .build();

        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        writer.write(&batch1).expect("Writing batch 1");
        writer.write(&batch2).expect("Writing batch 2");
        writer.write(&batch3).expect("Writing batch 3");
        writer.close().unwrap();

        // Read the file metadata to get row group byte positions
        let file = File::open(&file_path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();

        println!("File has {} row groups", metadata.num_row_groups());
        assert_eq!(metadata.num_row_groups(), 3, "Expected 3 row groups");

        // Get byte positions for each row group
        let row_group_0 = metadata.row_group(0);
        let row_group_1 = metadata.row_group(1);
        let row_group_2 = metadata.row_group(2);

        let rg0_start = 4u64; // Parquet files start with 4-byte magic "PAR1"
        let rg1_start = rg0_start + row_group_0.compressed_size() as u64;
        let rg2_start = rg1_start + row_group_1.compressed_size() as u64;
        let file_end = rg2_start + row_group_2.compressed_size() as u64;

        println!(
            "Row group 0: {} rows, starts at byte {}, {} bytes compressed",
            row_group_0.num_rows(),
            rg0_start,
            row_group_0.compressed_size()
        );
        println!(
            "Row group 1: {} rows, starts at byte {}, {} bytes compressed",
            row_group_1.num_rows(),
            rg1_start,
            row_group_1.compressed_size()
        );
        println!(
            "Row group 2: {} rows, starts at byte {}, {} bytes compressed",
            row_group_2.num_rows(),
            rg2_start,
            row_group_2.compressed_size()
        );

        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        // Task 1: read only the first row group
        let task1 = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(rg0_start)
            .with_length(row_group_0.compressed_size() as u64)
            .with_data_file_path(file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema.clone())
            .with_project_field_ids(vec![1])
            .with_record_count(Some(100))
            .with_case_sensitive(false)
            .build()
            .unwrap();

        // Task 2: read the second and third row groups
        let task2 = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(rg1_start)
            .with_length(file_end - rg1_start)
            .with_data_file_path(file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema.clone())
            .with_project_field_ids(vec![1])
            .with_record_count(Some(200))
            .with_case_sensitive(false)
            .build()
            .unwrap();

        let tasks1 = Box::pin(futures::stream::iter(vec![Ok(task1)])) as FileScanTaskStream;
        let result1 = reader
            .clone()
            .read(tasks1)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let total_rows_task1: usize = result1.iter().map(|b| b.num_rows()).sum();
        println!(
            "Task 1 (bytes {}-{}) returned {} rows",
            rg0_start,
            rg0_start + row_group_0.compressed_size() as u64,
            total_rows_task1
        );

        let tasks2 = Box::pin(futures::stream::iter(vec![Ok(task2)])) as FileScanTaskStream;
        let result2 = reader
            .read(tasks2)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let total_rows_task2: usize = result2.iter().map(|b| b.num_rows()).sum();
        println!("Task 2 (bytes {rg1_start}-{file_end}) returned {total_rows_task2} rows");

        assert_eq!(
            total_rows_task1, 100,
            "Task 1 should read only the first row group (100 rows), but got {total_rows_task1} rows"
        );

        assert_eq!(
            total_rows_task2, 200,
            "Task 2 should read only the second+third row groups (200 rows), but got {total_rows_task2} rows"
        );

        // Verify the actual data values are correct (not just the row count)
        if total_rows_task1 > 0 {
            let first_batch = &result1[0];
            let id_col = first_batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let first_val = id_col.value(0);
            let last_val = id_col.value(id_col.len() - 1);
            println!("Task 1 data range: {first_val} to {last_val}");

            assert_eq!(first_val, 0, "Task 1 should start with id=0");
            assert_eq!(last_val, 99, "Task 1 should end with id=99");
        }

        if total_rows_task2 > 0 {
            let first_batch = &result2[0];
            let id_col = first_batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let first_val = id_col.value(0);
            println!("Task 2 first value: {first_val}");

            assert_eq!(first_val, 100, "Task 2 should start with id=100, not id=0");
        }

        // Combining byte-range and predicate row-group filtering should narrow the
        // split's candidates instead of evaluating and intersecting independent lists.
        // This task owns row groups 1 and 2, while the predicate can match row groups
        // 0 and 2. Only row group 2 should be read.
        let predicate = Reference::new("id")
            .less_than(Datum::int(100))
            .or(Reference::new("id").greater_than_or_equal_to(Datum::int(200)))
            .bind(schema.clone(), true)
            .unwrap();

        // Test the pruning decision directly, not just the final filtered rows.
        let parquet_metadata = Arc::new((*metadata).clone());
        let field_id_map = HashMap::from([(1, 0)]);
        for (candidates, expected) in [(vec![1, 2], vec![2]), (vec![0, 1], vec![0])] {
            let selected = ArrowReader::get_selected_row_group_indices(
                &predicate,
                &parquet_metadata,
                &field_id_map,
                schema.as_ref(),
                &candidates,
            )
            .unwrap();
            assert_eq!(selected, expected);
        }

        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(rg1_start)
            .with_length(file_end - rg1_start)
            .with_data_file_path(file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema.clone())
            .with_project_field_ids(vec![1])
            .with_predicate(Some(predicate))
            .with_record_count(Some(200))
            .with_case_sensitive(false)
            .build()
            .unwrap();

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
            .with_row_group_filtering_enabled(true)
            .build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let mut ids = Vec::new();
        for batch in &result {
            let id_col = batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            ids.extend((0..id_col.len()).map(|index| id_col.value(index)));
        }

        assert_eq!(ids, (200..300).collect::<Vec<_>>());
    }

    /// A single data file split into multiple sub-row-group byte ranges (as Spark/Iceberg
    /// planning produces when split-size is smaller than a row group) must still yield each
    /// row exactly once. The previous overlap-based selection let every split whose byte range
    /// touched a row group read it, duplicating rows; ownership by midpoint reads each row group
    /// from exactly one split.
    #[tokio::test]
    async fn test_sub_row_group_splits_do_not_duplicate_rows() {
        use arrow_array::Int32Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/sub_split.parquet");

        // Three row groups of 100 rows each (ids 0..300).
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_row_count(Some(100))
            .build();
        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        for chunk in [0..100, 100..200, 200..300] {
            let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
                Int32Array::from(chunk.collect::<Vec<i32>>()),
            )])
            .unwrap();
            writer.write(&batch).expect("Writing batch");
        }
        writer.close().unwrap();

        let file_size = std::fs::metadata(&file_path).unwrap().len();
        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        // Tile the whole file into 64-byte splits, mirroring Spark's split-size planning, and
        // read every split. A 64-byte split is far smaller than a row group, so each row group
        // is touched by several splits but must be owned (read) by exactly one.
        let mut ids = Vec::new();
        let split_size = 64u64;
        let mut start = 0u64;
        while start < file_size {
            let length = split_size.min(file_size - start);
            let task = FileScanTask::builder()
                .with_file_size_in_bytes(file_size)
                .with_start(start)
                .with_length(length)
                .with_data_file_path(file_path.clone())
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1])
                .with_case_sensitive(false)
                .build()
                .unwrap();

            let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
            let batches = reader
                .clone()
                .read(tasks)
                .unwrap()
                .stream()
                .try_collect::<Vec<RecordBatch>>()
                .await
                .unwrap();

            for batch in &batches {
                let col = batch
                    .column(0)
                    .as_primitive::<arrow_array::types::Int32Type>();
                ids.extend(col.values().iter().copied());
            }

            start += length;
        }

        ids.sort_unstable();
        assert_eq!(
            ids,
            (0..300).collect::<Vec<i32>>(),
            "each row must be read exactly once across all splits, got {} rows",
            ids.len()
        );
    }

    /// When a split boundary lands exactly on a row group's midpoint, half-open ownership
    /// (`start <= midpoint < end`) must hand that row group to the upper split only: the lower
    /// split ends at the midpoint and so excludes it, the upper split starts at the midpoint and
    /// so claims it. Two splits meeting exactly at the middle row group's midpoint must therefore
    /// read every row once, with the middle row group going to the upper split.
    #[tokio::test]
    async fn test_split_boundary_on_row_group_midpoint() {
        use arrow_array::Int32Array;
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/midpoint.parquet");

        // Three row groups of 100 rows each (ids 0..300).
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_row_count(Some(100))
            .build();
        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        for chunk in [0..100, 100..200, 200..300] {
            let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
                Int32Array::from(chunk.collect::<Vec<i32>>()),
            )])
            .unwrap();
            writer.write(&batch).expect("Writing batch");
        }
        writer.close().unwrap();

        // Locate the middle row group's exact midpoint. Row groups are stored back to back after
        // the 4-byte magic header.
        let metadata = SerializedFileReader::new(File::open(&file_path).unwrap())
            .unwrap()
            .metadata()
            .clone();
        assert_eq!(metadata.num_row_groups(), 3);
        let rg1_start = 4 + metadata.row_group(0).compressed_size() as u64;
        let rg1_size = metadata.row_group(1).compressed_size() as u64;
        let rg1_midpoint = rg1_start + rg1_size / 2;
        let file_end = rg1_start + rg1_size + metadata.row_group(2).compressed_size() as u64;

        let file_size = std::fs::metadata(&file_path).unwrap().len();
        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();

        // Two splits meeting exactly at rg1's midpoint. The lower split ends there, the upper
        // starts there; the middle row group must fall to the upper split alone.
        let mut per_split = Vec::new();
        for (start, end) in [(0, rg1_midpoint), (rg1_midpoint, file_end)] {
            let task = FileScanTask::builder()
                .with_file_size_in_bytes(file_size)
                .with_start(start)
                .with_length(end - start)
                .with_data_file_path(file_path.clone())
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1])
                .with_case_sensitive(false)
                .build()
                .unwrap();

            let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
            let batches = reader
                .clone()
                .read(tasks)
                .unwrap()
                .stream()
                .try_collect::<Vec<RecordBatch>>()
                .await
                .unwrap();

            let mut ids = Vec::new();
            for batch in &batches {
                let col = batch
                    .column(0)
                    .as_primitive::<arrow_array::types::Int32Type>();
                ids.extend(col.values().iter().copied());
            }
            per_split.push(ids);
        }

        assert_eq!(
            per_split[0],
            (0..100).collect::<Vec<i32>>(),
            "lower split, ending at rg1's midpoint, must read only rg0"
        );
        assert_eq!(
            per_split[1],
            (100..300).collect::<Vec<i32>>(),
            "upper split, starting at rg1's midpoint, must read rg1 and rg2"
        );
    }

    fn int_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "x",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .unwrap(),
        )
    }

    fn simple_predicate(schema: SchemaRef) -> BoundPredicate {
        Reference::new("x")
            .greater_than(Datum::int(0))
            .bind(schema.clone(), false)
            .unwrap()
    }

    fn field_id_map() -> HashMap<i32, usize> {
        let mut m = HashMap::new();
        m.insert(1_i32, 0_usize);
        m
    }

    fn metadata_no_page_indexes() -> Arc<ParquetMetaData> {
        let msg_type = parse_message_type("message schema { REQUIRED INT32 x; }").unwrap();
        let schema_desc = Arc::new(SchemaDescriptor::new(Arc::new(msg_type)));
        let file_meta = FileMetaData::new(2, 0, None, None, schema_desc.clone(), None);
        Arc::new(ParquetMetaDataBuilder::new(file_meta).build())
    }

    fn metadata_column_index_only() -> Arc<ParquetMetaData> {
        let msg_type = parse_message_type("message schema { REQUIRED INT32 x; }").unwrap();
        let schema_desc = Arc::new(SchemaDescriptor::new(Arc::new(msg_type)));
        let file_meta = FileMetaData::new(2, 0, None, None, schema_desc, None);
        Arc::new(
            ParquetMetaDataBuilder::new(file_meta)
                .set_column_index(Some(vec![]))
                .build(),
        )
    }

    fn metadata_with_both_indexes() -> Arc<ParquetMetaData> {
        let msg_type = parse_message_type("message schema { REQUIRED INT32 x; }").unwrap();
        let schema_desc = Arc::new(SchemaDescriptor::new(Arc::new(msg_type)));
        let file_meta = FileMetaData::new(2, 0, None, None, schema_desc, None);
        Arc::new(
            ParquetMetaDataBuilder::new(file_meta)
                .set_column_index(Some(vec![]))
                .set_offset_index(Some(vec![]))
                .build(),
        )
    }

    /// A `FileScanTask` built or deserialized outside `TableScan` carries an
    /// un-normalized predicate, so a raw `Not(...)` can reach the reader.
    #[tokio::test]
    async fn test_not_predicate_read_returns_matching_rows() {
        use arrow_array::Int32Array;

        let schema = int_schema();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("x", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/not_pred.parquet");

        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), None).unwrap();
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Int32Array::from(
            (0..=10).collect::<Vec<i32>>(),
        ))])
        .unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();

        // NOT(x > 5) == x <= 5, bound without rewriting, as a non-TableScan caller
        // would supply it.
        let predicate = (!Reference::new("x").greater_than(Datum::int(5)))
            .bind(schema.clone(), false)
            .unwrap();
        assert!(
            matches!(predicate, BoundPredicate::Not(_)),
            "test must drive a raw Not(...) node"
        );

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
            .with_row_group_filtering_enabled(true)
            .build();

        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema.clone())
            .with_project_field_ids(vec![1])
            .with_predicate(Some(predicate))
            .with_case_sensitive(false)
            .build()
            .unwrap();

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let mut values = Vec::new();
        for batch in &batches {
            let col = batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            values.extend(col.values().iter().copied());
        }

        assert_eq!(values, (0..=5).collect::<Vec<i32>>());
    }

    #[tokio::test]
    async fn test_not_predicate_read_through_page_index() {
        use arrow_array::Int32Array;
        use parquet::file::reader::{FileReader, SerializedFileReader};
        use parquet::file::serialized_reader::ReadOptionsBuilder;

        let schema = int_schema();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("x", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/not_pred_pages.parquet");

        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), None).unwrap();
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Int32Array::from(
            (0..100).collect::<Vec<i32>>(),
        ))])
        .unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();

        {
            let f = File::open(&file_path).unwrap();
            let opts = ReadOptionsBuilder::new().with_page_index().build();
            let rdr = SerializedFileReader::new_with_options(f, opts).unwrap();
            assert!(
                rdr.metadata().column_index().is_some(),
                "fixture must carry a column index so the page-index path runs"
            );
        }

        // NOT(x >= 50) == x < 50, as a non-TableScan caller would supply it.
        let predicate = (!Reference::new("x").greater_than_or_equal_to(Datum::int(50)))
            .bind(schema.clone(), false)
            .unwrap();
        assert!(matches!(predicate, BoundPredicate::Not(_)));

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
            .with_row_group_filtering_enabled(false)
            .with_row_selection_enabled(true)
            .build();

        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path)
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema.clone())
            .with_project_field_ids(vec![1])
            .with_predicate(Some(predicate))
            .with_case_sensitive(false)
            .build()
            .unwrap();

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let mut values = Vec::new();
        for batch in &batches {
            let col = batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            values.extend(col.values().iter().copied());
        }

        assert_eq!(values, (0..50).collect::<Vec<i32>>());
    }

    #[test]
    fn test_row_group_pruning_rejects_invalid_candidate() {
        let schema = int_schema();
        let predicate = simple_predicate(schema.clone());
        let metadata = metadata_no_page_indexes();

        let err = ArrowReader::get_selected_row_group_indices(
            &predicate,
            &metadata,
            &field_id_map(),
            schema.as_ref(),
            &[0],
        )
        .unwrap_err();

        assert_eq!(err.kind(), crate::ErrorKind::DataInvalid);
    }

    /// Testing suite regarding: https://github.com/apache/iceberg-rust/issues/2452
    /// Testing when: both indices are absent, some present, both present
    #[test]
    fn test_absent_column_index_returns_ok_none() {
        let schema = int_schema();
        let predicate = simple_predicate(schema.clone());
        let metadata = metadata_no_page_indexes();
        let field_id_map = field_id_map();

        let result = ArrowReader::get_row_selection_for_filter_predicate(
            &predicate,
            &metadata,
            &None,
            &field_id_map,
            schema.as_ref(),
        );

        assert!(
            result.is_ok(),
            "expected Ok(_), got Err: {:?}",
            result.unwrap_err()
        );
        assert!(
            result.unwrap().is_none(),
            "expected Ok(None) when column index is absent"
        );
    }

    #[test]
    fn test_absent_offset_index_returns_ok_none() {
        let schema = int_schema();
        let predicate = simple_predicate(schema.clone());
        let metadata = metadata_column_index_only();
        let field_id_map = field_id_map();

        let result = ArrowReader::get_row_selection_for_filter_predicate(
            &predicate,
            &metadata,
            &None,
            &field_id_map,
            schema.as_ref(),
        );

        assert!(
            result.is_ok(),
            "expected Ok(_), got Err: {:?}",
            result.unwrap_err()
        );
        assert!(
            result.unwrap().is_none(),
            "expected Ok(None) when offset index is absent"
        );
    }

    #[test]
    fn test_absent_column_index_with_selected_row_groups_returns_ok_none() {
        let schema = int_schema();
        let predicate = simple_predicate(schema.clone());
        let metadata = metadata_no_page_indexes();
        let field_id_map = field_id_map();
        let selected = Some(vec![0usize, 1]);

        let result = ArrowReader::get_row_selection_for_filter_predicate(
            &predicate,
            &metadata,
            &selected,
            &field_id_map,
            schema.as_ref(),
        );

        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "absent column index must short-circuit before selected_row_groups is inspected"
        );
    }

    #[test]
    fn test_absent_offset_index_with_selected_row_groups_returns_ok_none() {
        let schema = int_schema();
        let predicate = simple_predicate(schema.clone());
        let metadata = metadata_column_index_only();
        let field_id_map = field_id_map();
        let selected = Some(vec![0usize]);

        let result = ArrowReader::get_row_selection_for_filter_predicate(
            &predicate,
            &metadata,
            &selected,
            &field_id_map,
            schema.as_ref(),
        );

        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "absent offset index must short-circuit before selected_row_groups is inspected"
        );
    }

    #[test]
    fn test_absent_column_index_with_empty_selected_row_groups_returns_ok_none() {
        let schema = int_schema();
        let predicate = simple_predicate(schema.clone());
        let metadata = metadata_no_page_indexes();
        let field_id_map = field_id_map();
        let selected: Option<Vec<usize>> = Some(vec![]);

        let result = ArrowReader::get_row_selection_for_filter_predicate(
            &predicate,
            &metadata,
            &selected,
            &field_id_map,
            schema.as_ref(),
        );

        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "column index check must fire before the empty-selected-row-groups branch"
        );
    }

    #[test]
    fn test_both_indexes_present_empty_selected_row_groups_returns_ok_some_empty() {
        let schema = int_schema();
        let predicate = simple_predicate(schema.clone());
        let metadata = metadata_with_both_indexes();
        let field_id_map = field_id_map();
        let selected: Option<Vec<usize>> = Some(vec![]);

        let result = ArrowReader::get_row_selection_for_filter_predicate(
            &predicate,
            &metadata,
            &selected,
            &field_id_map,
            schema.as_ref(),
        );

        assert!(
            result.is_ok(),
            "expected Ok(_), got Err: {:?}",
            result.unwrap_err()
        );

        let row_selection = result.unwrap().expect(
            "expected Ok(Some(_)) when both indexes are present and all row groups are filtered",
        );

        assert_eq!(
            row_selection.row_count(),
            0,
            "RowSelection must be empty (zero rows selected) when selected_row_groups is empty"
        );
    }

    /// Full-suite regression test for issue: https://github.com/apache/iceberg-rust/issues/2452
    #[tokio::test]
    async fn test_scan_without_page_indexes_does_not_error() {
        // Building schema (both iceberg and arrow for RecordBatch)
        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!("{}/data.parquet", tmp_dir.path().to_str().unwrap());

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            // Disabling page statistics
            .set_statistics_enabled(EnabledStatistics::None)
            .build();

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("alice"),
                Some("bob"),
                None,
                Some("dana"),
                Some("eve"),
            ])) as ArrayRef,
        ])
        .unwrap();

        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Truly exercising a file without column/offset index
        {
            use parquet::file::reader::{FileReader, SerializedFileReader};
            let f = File::open(&file_path).unwrap();
            let rdr = SerializedFileReader::new(f).unwrap();
            assert!(
                rdr.metadata().column_index().is_none(),
                "test fixture must produce a file without a column index"
            );
            assert!(
                rdr.metadata().offset_index().is_none(),
                "test fixture must a product a file without offset index"
            )
        }

        // Predicate: id > 2
        let predicate = Reference::new("id")
            .greater_than(Datum::int(2))
            .bind(iceberg_schema.clone(), false)
            .unwrap();

        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io.clone(), Runtime::current())
            // Enabling row selection ()
            .with_row_selection_enabled(true)
            .build();

        let file_size = std::fs::metadata(&file_path).unwrap().len();
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(file_size)
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(iceberg_schema.clone())
            .with_project_field_ids(vec![1, 2])
            .with_predicate(Some(predicate))
            .with_case_sensitive(false)
            .build()
            .unwrap();

        let stream = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader
            .read(stream)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();

        assert_eq!(
            ids,
            vec![3, 4, 5],
            "predicate must still be enforced via Arrow row filter even without page indexes"
        );

        // Absent index + position delete field present
        let pos_del_path = format!("{}/pos-del.parquet", tmp_dir.path().to_str().unwrap());

        // Build the position delete Arrow schema (standard Iceberg layout).
        let pos_del_arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ]));

        let pos_del_batch = RecordBatch::try_new(pos_del_arrow_schema.clone(), vec![
            // Both deletions reference the same data file
            Arc::new(StringArray::from(vec![
                file_path.as_str(),
                file_path.as_str(),
            ])) as ArrayRef,
            // Delete by index - index-0 (`1` in test case) and index-2 (`3` in test case)
            Arc::new(Int64Array::from(vec![0i64, 2i64])) as ArrayRef,
        ])
        .unwrap();

        // Write position delete file also without indices
        let pos_del_props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_statistics_enabled(EnabledStatistics::None)
            .build();

        let pos_del_file = File::create(&pos_del_path).unwrap();
        let mut pos_del_writer = ArrowWriter::try_new(
            pos_del_file,
            pos_del_arrow_schema.clone(),
            Some(pos_del_props),
        )
        .unwrap();
        pos_del_writer.write(&pos_del_batch).unwrap();
        pos_del_writer.close().unwrap();

        // Predicate: id > 1 (note that `3` was also removed by position delete)
        let predicate_sub2 = Reference::new("id")
            .greater_than(Datum::int(1))
            .bind(iceberg_schema.clone(), false)
            .unwrap();

        let reader_sub2 = ArrowReaderBuilder::new(file_io.clone(), Runtime::current())
            .with_row_selection_enabled(true)
            .build();

        let task_sub2 = FileScanTask::builder()
            .with_file_size_in_bytes(file_size)
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(iceberg_schema.clone())
            .with_project_field_ids(vec![1, 2])
            .with_predicate(Some(predicate_sub2))
            .with_deletes(vec![
                FileScanTaskDeleteFile::builder()
                    .with_file_path(pos_del_path.clone())
                    .with_file_type(DataContentType::PositionDeletes)
                    .with_file_format(DataFileFormat::Parquet)
                    .with_partition_spec_id(0)
                    .with_file_size_in_bytes(std::fs::metadata(&pos_del_path).unwrap().len())
                    .build()
                    .unwrap(),
            ])
            .with_case_sensitive(false)
            .build()
            .unwrap();

        let stream_sub2 =
            Box::pin(futures::stream::iter(vec![Ok(task_sub2)])) as FileScanTaskStream;
        let batches_sub2: Vec<RecordBatch> = reader_sub2
            .read(stream_sub2)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();

        let ids_sub2: Vec<i32> = batches_sub2
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();

        assert_eq!(
            ids_sub2,
            vec![2, 4, 5],
            "positional deletes must be applied correctly even when page indexes are absent"
        );
    }

    // Bloom filter pushdown: on-vs-off equivalence
    // Pushdown must never change results. An encoding bug in the probe shows up as
    // rows the bloom filter drops and the row filter keeps, so every case below
    // reads the same file twice and compares. Fixtures spread each row group's
    // values across the whole domain, so min/max statistics cannot prune and any
    // reduction in bytes read is attributable to the bloom filter.

    const ROWS_PER_GROUP: i32 = 20_000;
    const GROUPS: i32 = 3;
    /// Gap between consecutive values in a row group, so a value can be absent from
    /// the file yet still sit inside every row group's min/max range.
    const STRIDE: i32 = 30;

    /// The `i`th value of row group `group`, spread across the whole domain.
    fn interleaved(group: i32, i: i32) -> i32 {
        i * STRIDE + group
    }

    fn field_with_id(name: &str, ty: DataType, id: i32) -> Field {
        Field::new(name, ty, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }

    fn eqd_field(name: &str, dt: DataType, id: i32, nullable: bool) -> Field {
        Field::new(name, dt, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }

    /// Writes one row group per batch, optionally with bloom filters.
    fn write_row_groups(
        path: &str,
        arrow_schema: Arc<ArrowSchema>,
        row_groups: Vec<RecordBatch>,
        with_bloom_filters: bool,
    ) {
        let mut props = WriterProperties::builder().set_compression(Compression::UNCOMPRESSED);
        if with_bloom_filters {
            props = props
                .set_bloom_filter_enabled(true)
                .set_bloom_filter_max_ndv(ROWS_PER_GROUP as u64);
        }

        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props.build())).unwrap();
        for batch in row_groups {
            writer.write(&batch).unwrap();
            // Force a row group boundary so each batch is independently prunable.
            writer.flush().unwrap();
        }
        writer.close().unwrap();
    }

    async fn read_once(
        reader: ArrowReader,
        file_path: &str,
        schema: Arc<Schema>,
        project_field_ids: Vec<i32>,
        predicate: Predicate,
        partition: Option<(Arc<PartitionSpec>, Struct)>,
        deletes: Vec<FileScanTaskDeleteFile>,
    ) -> (Vec<RecordBatch>, u64) {
        let (partition_spec, partition) = partition.unzip();
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(file_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(file_path.to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema.clone())
            .with_project_field_ids(project_field_ids)
            .with_predicate(Some(predicate.bind(schema, true).unwrap()))
            .with_deletes(deletes)
            .with_partition(partition)
            .with_partition_spec(partition_spec)
            .with_case_sensitive(false)
            .build()
            .unwrap();

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result = reader.read(tasks).unwrap();
        let metrics = result.metrics().clone();
        let batches: Vec<RecordBatch> = result.stream().try_collect().await.unwrap();

        (batches, metrics.bytes_read())
    }

    /// Batch boundaries differ when fewer row groups are read, so collapse to a
    /// single batch before comparing. `None` means no rows at all.
    fn collapse(batches: &[RecordBatch]) -> Option<RecordBatch> {
        let first = batches.first()?;
        Some(arrow_select::concat::concat_batches(&first.schema(), batches).unwrap())
    }

    /// Reads the file with pushdown on and off, asserts the rows are identical, and
    /// returns the rows plus (bytes_on, bytes_off).
    async fn assert_pushdown_agrees(
        case: &str,
        file_path: &str,
        schema: Arc<Schema>,
        project_field_ids: Vec<i32>,
        predicate: Predicate,
    ) -> (Option<RecordBatch>, u64, u64) {
        let read = |bloom_enabled| {
            let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
                .with_bloom_filter_enabled(bloom_enabled)
                // Keep the fixed footer prefetch small so the byte measurement reflects
                // row group I/O rather than a constant metadata read.
                .with_metadata_size_hint(8 * 1024)
                .build();
            read_once(
                reader,
                file_path,
                schema.clone(),
                project_field_ids.clone(),
                predicate.clone(),
                None,
                vec![],
            )
        };
        let (off, bytes_off) = read(false).await;
        let (on, bytes_on) = read(true).await;

        let off = collapse(&off);
        let on = collapse(&on);
        assert_eq!(
            on, off,
            "{case}: bloom filter pushdown changed the rows returned"
        );

        (on, bytes_on, bytes_off)
    }

    fn rows(batch: &Option<RecordBatch>) -> usize {
        batch.as_ref().map_or(0, |b| b.num_rows())
    }

    fn value_schema(iceberg_type: Type) -> Arc<Schema> {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![NestedField::required(1, "v", iceberg_type).into()])
                .build()
                .unwrap(),
        )
    }

    /// Writes a single-column file, one row group per group index.
    fn write_value_fixture(
        path: &str,
        data_type: DataType,
        make_group: impl Fn(i32) -> ArrayRef,
        with_bloom_filters: bool,
    ) {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![field_with_id("v", data_type, 1)]));
        let row_groups = (0..GROUPS)
            .map(|g| RecordBatch::try_new(arrow_schema.clone(), vec![make_group(g)]).unwrap())
            .collect();
        write_row_groups(path, arrow_schema, row_groups, with_bloom_filters);
    }

    /// The core check for one physical encoding: a value that is present must
    /// survive pushdown, and one that is absent must prune every row group. The
    /// first catches a probe that encodes wrongly and drops real rows; the second
    /// catches a probe that silently never prunes.
    async fn assert_prunes_and_agrees(
        case: &str,
        file_path: &str,
        schema: Arc<Schema>,
        present: Datum,
        absent: Datum,
    ) {
        let (on, bytes_on, bytes_off) = assert_pushdown_agrees(
            &format!("{case}/present"),
            file_path,
            schema.clone(),
            vec![1],
            Reference::new("v").equal_to(present),
        )
        .await;
        assert_eq!(rows(&on), 1, "{case}/present: expected one matching row");
        assert!(
            bytes_on < bytes_off,
            "{case}/present: pushdown must skip row groups: {bytes_on} vs {bytes_off}"
        );

        let (on, bytes_on, bytes_off) = assert_pushdown_agrees(
            &format!("{case}/absent"),
            file_path,
            schema,
            vec![1],
            Reference::new("v").equal_to(absent),
        )
        .await;
        assert_eq!(rows(&on), 0, "{case}/absent: expected no matching rows");
        assert!(
            bytes_on * 2 < bytes_off,
            "{case}/absent: pruning every row group should cut reads sharply: \
             {bytes_on} vs {bytes_off}"
        );
    }

    /// Value present in exactly one row group: `interleaved(1, 51)`.
    const PRESENT_INT: i32 = 51 * STRIDE + 1;
    /// Not congruent to any group index mod STRIDE, so absent from the file while
    /// still inside every row group's min/max range.
    const ABSENT_INT: i32 = 7;

    fn int_group(g: i32) -> ArrayRef {
        Arc::new(Int32Array::from(
            (0..ROWS_PER_GROUP)
                .map(|i| interleaved(g, i))
                .collect::<Vec<_>>(),
        ))
    }

    #[tokio::test]
    async fn test_bloom_pushdown_int32_eq() {
        let tmp = TempDir::new().unwrap();
        let path = format!("{}/int32.parquet", tmp.path().to_str().unwrap());
        write_value_fixture(&path, DataType::Int32, int_group, true);

        assert_prunes_and_agrees(
            "int32",
            &path,
            value_schema(Type::Primitive(PrimitiveType::Int)),
            Datum::int(PRESENT_INT),
            Datum::int(ABSENT_INT),
        )
        .await;
    }

    /// `IN` takes a different evaluator path from `eq`: it must keep the row group
    /// when any literal might be present, and prune only when all are absent.
    #[tokio::test]
    async fn test_bloom_pushdown_int32_in() {
        let tmp = TempDir::new().unwrap();
        let path = format!("{}/int32_in.parquet", tmp.path().to_str().unwrap());
        write_value_fixture(&path, DataType::Int32, int_group, true);
        let schema = value_schema(Type::Primitive(PrimitiveType::Int));

        // One present literal keeps its row group; the absent one must not suppress it.
        let (on, bytes_on, bytes_off) = assert_pushdown_agrees(
            "int32_in/mixed",
            &path,
            schema.clone(),
            vec![1],
            Reference::new("v").is_in([Datum::int(PRESENT_INT), Datum::int(ABSENT_INT)]),
        )
        .await;
        assert_eq!(rows(&on), 1);
        assert!(bytes_on < bytes_off, "{bytes_on} vs {bytes_off}");

        // All literals absent: every row group prunes.
        let (on, bytes_on, bytes_off) = assert_pushdown_agrees(
            "int32_in/all_absent",
            &path,
            schema,
            vec![1],
            Reference::new("v").is_in([Datum::int(ABSENT_INT), Datum::int(ABSENT_INT + 1)]),
        )
        .await;
        assert_eq!(rows(&on), 0);
        assert!(bytes_on * 2 < bytes_off, "{bytes_on} vs {bytes_off}");
    }

    #[tokio::test]
    async fn test_bloom_pushdown_string_eq() {
        let tmp = TempDir::new().unwrap();
        let path = format!("{}/string.parquet", tmp.path().to_str().unwrap());
        write_value_fixture(
            &path,
            DataType::Utf8,
            |g| {
                Arc::new(StringArray::from(
                    (0..ROWS_PER_GROUP)
                        .map(|i| format!("{:016}", interleaved(g, i)))
                        .collect::<Vec<_>>(),
                ))
            },
            true,
        );

        assert_prunes_and_agrees(
            "string",
            &path,
            value_schema(Type::Primitive(PrimitiveType::String)),
            Datum::string(format!("{PRESENT_INT:016}")),
            Datum::string(format!("{ABSENT_INT:016}")),
        )
        .await;
    }

    /// Decimal precision decides the Parquet physical type, and the probe has to be
    /// encoded to match: precision 9 -> INT32, 15 -> INT64, 25 -> FIXED_LEN_BYTE_ARRAY.
    /// Each width is a separate arm of `check_in_bloom_filter`.
    async fn check_decimal_width(name: &str, precision: u32) {
        const SCALE: u32 = 2;
        let tmp = TempDir::new().unwrap();
        let path = format!("{}/{name}.parquet", tmp.path().to_str().unwrap());

        write_value_fixture(
            &path,
            DataType::Decimal128(precision as u8, SCALE as i8),
            |g| {
                let values: Vec<i128> = (0..ROWS_PER_GROUP)
                    .map(|i| i128::from(interleaved(g, i)))
                    .collect();
                Arc::new(
                    Decimal128Array::from(values)
                        .with_precision_and_scale(precision as u8, SCALE as i8)
                        .unwrap(),
                )
            },
            true,
        );

        let datum = |mantissa: i32| {
            Datum::decimal_with_precision(
                crate::spec::decimal_utils::decimal_from_i128_with_scale(
                    i128::from(mantissa),
                    SCALE,
                ),
                precision,
            )
            .unwrap()
        };

        assert_prunes_and_agrees(
            name,
            &path,
            value_schema(Type::Primitive(PrimitiveType::Decimal {
                precision,
                scale: SCALE,
            })),
            datum(PRESENT_INT),
            datum(ABSENT_INT),
        )
        .await;
    }

    #[tokio::test]
    async fn test_bloom_pushdown_decimal_int32() {
        check_decimal_width("decimal_int32", 9).await;
    }

    #[tokio::test]
    async fn test_bloom_pushdown_decimal_int64() {
        check_decimal_width("decimal_int64", 15).await;
    }

    #[tokio::test]
    async fn test_bloom_pushdown_decimal_fixed_len_byte_array() {
        check_decimal_width("decimal_flba", 25).await;
    }

    /// Widening a decimal's precision does not rewrite existing files, so the file
    /// keeps its original physical width while the bound literal arrives at the new
    /// precision. Deriving the probe encoding from the schema instead of the file
    /// would miss every entry the writer inserted and prune matching row groups.
    #[tokio::test]
    async fn test_bloom_pushdown_decimal_widened_precision() {
        const SCALE: u32 = 2;
        let tmp = TempDir::new().unwrap();
        let path = format!("{}/decimal_widened.parquet", tmp.path().to_str().unwrap());

        // File written at precision 9 -> INT32.
        write_value_fixture(
            &path,
            DataType::Decimal128(9, SCALE as i8),
            |g| {
                let values: Vec<i128> = (0..ROWS_PER_GROUP)
                    .map(|i| i128::from(interleaved(g, i)))
                    .collect();
                Arc::new(
                    Decimal128Array::from(values)
                        .with_precision_and_scale(9, SCALE as i8)
                        .unwrap(),
                )
            },
            true,
        );

        // Table schema has since widened to precision 30.
        let schema = value_schema(Type::Primitive(PrimitiveType::Decimal {
            precision: 30,
            scale: SCALE,
        }));
        let datum = |mantissa: i32| {
            Datum::decimal_with_precision(
                crate::spec::decimal_utils::decimal_from_i128_with_scale(
                    i128::from(mantissa),
                    SCALE,
                ),
                30,
            )
            .unwrap()
        };

        assert_prunes_and_agrees(
            "decimal_widened",
            &path,
            schema,
            datum(PRESENT_INT),
            datum(ABSENT_INT),
        )
        .await;
    }

    /// `Sbbf` hashes raw IEEE bytes, so `-0.0` and `0.0` are distinct entries. Arrow's
    /// float equality is bitwise too, for an independent reason: `ArrowNativeTypeOp::is_eq`
    /// implements total-order (`total_cmp`) semantics as `to_bits() == to_bits()`, so
    /// `-0.0 != 0.0` and `NaN == NaN` there. That agreement is what makes it safe to prune
    /// a row group holding only the opposite zero: if `is_eq` ever became IEEE-lenient,
    /// those pruned rows would be rows the row filter should have returned. A probe that
    /// canonicalized the sign instead only over-keeps, costing pruning rather than
    /// correctness — but it would also hide such a change from this test.
    ///
    /// Each row group pairs its zero with a larger filler value so its min/max range spans
    /// the other zero. Neither statistics path can prune it, so the bloom-off read really
    /// does evaluate `is_eq` against `ROWS_PER_GROUP - 1` copies of the opposite zero: that
    /// arm is the control pinning arrow's semantics, and IEEE-lenient equality would double
    /// the row count it returns. Pairing with a filler also avoids relying on the writer
    /// widening a single-value group's bounds to `[-0.0, +0.0]`.
    #[tokio::test]
    async fn test_bloom_pushdown_float_signed_zero() {
        const FILLER: f32 = 5.0;
        let tmp = TempDir::new().unwrap();
        let path = format!("{}/float_zero.parquet", tmp.path().to_str().unwrap());

        // Row group 0 holds -0.0 and never +0.0; row group 1 the reverse; row group 2
        // neither zero.
        write_value_fixture(
            &path,
            DataType::Float32,
            |g| {
                let zero = match g {
                    0 => -0.0f32,
                    1 => 0.0f32,
                    _ => 7.5f32,
                };
                let mut values = vec![zero; ROWS_PER_GROUP as usize - 1];
                values.push(FILLER);
                Arc::new(Float32Array::from(values))
            },
            true,
        );

        let schema = value_schema(Type::Primitive(PrimitiveType::Float));
        let zero_rows = ROWS_PER_GROUP as usize - 1;

        for (label, probe) in [("positive_zero", 0.0f32), ("negative_zero", -0.0f32)] {
            let (on, _, _) = assert_pushdown_agrees(
                label,
                &path,
                schema.clone(),
                vec![1],
                Reference::new("v").equal_to(Datum::float(probe)),
            )
            .await;
            // Only the group holding this exact zero matches, on both paths.
            assert_eq!(
                rows(&on),
                zero_rows,
                "{label}: expected only the row group holding this zero to match"
            );
        }
    }

    /// A file with no bloom filters at all must read identically with the option on,
    /// exercising the conservative might-match path end to end.
    #[tokio::test]
    async fn test_bloom_pushdown_file_without_bloom_filters() {
        let tmp = TempDir::new().unwrap();
        let path = format!("{}/no_filters.parquet", tmp.path().to_str().unwrap());
        write_value_fixture(&path, DataType::Int32, int_group, false);

        let (on, _, _) = assert_pushdown_agrees(
            "no_filters",
            &path,
            value_schema(Type::Primitive(PrimitiveType::Int)),
            vec![1],
            Reference::new("v").equal_to(Datum::int(PRESENT_INT)),
        )
        .await;
        assert_eq!(rows(&on), 1);
    }

    fn eqd_write(path: &str, schema: Arc<ArrowSchema>, columns: Vec<ArrayRef>) {
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let file = File::create(path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn eqd_delete_file(path: &str, equality_ids: Vec<i32>) -> FileScanTaskDeleteFile {
        FileScanTaskDeleteFile::builder()
            .with_file_path(path.to_string())
            .with_file_size_in_bytes(std::fs::metadata(path).unwrap().len())
            .with_file_type(DataContentType::EqualityDeletes)
            .with_file_format(DataFileFormat::Parquet)
            .with_equality_ids(Some(equality_ids))
            .with_partition_spec_id(0)
            .build()
            .unwrap()
    }

    async fn eqd_read(
        data_path: &str,
        table_schema: SchemaRef,
        project_field_ids: Vec<i32>,
        deletes: Vec<FileScanTaskDeleteFile>,
    ) -> Vec<RecordBatch> {
        eqd_read_with(
            data_path,
            table_schema,
            project_field_ids,
            deletes,
            None,
            false,
        )
        .await
    }

    /// `eqd_read` plus the two things that interact with equality-delete predicates: a
    /// scan predicate (pushed as a second `ArrowPredicate`) and the row-selection path.
    async fn eqd_read_with(
        data_path: &str,
        table_schema: SchemaRef,
        project_field_ids: Vec<i32>,
        deletes: Vec<FileScanTaskDeleteFile>,
        predicate: Option<BoundPredicate>,
        row_selection_enabled: bool,
    ) -> Vec<RecordBatch> {
        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io, Runtime::current())
            .with_row_selection_enabled(row_selection_enabled)
            .build();
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(data_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(data_path.to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(table_schema)
            .with_project_field_ids(project_field_ids)
            .with_deletes(deletes)
            .with_case_sensitive(false)
            .with_predicate(predicate)
            .build()
            .unwrap();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap()
    }

    fn eqd_collect_i64(batches: &[RecordBatch], col: usize) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(col)
                    .as_primitive::<Int64Type>()
                    .iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    // A single-column equality delete removes exactly the matching rows.
    #[tokio::test]
    async fn test_eq_delete_single_column_filters_matching_rows() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "val", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![
                eqd_field("id", DataType::Int64, 1, false),
                eqd_field("val", DataType::Utf8, 2, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])) as ArrayRef,
            ],
        );

        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![3])) as ArrayRef],
        );

        let result = eqd_read(&data_path, table_schema, vec![1, 2], vec![eqd_delete_file(
            &del_path,
            vec![1],
        )])
        .await;

        assert_eq!(eqd_collect_i64(&result, 0), vec![1, 2, 4, 5]);
    }

    // A multi-column key deletes only exact tuple matches and a row with a null in a key column
    // (and a row differing in any column) is kept.
    #[tokio::test]
    async fn test_eq_delete_multi_column_keeps_null_and_partial_matches() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "status", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![
                eqd_field("id", DataType::Int64, 1, false),
                eqd_field("status", DataType::Utf8, 2, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![2, 2, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("X"),
                    Some("Y"),
                    None,
                    Some("X"),
                ])) as ArrayRef,
            ],
        );

        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![
                eqd_field("id", DataType::Int64, 1, false),
                eqd_field("status", DataType::Utf8, 2, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("X")])) as ArrayRef,
            ],
        );

        let result = eqd_read(&data_path, table_schema, vec![1, 2], vec![eqd_delete_file(
            &del_path,
            vec![1, 2],
        )])
        .await;

        let mut pairs = Vec::new();
        for b in &result {
            let ids: Vec<i64> = b
                .column(0)
                .as_primitive::<Int64Type>()
                .iter()
                .flatten()
                .collect();
            let statuses: Vec<Option<String>> = b
                .column(1)
                .as_string::<i32>()
                .iter()
                .map(|o| o.map(|s| s.to_string()))
                .collect();
            for (id, status) in ids.into_iter().zip(statuses) {
                pairs.push((id, status));
            }
        }

        assert_eq!(pairs, vec![
            (2, Some("Y".to_string())),
            (2, None),
            (3, Some("X".to_string())),
        ]);
    }

    // The data file stored `id` as int32 but the table type is long, so it must be promoted
    // to the table type.
    #[tokio::test]
    async fn test_eq_delete_promotes_data_type_before_probe() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int32,
                1,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        );

        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![3])) as ArrayRef],
        );

        let result = eqd_read(&data_path, table_schema, vec![1], vec![eqd_delete_file(
            &del_path,
            vec![1],
        )])
        .await;

        assert_eq!(eqd_collect_i64(&result, 0), vec![1, 2]);
    }

    const MICROS_PER_DAY: i64 = 86_400_000_000;

    /// Writes a file that stores `d` as `date` for 2020-01-01 and 2020-01-02 and returns its
    /// path with a table schema that promotes `d` to `timestamp`.
    fn write_date_file_promoted_to_timestamp(loc: &str) -> (String, SchemaRef) {
        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "d",
                DataType::Date32,
                1,
                false,
            )])),
            vec![Arc::new(Date32Array::from(vec![18262, 18263])) as ArrayRef],
        );
        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "d", Type::Primitive(PrimitiveType::Timestamp)).into(),
                ])
                .build()
                .unwrap(),
        );
        (data_path, table_schema)
    }

    fn collect_timestamp_micros(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_primitive::<TimestampMicrosecondType>()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    // A filter on a column promoted from `date` to `timestamp` compares the stored dates as
    // timestamps, not the literal truncated to a date.
    #[tokio::test]
    async fn test_filter_on_date_column_promoted_to_timestamp() {
        let tmp = TempDir::new().unwrap();
        let (data_path, table_schema) =
            write_date_file_promoted_to_timestamp(tmp.path().to_str().unwrap());
        let (day1, day2) = (18262 * MICROS_PER_DAY, 18263 * MICROS_PER_DAY);
        let noon = Datum::timestamp_micros(day1 + MICROS_PER_DAY / 2);

        for (predicate, expected) in [
            (Reference::new("d").less_than(noon.clone()), vec![day1]),
            (
                Reference::new("d").less_than_or_equal_to(noon.clone()),
                vec![day1],
            ),
            (Reference::new("d").greater_than(noon.clone()), vec![day2]),
            (
                Reference::new("d").greater_than_or_equal_to(noon.clone()),
                vec![day2],
            ),
            (Reference::new("d").equal_to(noon.clone()), vec![]),
            (Reference::new("d").not_equal_to(noon.clone()), vec![
                day1, day2,
            ]),
            (Reference::new("d").is_in([noon.clone()]), vec![]),
            (Reference::new("d").is_not_in([noon.clone()]), vec![
                day1, day2,
            ]),
        ] {
            let result = eqd_read_with(
                &data_path,
                table_schema.clone(),
                vec![1],
                vec![],
                Some(predicate.bind(table_schema.clone(), true).unwrap()),
                false,
            )
            .await;

            assert_eq!(
                collect_timestamp_micros(&result),
                expected,
                "predicate={predicate}"
            );
        }
    }

    // A filter on a column promoted from `int` to `long` compares with a literal outside the
    // `int` range.
    #[tokio::test]
    async fn test_filter_on_int_column_promoted_to_long_with_literal_beyond_int_range() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();
        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int32,
                1,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
        );
        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );
        let large = Datum::long(1_i64 << 40);

        for (predicate, expected) in [
            (Reference::new("id").less_than(large.clone()), vec![1, 2]),
            (Reference::new("id").greater_than(large.clone()), vec![]),
            (
                Reference::new("id").is_not_in([large.clone(), Datum::long(1)]),
                vec![2],
            ),
        ] {
            let result = eqd_read_with(
                &data_path,
                table_schema.clone(),
                vec![1],
                vec![],
                Some(predicate.bind(table_schema.clone(), true).unwrap()),
                false,
            )
            .await;

            assert_eq!(
                eqd_collect_i64(&result, 0),
                expected,
                "predicate={predicate}"
            );
        }
    }

    // An equality delete keyed on a column promoted from `date` to `timestamp` matches the
    // stored dates read as timestamps.
    #[tokio::test]
    async fn test_eq_delete_on_date_column_promoted_to_timestamp() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();
        let (data_path, table_schema) = write_date_file_promoted_to_timestamp(loc);

        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "d",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                1,
                false,
            )])),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![
                18263 * MICROS_PER_DAY,
            ])) as ArrayRef],
        );

        let result = eqd_read(&data_path, table_schema, vec![1], vec![eqd_delete_file(
            &del_path,
            vec![1],
        )])
        .await;

        assert_eq!(collect_timestamp_micros(&result), vec![
            18262 * MICROS_PER_DAY
        ]);
    }

    // Two delete files that key on different columns each apply independently: a row is deleted
    // if it matches either.
    #[tokio::test]
    async fn test_eq_delete_distinct_layouts_apply_independently() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(2, "id2", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(3, "val", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![
                eqd_field("id", DataType::Int64, 1, false),
                eqd_field("id2", DataType::Int64, 2, false),
                eqd_field("val", DataType::Utf8, 3, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ],
        );

        let del_a = format!("{loc}/eq-del-a.parquet");
        eqd_write(
            &del_a,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
        );
        let del_b = format!("{loc}/eq-del-b.parquet");
        eqd_write(
            &del_b,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id2",
                DataType::Int64,
                2,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![20])) as ArrayRef],
        );

        let result = eqd_read(&data_path, table_schema, vec![1, 2, 3], vec![
            eqd_delete_file(&del_a, vec![1]),
            eqd_delete_file(&del_b, vec![2]),
        ])
        .await;

        assert_eq!(eqd_collect_i64(&result, 0), vec![3]);
    }

    // A scan predicate and an equality delete are pushed as two separate `ArrowPredicate`s
    // (they used to be "AND-ed" into one bound `Predicate`), so the result must be the
    // intersection of both.
    #[tokio::test]
    async fn test_eq_delete_with_scan_predicate_intersects() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef],
        );

        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![2, 5])) as ArrayRef],
        );

        for row_selection_enabled in [false, true] {
            let predicate = Reference::new("id")
                .greater_than_or_equal_to(Datum::long(3))
                .bind(table_schema.clone(), false)
                .unwrap();

            let result = eqd_read_with(
                &data_path,
                table_schema.clone(),
                vec![1],
                vec![eqd_delete_file(&del_path, vec![1])],
                Some(predicate),
                row_selection_enabled,
            )
            .await;

            assert_eq!(
                eqd_collect_i64(&result, 0),
                vec![3, 4, 6],
                "row_selection_enabled={row_selection_enabled}"
            );
        }
    }

    #[tokio::test]
    async fn test_pos_and_eq_delete_with_scan_predicate() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef],
        );

        // Positional delete removes row 2 (id 3), equality delete removes id 5, and the
        // predicate keeps id >= 2. Each removes something the others do not.
        let pos_del_path = format!("{loc}/pos-del.parquet");
        {
            let pos_schema = crate::arrow::delete_filter::tests::create_pos_del_schema();
            let batch = RecordBatch::try_new(pos_schema.clone(), vec![
                Arc::new(StringArray::from(vec![data_path.as_str()])) as ArrayRef,
                Arc::new(Int64Array::from(vec![2i64])) as ArrayRef,
            ])
            .unwrap();
            let file = File::create(&pos_del_path).unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let mut writer = ArrowWriter::try_new(file, pos_schema, Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let eq_del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &eq_del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![5])) as ArrayRef],
        );

        let pos_del = FileScanTaskDeleteFile::builder()
            .with_file_path(pos_del_path.clone())
            .with_file_size_in_bytes(std::fs::metadata(&pos_del_path).unwrap().len())
            .with_file_type(DataContentType::PositionDeletes)
            .with_file_format(DataFileFormat::Parquet)
            .with_partition_spec_id(0)
            .build()
            .unwrap();

        for row_selection_enabled in [false, true] {
            let predicate = Reference::new("id")
                .greater_than_or_equal_to(Datum::long(2))
                .bind(table_schema.clone(), false)
                .unwrap();

            let result = eqd_read_with(
                &data_path,
                table_schema.clone(),
                vec![1],
                vec![pos_del.clone(), eqd_delete_file(&eq_del_path, vec![1])],
                Some(predicate),
                row_selection_enabled,
            )
            .await;

            assert_eq!(
                eqd_collect_i64(&result, 0),
                vec![2, 4, 6],
                "row_selection_enabled={row_selection_enabled}"
            );
        }
    }

    // An equality-delete key column absent from the data file (added by later schema
    // evolution) probes as its `initial_default`, as projection reads it, and as null without one.
    #[tokio::test]
    async fn test_eq_delete_on_column_absent_from_data_file() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        // `added` exists in the table schema but not in the data file below.
        let required_with_default = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(2, "added", Type::Primitive(PrimitiveType::Long))
                        .with_initial_default(Literal::long(7))
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let optional_no_default = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "added", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
        );

        // Keyed on `added`, once with the initial default and once with another value.
        let del_path = |value: i64| {
            let path = format!("{loc}/eq-del-{value}.parquet");
            eqd_write(
                &path,
                Arc::new(ArrowSchema::new(vec![eqd_field(
                    "added",
                    DataType::Int64,
                    2,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![value])) as ArrayRef],
            );
            path
        };
        let (del_default, del_other) = (del_path(7), del_path(8));

        for (label, schema, del_path, expected) in [
            (
                "initial_default deleted",
                required_with_default.clone(),
                &del_default,
                vec![],
            ),
            (
                "other value deleted",
                required_with_default,
                &del_other,
                vec![1, 2, 3],
            ),
            ("no default", optional_no_default, &del_default, vec![
                1, 2, 3,
            ]),
        ] {
            let result = eqd_read(&data_path, schema, vec![1], vec![eqd_delete_file(
                del_path,
                vec![2],
            )])
            .await;

            assert_eq!(eqd_collect_i64(&result, 0), expected, "{label}");
        }
    }

    // A migrated Hive file stores no identity partition column `p`; a Flink upsert sink
    // keys its equality deletes on (id, p). The reader fills `p` from the partition value
    // before the probe, as Java does, so the delete applies.
    #[tokio::test]
    async fn test_eq_delete_on_identity_partition_column_absent_from_data_file() {
        use crate::spec::{PartitionSpec, Struct, Transform};

        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "p", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );
        let spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .add_partition_field("p", "p", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
        );
        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![
                eqd_field("id", DataType::Int64, 1, false),
                eqd_field("p", DataType::Int64, 2, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![2, 3])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(5), None])) as ArrayRef,
            ],
        );

        for (partition, expected) in [
            (Some(Struct::from_iter([Some(Literal::long(5))])), vec![
                1, 3,
            ]),
            (Some(Struct::from_iter([None])), vec![1, 2]),
            (None, vec![1, 2]),
        ] {
            let task = FileScanTask::builder()
                .with_file_size_in_bytes(std::fs::metadata(&data_path).unwrap().len())
                .with_start(0)
                .with_length(0)
                .with_data_file_path(data_path.clone())
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(schema.clone())
                .with_project_field_ids(vec![1])
                .with_deletes(vec![eqd_delete_file(&del_path, vec![1, 2])])
                .with_partition(partition.clone())
                .with_partition_spec(partition.as_ref().map(|_| spec.clone()))
                .with_case_sensitive(false)
                .build()
                .unwrap();
            let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
            let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
            let result: Vec<RecordBatch> = reader
                .read(tasks)
                .unwrap()
                .stream()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(
                eqd_collect_i64(&result, 0),
                expected,
                "partition {partition:?}"
            );
        }
    }

    // An absent `uuid` identity partition column probes as the partition value, although the
    // delete keys come back through Arrow as fixed[16].
    #[tokio::test]
    async fn test_eq_delete_on_uuid_partition_column_absent_from_data_file() {
        use arrow_array::FixedSizeBinaryArray;

        use crate::spec::{PartitionSpec, Struct, Transform};

        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();
        let u = uuid::Uuid::parse_str("f79c3e09-677c-4bbd-a479-3f349cb785e7").unwrap();
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "u", Type::Primitive(PrimitiveType::Uuid)).into(),
                ])
                .build()
                .unwrap(),
        );
        let spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .add_partition_field("u", "u", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
        );
        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![
                eqd_field("id", DataType::Int64, 1, false),
                eqd_field("u", DataType::FixedSizeBinary(16), 2, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![2])) as ArrayRef,
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(std::iter::once(u.as_bytes().to_vec()))
                        .unwrap(),
                ) as ArrayRef,
            ],
        );

        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&data_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(data_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema.clone())
            .with_project_field_ids(vec![1])
            .with_deletes(vec![eqd_delete_file(&del_path, vec![1, 2])])
            .with_partition(Some(Struct::from_iter([Some(Literal::uuid(u))])))
            .with_partition_spec(Some(spec))
            .with_case_sensitive(false)
            .build()
            .unwrap();
        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(eqd_collect_i64(&result, 0), vec![1, 3]);
    }

    // A scan task built outside the planner can carry a partition value stored before an
    // int->long promotion; the absent column still probes as the widened value.
    #[tokio::test]
    async fn test_eq_delete_on_absent_partition_column_with_unwidened_value() {
        use crate::spec::{PartitionSpec, Struct, Transform};

        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "p", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );
        let spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .add_partition_field("p", "p", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
        );
        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![
                eqd_field("id", DataType::Int64, 1, false),
                eqd_field("p", DataType::Int64, 2, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![2])) as ArrayRef,
                Arc::new(Int64Array::from(vec![5])) as ArrayRef,
            ],
        );

        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&data_path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(data_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema.clone())
            .with_project_field_ids(vec![1])
            .with_deletes(vec![eqd_delete_file(&del_path, vec![1, 2])])
            .with_partition(Some(Struct::from_iter([Some(Literal::int(5))])))
            .with_partition_spec(Some(spec))
            .with_case_sensitive(false)
            .build()
            .unwrap();
        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result: Vec<RecordBatch> = reader
            .read(tasks)
            .unwrap()
            .stream()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(eqd_collect_i64(&result, 0), vec![1, 3]);
    }

    #[tokio::test]
    async fn test_eq_delete_files_with_different_physical_types_merge() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef],
        );

        let del_i64 = format!("{loc}/eq-del-i64.parquet");
        eqd_write(
            &del_i64,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![2])) as ArrayRef],
        );

        // Written as int32: an older file from before the column was widened to long.
        let del_i32 = format!("{loc}/eq-del-i32.parquet");
        eqd_write(
            &del_i32,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int32,
                1,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![3])) as ArrayRef],
        );

        let result = eqd_read(&data_path, table_schema, vec![1], vec![
            eqd_delete_file(&del_i64, vec![1]),
            eqd_delete_file(&del_i32, vec![1]),
        ])
        .await;

        // Both keys apply, so the two files really did end up in one group.
        assert_eq!(eqd_collect_i64(&result, 0), vec![1, 4]);
    }

    // A decimal equality-delete key whose precision differs from the table's. Precision
    // widening is legal Iceberg schema evolution and goes through a different `Datum::to`
    // arm than the integer promotion already covered.
    #[tokio::test]
    async fn test_eq_delete_decimal_precision_widening() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(
                        2,
                        "amount",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 18,
                            scale: 2,
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        // Data file written at the narrower precision it had when it was created.
        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![
                eqd_field("id", DataType::Int64, 1, false),
                eqd_field("amount", DataType::Decimal128(9, 2), 2, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(
                    Decimal128Array::from(vec![100i128, 250, 375])
                        .with_precision_and_scale(9, 2)
                        .unwrap(),
                ) as ArrayRef,
            ],
        );

        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "amount",
                DataType::Decimal128(18, 2),
                2,
                false,
            )])),
            vec![Arc::new(
                Decimal128Array::from(vec![250i128])
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            ) as ArrayRef],
        );

        let result = eqd_read(&data_path, table_schema, vec![1, 2], vec![eqd_delete_file(
            &del_path,
            vec![2],
        )])
        .await;

        assert_eq!(eqd_collect_i64(&result, 0), vec![1, 3]);
    }

    // Double delete columns are out of spec. Nothing rejects them today, so this
    // pins what actually happens: `PrimitiveLiteral` stores doubles as
    // `OrderedFloat`, whose `Hash`/`Eq` canonicalise NaN and signed zero.
    // A NaN key therefore matches NaN data, and a -0.0 key also removes 0.0.
    #[tokio::test]
    async fn test_eq_delete_double_column_matches_nan_and_keeps_signed_zeros_distinct() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(2, "d", Type::Primitive(PrimitiveType::Double)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![
                eqd_field("id", DataType::Int64, 1, false),
                eqd_field("d", DataType::Float64, 2, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.5, f64::NAN, 0.0, -0.0, 2.5])) as ArrayRef,
            ],
        );

        // Literal equality follows Java's Double.compare (#3327): a NaN key matches the NaN
        // row even though IEEE-754 says NaN != NaN, and a -0.0 key removes only the -0.0 row,
        // not 0.0.
        let del_path = format!("{loc}/eq-del.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "d",
                DataType::Float64,
                2,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![f64::NAN, -0.0])) as ArrayRef],
        );

        let result = eqd_read(&data_path, table_schema, vec![1, 2], vec![eqd_delete_file(
            &del_path,
            vec![2],
        )])
        .await;

        assert_eq!(eqd_collect_i64(&result, 0), vec![1, 3, 5]);
    }

    #[tokio::test]
    async fn test_empty_eq_delete_file_deletes_nothing() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();

        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        let data_path = format!("{loc}/data.parquet");
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
        );

        let del_path = format!("{loc}/eq-del-empty.parquet");
        eqd_write(
            &del_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef],
        );

        let result = eqd_read(&data_path, table_schema, vec![1], vec![eqd_delete_file(
            &del_path,
            vec![1],
        )])
        .await;

        assert_eq!(eqd_collect_i64(&result, 0), vec![1, 2, 3]);
    }

    /// Writes a file that stores only field 1 (`a`), with values 1, 2, 3.
    fn write_file_without_b() -> (String, TempDir) {
        let tmp_dir = TempDir::new().unwrap();
        let file_path = format!("{}/1.parquet", tmp_dir.path().to_str().unwrap());
        let arrow_schema = Arc::new(ArrowSchema::new(vec![field_with_id(
            "a",
            DataType::Int64,
            1,
        )]));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Int64Array::from(
            vec![1, 2, 3],
        ))])
        .unwrap();
        write_row_groups(&file_path, arrow_schema, vec![batch], false);
        (file_path, tmp_dir)
    }

    fn schema_with_b(b: NestedField) -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "a", Type::Primitive(PrimitiveType::Long)).into(),
                    b.into(),
                ])
                .build()
                .unwrap(),
        )
    }

    /// Reads the file from [`write_file_without_b`], in which `b` reads as 7 on every row through
    /// `schema` or `partition`, and asserts the rows each predicate keeps. Each predicate, and each
    /// equality delete on `b`, must keep or drop all three rows as if `b` were stored as 7.
    async fn assert_absent_b_reads_as_7(
        schema: SchemaRef,
        partition: Option<(Arc<PartitionSpec>, Struct)>,
        cases: Vec<(Predicate, usize)>,
    ) {
        let (file_path, tmp_dir) = write_file_without_b();
        let read = async |row_selection_enabled, predicate, deletes| {
            let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
                .with_row_selection_enabled(row_selection_enabled)
                .build();
            let (batches, _) = read_once(
                reader,
                &file_path,
                schema.clone(),
                vec![1, 2],
                predicate,
                partition.clone(),
                deletes,
            )
            .await;
            batches
        };

        let batches = read(false, Predicate::AlwaysTrue, vec![]).await;
        assert_eq!(
            batches[0].column(1).as_primitive::<Int64Type>().values(),
            &[7, 7, 7]
        );

        // The keep predicate for a deleted value `v` is `b IS NULL OR b != v`, and `b IS NOT NULL`
        // for a deleted null.
        let delete_field = Field::new("b", DataType::Int64, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "2".to_string(),
        )]));
        let delete_schema = Arc::new(ArrowSchema::new(vec![delete_field]));
        let deletes: Vec<_> = [(Some(7), 0), (Some(5), 3), (None, 3)]
            .into_iter()
            .enumerate()
            .map(|(i, (deleted_b, rows))| {
                let delete_path =
                    format!("{}/eq-del-{i}.parquet", tmp_dir.path().to_str().unwrap());
                let batch = RecordBatch::try_new(delete_schema.clone(), vec![Arc::new(
                    Int64Array::from(vec![deleted_b]),
                )])
                .unwrap();
                write_row_groups(&delete_path, delete_schema.clone(), vec![batch], false);
                let delete = FileScanTaskDeleteFile::builder()
                    .with_file_size_in_bytes(std::fs::metadata(&delete_path).unwrap().len())
                    .with_file_path(delete_path)
                    .with_file_type(DataContentType::EqualityDeletes)
                    .with_file_format(DataFileFormat::Parquet)
                    .with_partition_spec_id(0)
                    .with_equality_ids(Some(vec![2]))
                    .build()
                    .unwrap();
                (deleted_b, delete, rows)
            })
            .collect();

        for row_selection_enabled in [false, true] {
            for (predicate, expected) in &cases {
                let batches = read(row_selection_enabled, predicate.clone(), vec![]).await;
                let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                assert_eq!(
                    rows, *expected,
                    "{predicate}, row_selection_enabled = {row_selection_enabled}"
                );
            }
            for (deleted_b, delete, expected) in &deletes {
                let batches = read(row_selection_enabled, Predicate::AlwaysTrue, vec![
                    delete.clone(),
                ])
                .await;
                let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                assert_eq!(
                    rows, *expected,
                    "equality delete on b = {deleted_b:?}, row_selection_enabled = {row_selection_enabled}"
                );
            }
        }
    }

    /// A file written before column `b` was added with `initial-default` 7.
    #[tokio::test]
    async fn test_predicate_on_absent_column_uses_initial_default() {
        let schema = schema_with_b(
            NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Long))
                .with_initial_default(Literal::long(7)),
        );
        let b = || Reference::new("b");
        assert_absent_b_reads_as_7(schema, None, vec![
            (b().equal_to(Datum::long(7)), 3),
            (b().is_not_null(), 3),
            (b().is_in([Datum::long(7), Datum::long(8)]), 3),
            (b().greater_than(Datum::long(5)), 3),
            (b().equal_to(Datum::long(8)), 0),
            (b().is_null(), 0),
        ])
        .await;
    }

    /// A file that doesn't store its identity partition column `b`, as after a Hive migration or
    /// `add_files`, with partition value 7.
    #[tokio::test]
    async fn test_predicate_on_absent_identity_partition_column_uses_partition_value() {
        let schema = schema_with_b(NestedField::optional(
            2,
            "b",
            Type::Primitive(PrimitiveType::Long),
        ));
        let partition_spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("b", "b", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let partition = Struct::from_iter([Some(Literal::long(7))]);
        let b = || Reference::new("b");
        assert_absent_b_reads_as_7(schema, Some((Arc::new(partition_spec), partition)), vec![
            (b().equal_to(Datum::long(7)), 3),
            (b().is_not_null(), 3),
            (b().equal_to(Datum::long(8)), 0),
            (b().is_null(), 0),
        ])
        .await;
    }

    /// Reads a file that stores only `id` (1, 2, 3) with each filter, with row selection off and
    /// on, and asserts the ids it keeps.
    async fn assert_filters_on_file_with_only_id(
        schema: SchemaRef,
        partition: Option<(Arc<PartitionSpec>, Struct)>,
        cases: Vec<(Predicate, Vec<i64>)>,
    ) {
        let tmp = TempDir::new().unwrap();
        let data_path = format!("{}/data.parquet", tmp.path().to_str().unwrap());
        eqd_write(
            &data_path,
            Arc::new(ArrowSchema::new(vec![eqd_field(
                "id",
                DataType::Int64,
                1,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
        );
        for row_selection_enabled in [false, true] {
            for (predicate, expected) in &cases {
                let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
                    .with_row_selection_enabled(row_selection_enabled)
                    .build();
                let (batches, _) = read_once(
                    reader,
                    &data_path,
                    schema.clone(),
                    vec![1],
                    predicate.clone(),
                    partition.clone(),
                    vec![],
                )
                .await;
                assert_eq!(
                    eqd_collect_i64(&batches, 0),
                    *expected,
                    "{predicate}, row_selection_enabled = {row_selection_enabled}"
                );
            }
        }
    }

    /// A migrated file that does not store its identity partition column `p` (partition value 5)
    /// keeps the rows a filter on `p` selects when `p` is read as 5.
    #[tokio::test]
    async fn test_filter_on_absent_identity_partition_column_keeps_matching_rows() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "p", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );
        let spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("p", "p", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let partition = Struct::from_iter([Some(Literal::long(5))]);
        let id = || Reference::new("id");
        let p = || Reference::new("p");
        assert_filters_on_file_with_only_id(schema, Some((Arc::new(spec), partition)), vec![
            (p().equal_to(Datum::long(5)), vec![1, 2, 3]),
            (p().is_in([Datum::long(5), Datum::long(6)]), vec![1, 2, 3]),
            (p().greater_than_or_equal_to(Datum::long(3)), vec![1, 2, 3]),
            (p().is_not_null(), vec![1, 2, 3]),
            (p().equal_to(Datum::long(6)).negate(), vec![1, 2, 3]),
            (
                id().equal_to(Datum::long(2))
                    .and(p().equal_to(Datum::long(5))),
                vec![2],
            ),
            (
                id().equal_to(Datum::long(2))
                    .or(p().equal_to(Datum::long(5))),
                vec![1, 2, 3],
            ),
            (p().less_than(Datum::long(6)), vec![1, 2, 3]),
            (p().less_than_or_equal_to(Datum::long(5)), vec![1, 2, 3]),
            (p().less_than(Datum::long(5)), vec![]),
            (p().less_than_or_equal_to(Datum::long(4)), vec![]),
        ])
        .await;
    }

    /// A column the file does not store, with no partition value or `initial-default`, reads as
    /// null, so every filter on it keeps the same rows as on a file that stores it as all null.
    #[tokio::test]
    async fn test_filter_on_absent_column_without_value_matches_all_null_column() {
        use arrow_array::{Float64Array, StringArray};

        let tmp = TempDir::new().unwrap();
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "c", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(3, "s", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(4, "d", Type::Primitive(PrimitiveType::Double)).into(),
                ])
                .build()
                .unwrap(),
        );
        let id_field = eqd_field("id", DataType::Int64, 1, false);
        let ids = || Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let absent = format!("{}/absent.parquet", tmp.path().to_str().unwrap());
        eqd_write(
            &absent,
            Arc::new(ArrowSchema::new(vec![id_field.clone()])),
            vec![ids()],
        );
        let all_null = format!("{}/all-null.parquet", tmp.path().to_str().unwrap());
        eqd_write(
            &all_null,
            Arc::new(ArrowSchema::new(vec![
                id_field,
                eqd_field("c", DataType::Int64, 2, true),
                eqd_field("s", DataType::Utf8, 3, true),
                eqd_field("d", DataType::Float64, 4, true),
            ])),
            vec![
                ids(),
                Arc::new(Int64Array::from(vec![None::<i64>; 3])) as ArrayRef,
                Arc::new(StringArray::from(vec![None::<&str>; 3])) as ArrayRef,
                Arc::new(Float64Array::from(vec![None::<f64>; 3])) as ArrayRef,
            ],
        );

        let c = || Reference::new("c");
        let cases = vec![
            (c().less_than(Datum::long(5)), vec![]),
            (c().less_than_or_equal_to(Datum::long(5)), vec![]),
            (c().greater_than(Datum::long(5)), vec![]),
            (c().greater_than_or_equal_to(Datum::long(5)), vec![]),
            (c().equal_to(Datum::long(5)), vec![]),
            (c().not_equal_to(Datum::long(5)), vec![]),
            (c().is_in([Datum::long(5), Datum::long(6)]), vec![]),
            (c().is_not_in([Datum::long(5), Datum::long(6)]), vec![]),
            (c().less_than(Datum::long(5)).negate(), vec![]),
            (c().is_null(), vec![1, 2, 3]),
            (c().is_not_null(), vec![]),
            (Reference::new("s").starts_with(Datum::string("a")), vec![]),
            (
                Reference::new("s").not_starts_with(Datum::string("a")),
                vec![],
            ),
            (Reference::new("d").is_nan(), vec![]),
            (Reference::new("d").is_not_nan(), vec![1, 2, 3]),
        ];
        for row_selection_enabled in [false, true] {
            for path in [&absent, &all_null] {
                for (predicate, expected) in &cases {
                    let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
                        .with_row_selection_enabled(row_selection_enabled)
                        .build();
                    let (batches, _) = read_once(
                        reader,
                        path,
                        schema.clone(),
                        vec![1],
                        predicate.clone(),
                        None,
                        vec![],
                    )
                    .await;
                    assert_eq!(
                        eqd_collect_i64(&batches, 0),
                        *expected,
                        "{predicate} on {path}, row_selection_enabled = {row_selection_enabled}"
                    );
                }
            }
        }
    }

    /// pandas and pyarrow write nanosecond timestamps, which `add_files` accepts into
    /// microsecond columns: `ts` (`timestamp`) and `tz` (`timestamptz`) store nanoseconds.
    fn nanosecond_file(path: &str, micros: &[i64], with_bloom_filters: bool) -> SchemaRef {
        use arrow_array::TimestampNanosecondArray;
        use arrow_schema::TimeUnit;

        let nanos: Vec<i64> = micros.iter().map(|us| us * 1000).collect();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            eqd_field("id", DataType::Int64, 1, false),
            eqd_field(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                2,
                true,
            ),
            eqd_field(
                "tz",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                3,
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int64Array::from_iter_values(1..=micros.len() as i64)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(nanos.clone())),
            Arc::new(TimestampNanosecondArray::from(nanos).with_timezone("+00:00")),
        ])
        .unwrap();
        write_row_groups(path, arrow_schema, vec![batch], with_bloom_filters);
        Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "ts", Type::Primitive(PrimitiveType::Timestamp))
                        .into(),
                    NestedField::optional(3, "tz", Type::Primitive(PrimitiveType::Timestamptz))
                        .into(),
                ])
                .build()
                .unwrap(),
        )
    }

    // 2024-01-01T00:00:00.123456, 2024-06-01, 2024-12-31
    const NANOSECOND_FILE_MICROS: [i64; 3] = [
        1_704_067_200_123_456,
        1_717_200_000_000_000,
        1_735_603_200_000_000,
    ];

    /// Equality deletes keyed on a column the file stores in nanoseconds match by value, also
    /// where the UTC flag of the file differs from the table column.
    #[tokio::test]
    async fn test_eq_delete_on_nanosecond_column_in_microsecond_field() {
        use arrow_array::TimestampMicrosecondArray;
        use arrow_schema::TimeUnit;

        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();
        let data_path = format!("{loc}/data.parquet");
        nanosecond_file(&data_path, &NANOSECOND_FILE_MICROS, false);
        let delete_file = |name: &str, id: i32, ty: &PrimitiveType, micros: i64| {
            let path = format!("{loc}/{name}.parquet");
            let tz = (*ty == PrimitiveType::Timestamptz).then(|| "+00:00".into());
            eqd_write(
                &path,
                Arc::new(ArrowSchema::new(vec![eqd_field(
                    name,
                    DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                    id,
                    true,
                )])),
                vec![Arc::new(
                    TimestampMicrosecondArray::from(vec![micros]).with_timezone_opt(tz),
                )],
            );
            eqd_delete_file(&path, vec![id])
        };

        // `ts` stores nanoseconds without UTC, `tz` with UTC.
        for (ts_type, tz_type, projection) in [
            (PrimitiveType::Timestamp, PrimitiveType::Timestamptz, vec![
                1, 2, 3,
            ]),
            (PrimitiveType::Timestamptz, PrimitiveType::Timestamp, vec![
                1,
            ]),
        ] {
            let schema = Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                        NestedField::optional(2, "ts", Type::Primitive(ts_type.clone())).into(),
                        NestedField::optional(3, "tz", Type::Primitive(tz_type.clone())).into(),
                    ])
                    .build()
                    .unwrap(),
            );
            let deletes = vec![
                delete_file("ts", 2, &ts_type, NANOSECOND_FILE_MICROS[0]),
                delete_file("tz", 3, &tz_type, NANOSECOND_FILE_MICROS[1]),
            ];
            let result = eqd_read(&data_path, schema, projection, deletes).await;
            assert_eq!(
                eqd_collect_i64(&result, 0),
                vec![3],
                "ts as {ts_type}, tz as {tz_type}"
            );
        }
    }

    /// pandas writes `datetime64[ns]` without the UTC flag, also for a `timestamptz` column, and
    /// `datetime64[ns, UTC]` with it, also for a `timestamp` column. Such columns read, filter
    /// and match equality deletes by value, projected or not.
    #[tokio::test]
    async fn test_nanosecond_column_with_other_utc_flag_in_microsecond_field() {
        let tmp = TempDir::new().unwrap();
        let loc = tmp.path().to_str().unwrap();
        let data_path = format!("{loc}/data.parquet");
        nanosecond_file(&data_path, &NANOSECOND_FILE_MICROS, false);
        // `ts` stores nanoseconds without UTC, `tz` with UTC.
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "ts", Type::Primitive(PrimitiveType::Timestamptz))
                        .into(),
                    NestedField::optional(3, "tz", Type::Primitive(PrimitiveType::Timestamp))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let batches = eqd_read(&data_path, schema.clone(), vec![1, 2, 3], vec![]).await;
        assert_eq!(batches.len(), 1);
        for (column, tz) in [(1, Some("+00:00")), (2, None)] {
            let array = batches[0]
                .column(column)
                .as_primitive::<TimestampMicrosecondType>();
            assert_eq!(array.values(), &NANOSECOND_FILE_MICROS, "column {column}");
            assert_eq!(array.timezone(), tz, "column {column}");
        }

        let delete_file = |name: &str, id: i32, tz: Option<Arc<str>>, micros: i64| {
            let path = format!("{loc}/{name}.parquet");
            eqd_write(
                &path,
                Arc::new(ArrowSchema::new(vec![eqd_field(
                    name,
                    DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                    id,
                    true,
                )])),
                vec![Arc::new(
                    TimestampMicrosecondArray::from(vec![micros]).with_timezone_opt(tz),
                )],
            );
            eqd_delete_file(&path, vec![id])
        };
        let deletes = [
            (
                "ts",
                delete_file("ts", 2, Some("+00:00".into()), NANOSECOND_FILE_MICROS[0]),
                vec![2, 3],
            ),
            (
                "tz",
                delete_file("tz", 3, None, NANOSECOND_FILE_MICROS[1]),
                vec![1, 3],
            ),
        ];
        let filters = [
            (
                Reference::new("ts").equal_to(Datum::timestamptz_micros(NANOSECOND_FILE_MICROS[0])),
                vec![1],
            ),
            (
                Reference::new("ts")
                    .less_than(Datum::timestamptz_micros(NANOSECOND_FILE_MICROS[1])),
                vec![1],
            ),
            (
                Reference::new("tz").equal_to(Datum::timestamp_micros(NANOSECOND_FILE_MICROS[1])),
                vec![2],
            ),
            (
                Reference::new("tz")
                    .greater_than(Datum::timestamp_micros(NANOSECOND_FILE_MICROS[0])),
                vec![2, 3],
            ),
        ];
        for projection in [vec![1, 2, 3], vec![1]] {
            for (column, delete, expected) in &deletes {
                let result = eqd_read(&data_path, schema.clone(), projection.clone(), vec![
                    delete.clone(),
                ])
                .await;
                assert_eq!(
                    eqd_collect_i64(&result, 0),
                    *expected,
                    "delete on {column}, projection {projection:?}"
                );
            }
            for row_selection_enabled in [false, true] {
                for (predicate, expected) in &filters {
                    let result = eqd_read_with(
                        &data_path,
                        schema.clone(),
                        projection.clone(),
                        vec![],
                        Some(predicate.clone().bind(schema.clone(), true).unwrap()),
                        row_selection_enabled,
                    )
                    .await;
                    assert_eq!(
                        eqd_collect_i64(&result, 0),
                        *expected,
                        "{predicate}, projection {projection:?}, row selection \
                         {row_selection_enabled}"
                    );
                }
            }
        }
    }

    /// Statistics, the page index and bloom filters of a column that stores nanoseconds hold
    /// nanoseconds, so none of them may prune by the microsecond values of the table column.
    #[tokio::test]
    async fn test_pruning_on_nanosecond_column_in_microsecond_field() {
        let tmp = TempDir::new().unwrap();
        let path = format!("{}/data.parquet", tmp.path().to_str().unwrap());
        let schema = nanosecond_file(&path, &NANOSECOND_FILE_MICROS[..2], true);

        let cases = [
            (
                Reference::new("ts")
                    .less_than(Datum::timestamp_from_str("2025-01-01T00:00:00").unwrap()),
                2,
            ),
            (
                Reference::new("ts")
                    .equal_to(Datum::timestamp_from_str("2024-01-01T00:00:00.123456").unwrap()),
                1,
            ),
            (
                Reference::new("tz")
                    .less_than(Datum::timestamptz_from_str("2025-01-01T00:00:00+00:00").unwrap()),
                2,
            ),
            (
                Reference::new("tz").equal_to(
                    Datum::timestamptz_from_str("2024-01-01T00:00:00.123456+00:00").unwrap(),
                ),
                1,
            ),
        ];
        // Each pruning step on its own: row-group statistics, the page index, bloom filters.
        for (row_groups, pages, blooms) in [
            (true, false, false),
            (false, true, false),
            (false, false, true),
        ] {
            for (predicate, expected) in &cases {
                let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
                    .with_row_group_filtering_enabled(row_groups)
                    .with_row_selection_enabled(pages)
                    .with_bloom_filter_enabled(blooms)
                    .build();
                let (batches, _) = read_once(
                    reader,
                    &path,
                    schema.clone(),
                    vec![1],
                    predicate.clone(),
                    None,
                    vec![],
                )
                .await;
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(
                    rows, *expected,
                    "{predicate} with row groups {row_groups}, pages {pages}, blooms {blooms}"
                );
            }
        }
    }
}
