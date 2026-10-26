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

//! INT96 timestamp coercion for Parquet files.

use std::sync::Arc;

use arrow_schema::{
    DataType, Field, FieldRef, Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use parquet::basic::Type as PhysicalType;
use parquet::schema::types::SchemaDescriptor;

use crate::arrow::schema::{
    ArrowSchemaVisitor, DEFAULT_MAP_FIELD_NAME, UTC_TIME_ZONE, visit_schema,
};
use crate::error::Result;
use crate::{Error, ErrorKind};

/// Coerce Arrow schema types for INT96 columns to microseconds.
///
/// arrow-rs defaults INT96 to `Timestamp(Nanosecond)`, which overflows i64 for dates outside
/// ~1677-2262. We use arrow-rs's schema hint mechanism to read every INT96 leaf as microseconds,
/// in UTC if the file's Arrow schema names a time zone, as Iceberg Java does. In a `timestamp_ns`
/// or `timestamptz_ns` column the type check then fails the read: nanoseconds would wrap outside
/// 1677-2262, and microseconds drop the nanoseconds INT96 can hold.
///
/// Iceberg Java handles this differently: it bypasses parquet-mr with a custom column reader
/// (`GenericParquetReaders.TimestampInt96Reader`). We achieve the same result via schema hints.
///
/// Only INT96 leaves get a hint. parquet-rs also accepts it for an INT64 `TIMESTAMP(NANOS)`
/// leaf, but then reinterprets the nanosecond values as microseconds.
///
/// References:
/// - Iceberg spec primitive types: <https://iceberg.apache.org/spec/#primitive-types>
/// - arrow-rs schema hint support: <https://github.com/apache/arrow-rs/pull/7285>
pub(crate) fn coerce_int96_timestamps(
    arrow_schema: &ArrowSchemaRef,
    parquet_schema: &SchemaDescriptor,
) -> Result<Option<Arc<ArrowSchema>>> {
    // The visitor cannot walk every Arrow type, so it runs only for files with INT96 leaves.
    if !parquet_schema
        .columns()
        .iter()
        .any(|column| column.physical_type() == PhysicalType::INT96)
    {
        return Ok(None);
    }
    let mut visitor = Int96CoercionVisitor::new(parquet_schema);
    let coerced = visit_schema(arrow_schema, &mut visitor)?;
    Ok(visitor.changed.then(|| Arc::new(coerced)))
}

/// Visitor that coerces the Arrow fields of INT96 leaves to microseconds.
struct Int96CoercionVisitor<'a> {
    parquet_schema: &'a SchemaDescriptor,
    field_stack: Vec<FieldRef>,
    /// Index of the next Parquet leaf column; Arrow leaves are visited in Parquet leaf order.
    next_leaf: usize,
    changed: bool,
}

impl<'a> Int96CoercionVisitor<'a> {
    fn new(parquet_schema: &'a SchemaDescriptor) -> Self {
        Self {
            parquet_schema,
            field_stack: Vec::new(),
            next_leaf: 0,
            changed: false,
        }
    }

    /// The Arrow type to read an INT96 leaf as: microseconds, and UTC if the file's Arrow type
    /// names any time zone. INT96 holds UTC instants, and the zone of a stored Arrow schema only
    /// tells how to display them; Java ignores it too.
    fn target_type(&self, field: &FieldRef, leaf: usize) -> Option<DataType> {
        let is_int96 = self
            .parquet_schema
            .columns()
            .get(leaf)
            .is_some_and(|column| column.physical_type() == PhysicalType::INT96);
        let DataType::Timestamp(_, tz) = field.data_type() else {
            return None;
        };
        let target = DataType::Timestamp(
            TimeUnit::Microsecond,
            tz.as_ref().map(|_| UTC_TIME_ZONE.into()),
        );
        (is_int96 && &target != field.data_type()).then_some(target)
    }
}

impl ArrowSchemaVisitor for Int96CoercionVisitor<'_> {
    type T = Field;
    type U = ArrowSchema;

    fn before_field(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.clone());
        Ok(())
    }

    fn after_field(&mut self, _field: &FieldRef) -> Result<()> {
        self.field_stack.pop();
        Ok(())
    }

    fn before_list_element(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.clone());
        Ok(())
    }

    fn after_list_element(&mut self, _field: &FieldRef) -> Result<()> {
        self.field_stack.pop();
        Ok(())
    }

    fn before_map_key(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.clone());
        Ok(())
    }

    fn after_map_key(&mut self, _field: &FieldRef) -> Result<()> {
        self.field_stack.pop();
        Ok(())
    }

    fn before_map_value(&mut self, field: &FieldRef) -> Result<()> {
        self.field_stack.push(field.clone());
        Ok(())
    }

    fn after_map_value(&mut self, _field: &FieldRef) -> Result<()> {
        self.field_stack.pop();
        Ok(())
    }

    fn schema(&mut self, schema: &ArrowSchema, values: Vec<Field>) -> Result<ArrowSchema> {
        Ok(ArrowSchema::new_with_metadata(
            values,
            schema.metadata().clone(),
        ))
    }

    fn r#struct(&mut self, _fields: &Fields, results: Vec<Field>) -> Result<Field> {
        let field_info = self
            .field_stack
            .last()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Field stack underflow in struct"))?;
        Ok(Field::new(
            field_info.name(),
            DataType::Struct(Fields::from(results)),
            field_info.is_nullable(),
        )
        .with_metadata(field_info.metadata().clone()))
    }

    fn list(&mut self, list: &DataType, value: Field) -> Result<Field> {
        let field_info = self
            .field_stack
            .last()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Field stack underflow in list"))?;
        let list_type = match list {
            DataType::List(_) => DataType::List(Arc::new(value)),
            DataType::LargeList(_) => DataType::LargeList(Arc::new(value)),
            DataType::FixedSizeList(_, size) => DataType::FixedSizeList(Arc::new(value), *size),
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Expected list type, got {list}"),
                ));
            }
        };
        Ok(
            Field::new(field_info.name(), list_type, field_info.is_nullable())
                .with_metadata(field_info.metadata().clone()),
        )
    }

    fn map(&mut self, map: &DataType, key_value: Field, value: Field) -> Result<Field> {
        let field_info = self
            .field_stack
            .last()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Field stack underflow in map"))?;
        let sorted = match map {
            DataType::Map(_, sorted) => *sorted,
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Expected map type, got {map}"),
                ));
            }
        };
        let struct_field = Field::new(
            DEFAULT_MAP_FIELD_NAME,
            DataType::Struct(Fields::from(vec![key_value, value])),
            false,
        );
        Ok(Field::new(
            field_info.name(),
            DataType::Map(Arc::new(struct_field), sorted),
            field_info.is_nullable(),
        )
        .with_metadata(field_info.metadata().clone()))
    }

    fn primitive(&mut self, p: &DataType) -> Result<Field> {
        let field_info = self.field_stack.last().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "Field stack underflow in primitive")
        })?;
        let leaf = self.next_leaf;
        self.next_leaf += 1;

        if let Some(target_type) = self.target_type(field_info, leaf) {
            self.changed = true;
            Ok(
                Field::new(field_info.name(), target_type, field_info.is_nullable())
                    .with_metadata(field_info.metadata().clone()),
            )
        } else {
            Ok(
                Field::new(field_info.name(), p.clone(), field_info.is_nullable())
                    .with_metadata(field_info.metadata().clone()),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef, TimeUnit};
    use parquet::arrow::{ArrowSchemaConverter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::{LogicalType, Type as PhysicalType};
    use parquet::schema::types::{SchemaDescriptor, Type as ParquetType};

    use super::coerce_int96_timestamps;

    /// The Parquet schema of `arrow_schema`, with every timestamp leaf stored as INT96.
    fn int96_parquet_schema(arrow_schema: &ArrowSchema) -> SchemaDescriptor {
        fn rewrite(t: &ParquetType) -> Arc<ParquetType> {
            let info = t.get_basic_info();
            let id = info.has_id().then(|| info.id());
            match t {
                ParquetType::PrimitiveType { .. }
                    if matches!(info.logical_type_ref(), Some(LogicalType::Timestamp(_))) =>
                {
                    Arc::new(
                        ParquetType::primitive_type_builder(info.name(), PhysicalType::INT96)
                            .with_repetition(info.repetition())
                            .with_id(id)
                            .build()
                            .unwrap(),
                    )
                }
                ParquetType::GroupType { fields, .. } => {
                    let mut builder = ParquetType::group_type_builder(info.name())
                        .with_fields(fields.iter().map(|f| rewrite(f)).collect())
                        .with_logical_type(info.logical_type_ref().cloned())
                        .with_converted_type(info.converted_type())
                        .with_id(id);
                    if info.has_repetition() {
                        builder = builder.with_repetition(info.repetition());
                    }
                    Arc::new(builder.build().unwrap())
                }
                _ => Arc::new(t.clone()),
            }
        }
        let descriptor = ArrowSchemaConverter::new().convert(arrow_schema).unwrap();
        SchemaDescriptor::new(rewrite(descriptor.root_schema()))
    }

    /// Coerces `arrow_schema` as read from a file that stores its nanosecond timestamps as INT96.
    fn coerce(arrow_schema: &SchemaRef) -> Option<Arc<ArrowSchema>> {
        coerce_int96_timestamps(arrow_schema, &int96_parquet_schema(arrow_schema)).unwrap()
    }

    fn field_id_meta(id: i32) -> HashMap<String, String> {
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())])
    }

    #[test]
    fn test_coerce_timestamp_ns_to_us() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                .with_metadata(field_id_meta(1)),
            Field::new("id", DataType::Int32, false).with_metadata(field_id_meta(2)),
        ]));

        let coerced = coerce(&arrow_schema).unwrap();
        assert_eq!(
            coerced.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        // Non-timestamp field unchanged
        assert_eq!(coerced.field(1).data_type(), &DataType::Int32);
    }

    /// INT96 holds UTC instants: every unit reads as microseconds, and any time zone the
    /// file's Arrow schema names reads as UTC.
    #[test]
    fn test_coerce_unit_and_time_zone() {
        let utc = || Some("+00:00".into());
        for (file_type, expected) in [
            // Spark writes INT96 without an Arrow schema.
            (
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            // pyarrow stores the Arrow type it wrote from.
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                DataType::Timestamp(TimeUnit::Microsecond, utc()),
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into())),
                DataType::Timestamp(TimeUnit::Microsecond, utc()),
            ),
            (
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
        ] {
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("ts", file_type.clone(), true).with_metadata(field_id_meta(1)),
            ]));

            let coerced = coerce(&arrow_schema).unwrap();
            assert_eq!(coerced.field(0).data_type(), &expected, "{file_type}");
        }
    }

    #[test]
    fn test_no_coercion_of_int64_nanosecond_leaf() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                .with_metadata(field_id_meta(1)),
            Field::new("id", DataType::Int32, false).with_metadata(field_id_meta(2)),
        ]));
        let parquet_schema = ArrowSchemaConverter::new().convert(&arrow_schema).unwrap();

        assert!(
            coerce_int96_timestamps(&arrow_schema, &parquet_schema)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_no_coercion_when_already_microsecond() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true)
                .with_metadata(field_id_meta(1)),
            Field::new("id", DataType::Int32, false).with_metadata(field_id_meta(2)),
        ]));

        assert!(coerce(&arrow_schema).is_none());
    }

    #[test]
    fn test_defaults_to_us_without_field_ids() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));

        let coerced = coerce(&arrow_schema).unwrap();
        assert_eq!(
            coerced.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_coerce_preserves_field_metadata() {
        let mut meta = field_id_meta(1);
        meta.insert("custom_key".to_string(), "custom_value".to_string());

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                .with_metadata(meta.clone()),
        ]));

        let coerced = coerce(&arrow_schema).unwrap();
        assert_eq!(coerced.field(0).metadata(), &meta);
    }

    #[test]
    fn test_coerce_timestamp_in_struct() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "data",
                DataType::Struct(
                    vec![
                        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                            .with_metadata(field_id_meta(2)),
                    ]
                    .into(),
                ),
                false,
            )
            .with_metadata(field_id_meta(1)),
        ]));

        let coerced = coerce(&arrow_schema).unwrap();
        let inner = match coerced.field(0).data_type() {
            DataType::Struct(fields) => fields,
            other => panic!("Expected Struct, got {other}"),
        };
        assert_eq!(
            inner[0].data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_coerce_timestamp_in_list() {
        let element_field = Field::new(
            "element",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )
        .with_metadata(field_id_meta(2));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("timestamps", DataType::List(Arc::new(element_field)), true)
                .with_metadata(field_id_meta(1)),
        ]));

        let coerced = coerce(&arrow_schema).unwrap();
        let element_dt = match coerced.field(0).data_type() {
            DataType::List(f) => f.data_type(),
            other => panic!("Expected List, got {other}"),
        };
        assert_eq!(
            element_dt,
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_coerce_timestamp_in_map_value() {
        let key_field = Field::new("key", DataType::Utf8, false).with_metadata(field_id_meta(2));
        let value_field = Field::new(
            "value",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )
        .with_metadata(field_id_meta(3));
        let entries_field = Field::new(
            "key_value",
            DataType::Struct(vec![key_field, value_field].into()),
            false,
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "ts_map",
                DataType::Map(Arc::new(entries_field), false),
                true,
            )
            .with_metadata(field_id_meta(1)),
        ]));

        let coerced = coerce(&arrow_schema).unwrap();
        let value_dt = match coerced.field(0).data_type() {
            DataType::Map(entries, _) => match entries.data_type() {
                DataType::Struct(fields) => fields[1].data_type().clone(),
                other => panic!("Expected Struct inside Map, got {other}"),
            },
            other => panic!("Expected Map, got {other}"),
        };
        assert_eq!(value_dt, DataType::Timestamp(TimeUnit::Microsecond, None));
    }
}
