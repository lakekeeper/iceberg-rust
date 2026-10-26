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
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::basic::Type as PhysicalType;
use parquet::schema::types::SchemaDescriptor;

use crate::arrow::schema::{
    ArrowSchemaVisitor, DEFAULT_MAP_FIELD_NAME, UTC_TIME_ZONE, visit_schema,
};
use crate::error::Result;
use crate::spec::{PrimitiveType, Schema, Type};
use crate::{Error, ErrorKind};

/// Coerce Arrow schema types for INT96 columns to match the Iceberg table schema.
///
/// arrow-rs defaults INT96 to `Timestamp(Nanosecond)`, which overflows i64 for dates outside
/// ~1677-2262. We use arrow-rs's schema hint mechanism to read INT96 as microseconds, in UTC
/// for `timestamptz` columns. Reads into `timestamp_ns` and `timestamptz_ns` columns fail.
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
    iceberg_schema: &Schema,
) -> Result<Option<Arc<ArrowSchema>>> {
    // The visitor cannot walk every Arrow type, so it runs only for files with INT96 leaves.
    if !parquet_schema
        .columns()
        .iter()
        .any(|column| column.physical_type() == PhysicalType::INT96)
    {
        return Ok(None);
    }
    let mut visitor = Int96CoercionVisitor::new(parquet_schema, iceberg_schema);
    let coerced = visit_schema(arrow_schema, &mut visitor)?;
    Ok(visitor.changed.then(|| Arc::new(coerced)))
}

/// Visitor that coerces the Arrow fields of INT96 leaves to the resolution and time zone
/// indicated by the Iceberg schema.
struct Int96CoercionVisitor<'a> {
    parquet_schema: &'a SchemaDescriptor,
    iceberg_schema: &'a Schema,
    field_stack: Vec<FieldRef>,
    /// Index of the next Parquet leaf column; Arrow leaves are visited in Parquet leaf order.
    next_leaf: usize,
    changed: bool,
}

impl<'a> Int96CoercionVisitor<'a> {
    fn new(parquet_schema: &'a SchemaDescriptor, iceberg_schema: &'a Schema) -> Self {
        Self {
            parquet_schema,
            iceberg_schema,
            field_stack: Vec::new(),
            next_leaf: 0,
            changed: false,
        }
    }

    /// The Arrow type to read an INT96 leaf as: the resolution and time zone of its Iceberg
    /// column. INT96 holds UTC instants, so `timestamptz` columns read them as UTC, as Java does.
    /// Without a timestamp column (no field ID, or another type), falls back to microseconds
    /// like Iceberg Java.
    fn target_type(&self, field: &FieldRef, leaf: usize) -> Option<DataType> {
        let is_int96 = self
            .parquet_schema
            .columns()
            .get(leaf)
            .is_some_and(|column| column.physical_type() == PhysicalType::INT96);
        if !is_int96 {
            return None;
        }
        let DataType::Timestamp(_, file_tz) = field.data_type() else {
            return None;
        };

        let (unit, tz) = match field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|id_str| id_str.parse::<i32>().ok())
            .and_then(|field_id| self.iceberg_schema.field_by_id(field_id))
            .map(|f| &*f.field_type)
        {
            // Nanosecond columns read INT96 as microseconds too, and the type check then refuses
            // them: parquet-rs wraps INT96 values outside 1677-2262 at nanosecond resolution,
            // and microseconds drop the nanoseconds INT96 can hold.
            Some(Type::Primitive(PrimitiveType::Timestamp | PrimitiveType::TimestampNs)) => {
                (TimeUnit::Microsecond, None)
            }
            Some(Type::Primitive(PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs)) => {
                (TimeUnit::Microsecond, Some(UTC_TIME_ZONE.into()))
            }
            // Iceberg Java reads INT96 as microseconds by default
            _ => (TimeUnit::Microsecond, file_tz.clone()),
        };
        let target = DataType::Timestamp(unit, tz);
        (&target != field.data_type()).then_some(target)
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
    use crate::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type};

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
    fn coerce(arrow_schema: &SchemaRef, iceberg: &Schema) -> Option<Arc<ArrowSchema>> {
        coerce_int96_timestamps(arrow_schema, &int96_parquet_schema(arrow_schema), iceberg).unwrap()
    }

    fn iceberg_schema_with_timestamp() -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::Timestamp)).into(),
                NestedField::required(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap()
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
        let iceberg = iceberg_schema_with_timestamp();

        let coerced = coerce(&arrow_schema, &iceberg).unwrap();
        assert_eq!(
            coerced.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        // Non-timestamp field unchanged
        assert_eq!(coerced.field(1).data_type(), &DataType::Int32);
    }

    /// INT96 holds UTC instants: the time zone follows the Iceberg column, whatever time zone
    /// the file's Arrow schema names.
    #[test]
    fn test_coerce_time_zone_to_iceberg_type() {
        let utc = || Some("+00:00".into());
        for (iceberg_type, file_type, expected) in [
            // Spark writes INT96 without an Arrow schema.
            (
                PrimitiveType::Timestamptz,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, utc()),
            ),
            // pyarrow stores the Arrow type it wrote from.
            (
                PrimitiveType::Timestamptz,
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                DataType::Timestamp(TimeUnit::Microsecond, utc()),
            ),
            (
                PrimitiveType::Timestamp,
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            // Microseconds, so that the type check refuses nanosecond columns.
            (
                PrimitiveType::TimestampNs,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                PrimitiveType::TimestamptzNs,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, utc()),
            ),
        ] {
            let iceberg = Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "ts", Type::Primitive(iceberg_type.clone())).into(),
                ])
                .build()
                .unwrap();
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("ts", file_type.clone(), true).with_metadata(field_id_meta(1)),
            ]));

            let coerced = coerce(&arrow_schema, &iceberg).unwrap();
            assert_eq!(
                coerced.field(0).data_type(),
                &expected,
                "{file_type} as {iceberg_type}"
            );
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
        let iceberg = iceberg_schema_with_timestamp();

        assert!(
            coerce_int96_timestamps(&arrow_schema, &parquet_schema, &iceberg)
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
        let iceberg = iceberg_schema_with_timestamp();

        assert!(coerce(&arrow_schema, &iceberg).is_none());
    }

    // Without field IDs, the visitor can't look up the Iceberg type and falls back
    // to microsecond to match Iceberg Java behavior.
    #[test]
    fn test_defaults_to_us_without_field_ids() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let iceberg = iceberg_schema_with_timestamp();

        let coerced = coerce(&arrow_schema, &iceberg).unwrap();
        assert_eq!(
            coerced.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    // Field ID exists but points to a non-timestamp Iceberg type. The field_by_id
    // lookup succeeds but the match arm returns None, so unwrap_or falls back to
    // microsecond.
    #[test]
    fn test_defaults_to_us_when_iceberg_type_is_not_timestamp() {
        let iceberg = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                .with_metadata(field_id_meta(1)),
        ]));

        let coerced = coerce(&arrow_schema, &iceberg).unwrap();
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
        let iceberg = iceberg_schema_with_timestamp();

        let coerced = coerce(&arrow_schema, &iceberg).unwrap();
        assert_eq!(coerced.field(0).metadata(), &meta);
    }

    #[test]
    fn test_coerce_timestamp_in_struct() {
        let iceberg = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(
                    1,
                    "data",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(2, "ts", Type::Primitive(PrimitiveType::Timestamp))
                            .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap();

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

        let coerced = coerce(&arrow_schema, &iceberg).unwrap();
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
        let iceberg = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(
                    1,
                    "timestamps",
                    Type::List(ListType {
                        element_field: NestedField::optional(
                            2,
                            "element",
                            Type::Primitive(PrimitiveType::Timestamp),
                        )
                        .into(),
                    }),
                )
                .into(),
            ])
            .build()
            .unwrap();

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

        let coerced = coerce(&arrow_schema, &iceberg).unwrap();
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
        let iceberg = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(
                    1,
                    "ts_map",
                    Type::Map(MapType {
                        key_field: NestedField::required(
                            2,
                            "key",
                            Type::Primitive(PrimitiveType::String),
                        )
                        .into(),
                        value_field: NestedField::optional(
                            3,
                            "value",
                            Type::Primitive(PrimitiveType::Timestamp),
                        )
                        .into(),
                    }),
                )
                .into(),
            ])
            .build()
            .unwrap();

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

        let coerced = coerce(&arrow_schema, &iceberg).unwrap();
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
