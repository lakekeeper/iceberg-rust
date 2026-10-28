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

//! Which column metrics a written data file records, as Iceberg Java's
//! `MetricsConfig` decides it from the `write.metadata.metrics.*` table
//! properties.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::str::FromStr;

use crate::error::invalid_data;
use crate::metadata_columns::{
    RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
};
use crate::spec::{
    Datum, PartitionSpec, PrimitiveLiteral, PrimitiveType, Schema, SortOrder, StructType,
    TableMetadata, TableProperties, Type,
};
use crate::util::truncate::{
    truncate_binary_max, truncate_binary_min, truncate_string_max, truncate_string_min,
};
use crate::{Error, Result};

/// Length a string or binary bound is truncated to by default, and for sort
/// columns whose configured mode records no bounds.
const DEFAULT_TRUNCATE_LENGTH: NonZeroUsize = NonZeroUsize::new(16).unwrap();

/// The metrics a data file records for one column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetricsMode {
    /// No metrics: no column size, value, null or `NaN` count and no bounds.
    None,
    /// Column size and value, null and `NaN` counts, no bounds.
    Counts,
    /// Counts and bounds; string and binary bounds keep at most this many
    /// characters or bytes.
    Truncate(NonZeroUsize),
    /// Counts and bounds, with bounds holding the whole value.
    Full,
}

impl MetricsMode {
    /// Whether the mode records lower and upper bounds.
    pub fn has_bounds(&self) -> bool {
        matches!(self, Self::Truncate(_) | Self::Full)
    }

    /// The mode a sort column gets: `truncate(16)` when `default` records no
    /// bounds, `default` otherwise.
    fn including_bounds(default: Option<Self>) -> Self {
        match default {
            Some(mode) if mode.has_bounds() => mode,
            _ => Self::Truncate(DEFAULT_TRUNCATE_LENGTH),
        }
    }
}

impl Default for MetricsMode {
    fn default() -> Self {
        Self::Truncate(DEFAULT_TRUNCATE_LENGTH)
    }
}

impl FromStr for MetricsMode {
    type Err = Error;

    /// Parses `none`, `counts`, `full` or `truncate(<length>)`, ignoring case.
    fn from_str(mode: &str) -> Result<Self> {
        let lower = mode.to_ascii_lowercase();
        match lower.as_str() {
            "none" => return Ok(Self::None),
            "counts" => return Ok(Self::Counts),
            "full" => return Ok(Self::Full),
            _ => {}
        }
        let length = lower
            .strip_prefix("truncate(")
            .and_then(|rest| rest.strip_suffix(')'))
            .filter(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
            .ok_or_else(|| invalid_data!("Invalid metrics mode: {mode}"))?;
        let length: usize = length
            .parse()
            .map_err(|e| invalid_data!("Invalid metrics mode: {mode}").with_source(e))?;
        NonZeroUsize::new(length)
            .map(Self::Truncate)
            .ok_or_else(|| invalid_data!("Truncate length should be positive: {mode}"))
    }
}

impl fmt::Display for MetricsMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("none"),
            Self::Counts => f.write_str("counts"),
            Self::Truncate(length) => write!(f, "truncate({length})"),
            Self::Full => f.write_str("full"),
        }
    }
}

/// The [`MetricsMode`] of every column of a written data file.
///
/// [`MetricsConfig::for_table`] follows Iceberg Java's `MetricsConfig.forTable`:
/// * `write.metadata.metrics.default` sets the mode of every column. When it is
///   not set, the first `write.metadata.metrics.max-inferred-column-defaults`
///   (100) primitive columns get `truncate(16)` and the others `none`.
/// * A column of the default sort order gets at least `truncate(16)`.
/// * `write.metadata.metrics.column.<full column name>` sets one column's mode.
/// * A source column of an order-preserving partition field gets `full`,
///   whatever else is configured.
///
/// An invalid mode or count is logged and ignored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricsConfig {
    column_modes: HashMap<i32, MetricsMode>,
    default_mode: MetricsMode,
}

impl Default for MetricsConfig {
    /// `truncate(16)` for every column.
    fn default() -> Self {
        Self {
            column_modes: HashMap::new(),
            default_mode: MetricsMode::default(),
        }
    }
}

impl MetricsConfig {
    /// The metrics config of data files written to the table.
    pub fn for_table(metadata: &TableMetadata) -> Self {
        Self::from_properties(
            metadata.properties(),
            metadata.current_schema(),
            Some(metadata.default_sort_order()),
            Some(metadata.default_partition_spec()),
        )
    }

    /// Every metric, with whole bounds, for every column.
    pub fn full() -> Self {
        Self {
            column_modes: HashMap::new(),
            default_mode: MetricsMode::Full,
        }
    }

    /// The metrics config of position delete files: `full` for the file path
    /// and position, `none` for everything else.
    pub fn for_position_delete() -> Self {
        Self {
            column_modes: HashMap::from([
                (RESERVED_FIELD_ID_DELETE_FILE_PATH, MetricsMode::Full),
                (RESERVED_FIELD_ID_DELETE_FILE_POS, MetricsMode::Full),
            ]),
            default_mode: MetricsMode::None,
        }
    }

    /// The metrics config that `properties` give columns of `schema`, with the
    /// columns of `sort_order` and the partition sources of `spec` promoted.
    pub fn from_properties(
        properties: &HashMap<String, String>,
        schema: &Schema,
        sort_order: Option<&SortOrder>,
        spec: Option<&PartitionSpec>,
    ) -> Self {
        let max_inferred = max_inferred_column_defaults(properties);
        let configured_default = properties
            .get(TableProperties::PROPERTY_WRITE_METADATA_METRICS_DEFAULT)
            .and_then(|mode| parse_mode(mode, "default"));

        let inferred = limit_field_ids(schema, max_inferred);
        let default_mode = match configured_default {
            Some(mode) => mode,
            None if inferred.len() < max_inferred => MetricsMode::default(),
            None => MetricsMode::None,
        };
        let inferred_mode = configured_default.unwrap_or_default();
        let mut column_modes: HashMap<i32, MetricsMode> = inferred
            .into_iter()
            .map(|field_id| (field_id, inferred_mode))
            .collect();

        if let Some(sort_order) = sort_order {
            let sort_mode = MetricsMode::including_bounds(configured_default);
            for field in &sort_order.fields {
                if field.transform.preserves_order()
                    && schema.field_by_id(field.source_id).is_some()
                {
                    column_modes.insert(field.source_id, sort_mode);
                }
            }
        }

        let field_ids_by_name: HashMap<&str, i32> = schema
            .field_id_to_name_map()
            .iter()
            .map(|(field_id, name)| (name.as_str(), *field_id))
            .collect();
        for (key, value) in properties {
            let Some(column) =
                key.strip_prefix(TableProperties::PROPERTY_WRITE_METADATA_METRICS_COLUMN_PREFIX)
            else {
                continue;
            };
            if let (Some(field_id), Some(mode)) = (
                field_ids_by_name.get(column),
                parse_mode(value, &format!("column {column}")),
            ) {
                column_modes.insert(*field_id, mode);
            }
        }

        if let Some(spec) = spec {
            for field in spec.fields() {
                if field.transform.preserves_order()
                    && schema.field_by_id(field.source_id).is_some()
                {
                    column_modes.insert(field.source_id, MetricsMode::Full);
                }
            }
        }

        Self {
            column_modes,
            default_mode,
        }
    }

    /// The mode of the column with `field_id`.
    pub fn column_mode(&self, field_id: i32) -> MetricsMode {
        self.column_modes
            .get(&field_id)
            .copied()
            .unwrap_or(self.default_mode)
    }

    /// Drops the metrics each column's mode does not record and truncates
    /// string and binary bounds to the mode's length.
    pub(crate) fn apply(&self, metrics: &mut ColumnMetrics) {
        let field_ids: HashSet<i32> = metrics.field_ids().collect();
        for field_id in field_ids {
            match self.column_mode(field_id) {
                MetricsMode::None => metrics.remove(field_id),
                MetricsMode::Counts => metrics.remove_bounds(field_id),
                MetricsMode::Truncate(length) => metrics.truncate_bounds(field_id, length.get()),
                MetricsMode::Full => {}
            }
        }
    }
}

/// The per-column metrics of one data file, keyed by field id.
#[derive(Debug, Default)]
pub(crate) struct ColumnMetrics {
    pub column_sizes: HashMap<i32, u64>,
    pub value_counts: HashMap<i32, u64>,
    pub null_value_counts: HashMap<i32, u64>,
    pub nan_value_counts: HashMap<i32, u64>,
    pub lower_bounds: HashMap<i32, Datum>,
    pub upper_bounds: HashMap<i32, Datum>,
}

impl ColumnMetrics {
    fn field_ids(&self) -> impl Iterator<Item = i32> + '_ {
        self.column_sizes
            .keys()
            .chain(self.value_counts.keys())
            .chain(self.null_value_counts.keys())
            .chain(self.nan_value_counts.keys())
            .chain(self.lower_bounds.keys())
            .chain(self.upper_bounds.keys())
            .copied()
    }

    fn remove(&mut self, field_id: i32) {
        self.column_sizes.remove(&field_id);
        self.value_counts.remove(&field_id);
        self.null_value_counts.remove(&field_id);
        self.nan_value_counts.remove(&field_id);
        self.remove_bounds(field_id);
    }

    fn remove_bounds(&mut self, field_id: i32) {
        self.lower_bounds.remove(&field_id);
        self.upper_bounds.remove(&field_id);
    }

    /// Truncates the column's string or binary bounds to `length`. An upper
    /// bound with no truncated value above it is dropped; the lower bound stays.
    fn truncate_bounds(&mut self, field_id: i32, length: usize) {
        if let Some(lower) = self.lower_bounds.get_mut(&field_id) {
            *lower = truncate_lower_bound(lower, length);
        }
        if let Some(upper) = self.upper_bounds.remove(&field_id)
            && let Some(upper) = truncate_upper_bound(upper, length)
        {
            self.upper_bounds.insert(field_id, upper);
        }
    }
}

fn truncate_lower_bound(bound: &Datum, length: usize) -> Datum {
    match (bound.data_type(), bound.literal()) {
        (PrimitiveType::String, PrimitiveLiteral::String(value)) => {
            Datum::string(truncate_string_min(value, length))
        }
        (PrimitiveType::Binary, PrimitiveLiteral::Binary(value)) => {
            Datum::binary(truncate_binary_min(value, length).to_vec())
        }
        _ => bound.clone(),
    }
}

fn truncate_upper_bound(bound: Datum, length: usize) -> Option<Datum> {
    match (bound.data_type(), bound.literal()) {
        (PrimitiveType::String, PrimitiveLiteral::String(value)) => {
            truncate_string_max(value, length).map(Datum::string)
        }
        (PrimitiveType::Binary, PrimitiveLiteral::Binary(value)) => {
            truncate_binary_max(value, length).map(Datum::binary)
        }
        _ => Some(bound),
    }
}

fn parse_mode(mode: &str, context: &str) -> Option<MetricsMode> {
    match mode.parse() {
        Ok(mode) => Some(mode),
        Err(err) => {
            tracing::warn!("Ignoring invalid metrics mode ({context}): {mode}: {err}");
            None
        }
    }
}

fn max_inferred_column_defaults(properties: &HashMap<String, String>) -> usize {
    let key = TableProperties::PROPERTY_WRITE_METADATA_METRICS_MAX_INFERRED_COLUMN_DEFAULTS;
    let default =
        TableProperties::PROPERTY_WRITE_METADATA_METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT;
    let Some(value) = properties.get(key) else {
        return default;
    };
    match value.trim().parse::<i64>() {
        Ok(count) if count >= 0 => usize::try_from(count).unwrap_or(usize::MAX),
        _ => {
            tracing::warn!("Invalid value for {key}: {value}, falling back to {default}");
            default
        }
    }
}

/// The ids of the first `limit` primitive or variant columns of `schema`: a
/// struct's own columns before the columns nested in them, a list's element
/// and a map's key and value before their nested columns.
fn limit_field_ids(schema: &Schema, limit: usize) -> HashSet<i32> {
    let mut field_ids = HashSet::new();
    visit_struct(schema.as_struct(), limit, &mut field_ids);
    field_ids
}

fn has_metrics(ty: &Type) -> bool {
    matches!(ty, Type::Primitive(_) | Type::Variant(_))
}

fn visit_struct(struct_type: &StructType, limit: usize, field_ids: &mut HashSet<i32>) {
    for field in struct_type.fields() {
        if field_ids.len() >= limit {
            return;
        }
        if has_metrics(&field.field_type) {
            field_ids.insert(field.id);
        }
    }
    for field in struct_type.fields() {
        if field_ids.len() >= limit {
            return;
        }
        visit_type(&field.field_type, limit, field_ids);
    }
}

fn visit_type(ty: &Type, limit: usize, field_ids: &mut HashSet<i32>) {
    match ty {
        Type::Struct(struct_type) => visit_struct(struct_type, limit, field_ids),
        Type::List(list) => {
            if field_ids.len() < limit && has_metrics(&list.element_field.field_type) {
                field_ids.insert(list.element_field.id);
            }
            visit_type(&list.element_field.field_type, limit, field_ids);
        }
        Type::Map(map) => {
            for field in [&map.key_field, &map.value_field] {
                if field_ids.len() < limit && has_metrics(&field.field_type) {
                    field_ids.insert(field.id);
                }
            }
            visit_type(&map.key_field.field_type, limit, field_ids);
            visit_type(&map.value_field.field_type, limit, field_ids);
        }
        Type::Primitive(_) | Type::Variant(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::spec::{
        ListType, MapType, NestedField, NullOrder, SortDirection, SortField, Transform,
    };

    fn truncate(length: usize) -> MetricsMode {
        MetricsMode::Truncate(NonZeroUsize::new(length).unwrap())
    }

    fn props(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn column(name: &str) -> String {
        format!(
            "{}{name}",
            TableProperties::PROPERTY_WRITE_METADATA_METRICS_COLUMN_PREFIX
        )
    }

    fn primitive(id: i32, name: &str) -> Arc<NestedField> {
        NestedField::optional(id, name, Type::Primitive(PrimitiveType::String)).into()
    }

    /// `id`, `data`, `location { lat, long }`, `tags: list<string>`,
    /// `attrs: map<string, string>`.
    fn nested_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                primitive(1, "id"),
                NestedField::optional(
                    2,
                    "location",
                    Type::Struct(StructType::new(vec![
                        primitive(3, "lat"),
                        primitive(4, "long"),
                    ])),
                )
                .into(),
                primitive(5, "data"),
                NestedField::optional(
                    6,
                    "tags",
                    Type::List(ListType::new(primitive(7, "element"))),
                )
                .into(),
                NestedField::optional(
                    8,
                    "attrs",
                    Type::Map(MapType::new(
                        NestedField::required(9, "key", Type::Primitive(PrimitiveType::String))
                            .into(),
                        primitive(10, "value"),
                    )),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    fn wide_schema(columns: i32) -> Schema {
        Schema::builder()
            .with_fields((1..=columns).map(|id| primitive(id, &format!("c{id}"))))
            .build()
            .unwrap()
    }

    fn modes(config: &MetricsConfig, field_ids: impl IntoIterator<Item = i32>) -> Vec<MetricsMode> {
        field_ids
            .into_iter()
            .map(|id| config.column_mode(id))
            .collect()
    }

    #[test]
    fn parses_metrics_modes_ignoring_case() {
        assert_eq!("none".parse::<MetricsMode>().unwrap(), MetricsMode::None);
        assert_eq!("nOnE".parse::<MetricsMode>().unwrap(), MetricsMode::None);
        assert_eq!(
            "counts".parse::<MetricsMode>().unwrap(),
            MetricsMode::Counts
        );
        assert_eq!(
            "coUntS".parse::<MetricsMode>().unwrap(),
            MetricsMode::Counts
        );
        assert_eq!("truncate(1)".parse::<MetricsMode>().unwrap(), truncate(1));
        assert_eq!("truNcAte(10)".parse::<MetricsMode>().unwrap(), truncate(10));
        assert_eq!("full".parse::<MetricsMode>().unwrap(), MetricsMode::Full);
        assert_eq!("FULL".parse::<MetricsMode>().unwrap(), MetricsMode::Full);
    }

    #[test]
    fn rejects_invalid_metrics_modes() {
        for mode in [
            "truncate(0)",
            "truncate()",
            "truncate(-1)",
            "truncate(x)",
            "truncate 16",
            "all",
            "",
        ] {
            assert!(mode.parse::<MetricsMode>().is_err(), "{mode:?} parsed");
        }
    }

    #[test]
    fn displays_metrics_modes_as_parsed() {
        for mode in [
            MetricsMode::None,
            MetricsMode::Counts,
            truncate(16),
            MetricsMode::Full,
        ] {
            assert_eq!(mode.to_string().parse::<MetricsMode>().unwrap(), mode);
        }
    }

    #[test]
    fn default_config_truncates_every_column_to_16() {
        let config = MetricsConfig::default();
        assert_eq!(modes(&config, [1, 1000]), vec![truncate(16), truncate(16)]);
    }

    #[test]
    fn full_config_records_every_column_in_full() {
        let config = MetricsConfig::full();
        assert_eq!(modes(&config, [1, 1000]), vec![MetricsMode::Full; 2]);
    }

    #[test]
    fn position_delete_config_records_only_path_and_position() {
        let config = MetricsConfig::for_position_delete();
        assert_eq!(
            modes(&config, [
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
                RESERVED_FIELD_ID_DELETE_FILE_POS,
                1
            ]),
            vec![MetricsMode::Full, MetricsMode::Full, MetricsMode::None]
        );
    }

    #[test]
    fn unset_properties_infer_truncate_16_for_the_first_100_columns() {
        let config = MetricsConfig::from_properties(&props(&[]), &wide_schema(101), None, None);
        assert_eq!(modes(&config, [1, 100, 101]), vec![
            truncate(16),
            truncate(16),
            MetricsMode::None
        ]);
    }

    #[test]
    fn narrow_schema_keeps_truncate_16_for_columns_added_later() {
        let config = MetricsConfig::from_properties(&props(&[]), &wide_schema(3), None, None);
        assert_eq!(modes(&config, [1, 3, 4]), vec![truncate(16); 3]);
    }

    #[test]
    fn inferred_columns_follow_java_order() {
        // The top-level columns first, then the nested ones: `location`'s fields,
        // `tags`'s element, `attrs`'s key and value.
        let schema = nested_schema();
        let max = |count: &str| {
            props(&[(
                TableProperties::PROPERTY_WRITE_METADATA_METRICS_MAX_INFERRED_COLUMN_DEFAULTS,
                count,
            )])
        };
        let config = MetricsConfig::from_properties(&max("3"), &schema, None, None);
        assert_eq!(modes(&config, 1..=10), vec![
            truncate(16),
            MetricsMode::None,
            truncate(16),
            MetricsMode::None,
            truncate(16),
            MetricsMode::None,
            MetricsMode::None,
            MetricsMode::None,
            MetricsMode::None,
            MetricsMode::None,
        ]);
        let config = MetricsConfig::from_properties(&max("5"), &schema, None, None);
        assert_eq!(modes(&config, [3, 4, 7, 9]), vec![
            truncate(16),
            truncate(16),
            truncate(16),
            MetricsMode::None
        ]);
        let config = MetricsConfig::from_properties(&max("0"), &schema, None, None);
        assert_eq!(modes(&config, [1]), vec![MetricsMode::None]);
    }

    #[test]
    fn invalid_max_inferred_column_defaults_falls_back_to_100() {
        for count in ["-1", "many"] {
            let properties = props(&[(
                TableProperties::PROPERTY_WRITE_METADATA_METRICS_MAX_INFERRED_COLUMN_DEFAULTS,
                count,
            )]);
            let config = MetricsConfig::from_properties(&properties, &wide_schema(101), None, None);
            assert_eq!(modes(&config, [100, 101]), vec![
                truncate(16),
                MetricsMode::None
            ]);
        }
    }

    #[test]
    fn configured_default_applies_to_every_column() {
        let properties = props(&[(
            TableProperties::PROPERTY_WRITE_METADATA_METRICS_DEFAULT,
            "counts",
        )]);
        let config = MetricsConfig::from_properties(&properties, &wide_schema(101), None, None);
        assert_eq!(modes(&config, [1, 101, 500]), vec![MetricsMode::Counts; 3]);
    }

    #[test]
    fn invalid_configured_default_is_ignored() {
        let properties = props(&[(
            TableProperties::PROPERTY_WRITE_METADATA_METRICS_DEFAULT,
            "truncate(0)",
        )]);
        let config = MetricsConfig::from_properties(&properties, &wide_schema(101), None, None);
        assert_eq!(modes(&config, [1, 101]), vec![
            truncate(16),
            MetricsMode::None
        ]);
    }

    #[test]
    fn column_overrides_use_full_names() {
        let properties = props(&[
            (
                TableProperties::PROPERTY_WRITE_METADATA_METRICS_DEFAULT,
                "none",
            ),
            (&column("data"), "full"),
            (&column("location.lat"), "truncate(4)"),
            (&column("tags.element"), "counts"),
            (&column("unknown"), "full"),
            (&column("id"), "bogus"),
        ]);
        let config = MetricsConfig::from_properties(&properties, &nested_schema(), None, None);
        assert_eq!(modes(&config, [1, 3, 4, 5, 7]), vec![
            MetricsMode::None,
            truncate(4),
            MetricsMode::None,
            MetricsMode::Full,
            MetricsMode::Counts,
        ]);
    }

    fn sort_order(fields: &[(i32, Transform)]) -> SortOrder {
        SortOrder {
            order_id: 1,
            fields: fields
                .iter()
                .map(|(source_id, transform)| SortField {
                    source_id: *source_id,
                    transform: *transform,
                    direction: SortDirection::Ascending,
                    null_order: NullOrder::First,
                })
                .collect(),
        }
    }

    #[test]
    fn sort_columns_get_bounds() {
        let schema = wide_schema(3);
        let order = sort_order(&[(1, Transform::Identity), (2, Transform::Bucket(4))]);
        for (default, sort_mode) in [
            ("none", truncate(16)),
            ("counts", truncate(16)),
            ("truncate(8)", truncate(8)),
            ("full", MetricsMode::Full),
        ] {
            let properties = props(&[(
                TableProperties::PROPERTY_WRITE_METADATA_METRICS_DEFAULT,
                default,
            )]);
            let config = MetricsConfig::from_properties(&properties, &schema, Some(&order), None);
            let default: MetricsMode = default.parse().unwrap();
            // A bucket transform does not preserve the order: column 2 keeps the default.
            assert_eq!(modes(&config, [1, 2, 3]), vec![sort_mode, default, default]);
        }
    }

    #[test]
    fn column_override_wins_over_sort_promotion() {
        let properties = props(&[(&column("c1"), "counts")]);
        let order = sort_order(&[(1, Transform::Identity)]);
        let config =
            MetricsConfig::from_properties(&properties, &wide_schema(2), Some(&order), None);
        assert_eq!(config.column_mode(1), MetricsMode::Counts);
    }

    #[test]
    fn partition_sources_get_full_over_any_override() {
        let schema = wide_schema(3);
        let spec = PartitionSpec::builder(schema.clone())
            .add_partition_field("c1", "c1_trunc", Transform::Truncate(2))
            .unwrap()
            .add_partition_field("c2", "c2_bucket", Transform::Bucket(4))
            .unwrap()
            .build()
            .unwrap();
        let properties = props(&[
            (
                TableProperties::PROPERTY_WRITE_METADATA_METRICS_DEFAULT,
                "none",
            ),
            (&column("c1"), "counts"),
        ]);
        let config = MetricsConfig::from_properties(&properties, &schema, None, Some(&spec));
        assert_eq!(modes(&config, [1, 2, 3]), vec![
            MetricsMode::Full,
            MetricsMode::None,
            MetricsMode::None
        ]);
    }

    fn metrics_for(field_id: i32, lower: Datum, upper: Datum) -> ColumnMetrics {
        ColumnMetrics {
            column_sizes: HashMap::from([(field_id, 10)]),
            value_counts: HashMap::from([(field_id, 3)]),
            null_value_counts: HashMap::from([(field_id, 1)]),
            nan_value_counts: HashMap::from([(field_id, 0)]),
            lower_bounds: HashMap::from([(field_id, lower)]),
            upper_bounds: HashMap::from([(field_id, upper)]),
        }
    }

    fn config_with(field_id: i32, mode: MetricsMode) -> MetricsConfig {
        MetricsConfig {
            column_modes: HashMap::from([(field_id, mode)]),
            default_mode: MetricsMode::Full,
        }
    }

    #[test]
    fn apply_none_drops_every_metric() {
        let mut metrics = metrics_for(1, Datum::int(1), Datum::int(2));
        config_with(1, MetricsMode::None).apply(&mut metrics);
        assert!(metrics.field_ids().next().is_none(), "{metrics:?}");
    }

    #[test]
    fn apply_counts_keeps_counts_and_drops_bounds() {
        let mut metrics = metrics_for(1, Datum::int(1), Datum::int(2));
        config_with(1, MetricsMode::Counts).apply(&mut metrics);
        assert_eq!(metrics.column_sizes, HashMap::from([(1, 10)]));
        assert_eq!(metrics.value_counts, HashMap::from([(1, 3)]));
        assert_eq!(metrics.null_value_counts, HashMap::from([(1, 1)]));
        assert_eq!(metrics.nan_value_counts, HashMap::from([(1, 0)]));
        assert_eq!(metrics.lower_bounds, HashMap::new());
        assert_eq!(metrics.upper_bounds, HashMap::new());
    }

    #[test]
    fn apply_truncate_shortens_string_and_binary_bounds_only() {
        let long = "z".repeat(20);
        let fixed = vec![0xAB; 20];
        let mut metrics = ColumnMetrics {
            lower_bounds: HashMap::from([
                (1, Datum::string(format!("a{long}"))),
                (2, Datum::binary(b"abcdefgh".to_vec())),
                (3, Datum::fixed(fixed.clone())),
                (4, Datum::long(7)),
            ]),
            upper_bounds: HashMap::from([
                (1, Datum::string(&long)),
                (2, Datum::binary(b"abcdefgh".to_vec())),
                (3, Datum::fixed(fixed.clone())),
                (4, Datum::long(9)),
            ]),
            ..ColumnMetrics::default()
        };
        MetricsConfig {
            column_modes: HashMap::new(),
            default_mode: truncate(4),
        }
        .apply(&mut metrics);
        assert_eq!(
            metrics.lower_bounds,
            HashMap::from([
                (1, Datum::string("azzz")),
                (2, Datum::binary(b"abcd".to_vec())),
                (3, Datum::fixed(fixed.clone())),
                (4, Datum::long(7)),
            ])
        );
        assert_eq!(
            metrics.upper_bounds,
            HashMap::from([
                (1, Datum::string("zzz{")),
                (2, Datum::binary(b"abce".to_vec())),
                (3, Datum::fixed(fixed)),
                (4, Datum::long(9)),
            ])
        );
    }

    #[test]
    fn apply_truncate_drops_an_upper_bound_that_cannot_be_incremented() {
        let max = "\u{10FFFF}".repeat(3);
        let mut metrics = metrics_for(1, Datum::string(&max), Datum::string(&max));
        config_with(1, truncate(2)).apply(&mut metrics);
        assert_eq!(
            metrics.lower_bounds,
            HashMap::from([(1, Datum::string("\u{10FFFF}".repeat(2)))])
        );
        assert_eq!(metrics.upper_bounds, HashMap::new());
    }

    #[test]
    fn apply_full_keeps_whole_bounds() {
        let long = "z".repeat(40);
        let mut metrics = metrics_for(1, Datum::string(&long), Datum::string(&long));
        config_with(1, MetricsMode::Full).apply(&mut metrics);
        assert_eq!(
            metrics.lower_bounds,
            HashMap::from([(1, Datum::string(&long))])
        );
        assert_eq!(
            metrics.upper_bounds,
            HashMap::from([(1, Datum::string(&long))])
        );
    }
}
