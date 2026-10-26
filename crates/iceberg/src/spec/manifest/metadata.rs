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

use std::collections::HashMap;
use std::sync::Arc;

use typed_builder::TypedBuilder;

use super::{FormatVersion, ManifestContentType, PartitionSpec, Schema};
use crate::error::{Result, invalid_data};
use crate::spec::{PartitionField, SchemaId, SchemaRef, TableMetadataRef};

/// Meta data of a manifest that is stored in the key-value metadata of the Avro file
#[derive(Debug, PartialEq, Clone, Eq, TypedBuilder)]
pub struct ManifestMetadata {
    /// The table schema at the time the manifest
    /// was written
    pub schema: SchemaRef,
    /// ID of the schema used to write the manifest as a string
    pub schema_id: SchemaId,
    /// The partition spec used to write the manifest
    pub partition_spec: PartitionSpec,
    /// Table format version number of the manifest as a string
    pub format_version: FormatVersion,
    /// Type of content files tracked by the manifest: “data” or “deletes”
    pub content: ManifestContentType,
}

impl ManifestMetadata {
    /// Parse from metadata in avro file.
    pub fn parse(meta: &HashMap<String, Vec<u8>>) -> Result<Self> {
        Self::parse_with(meta, None)
    }

    /// Parse from the avro file's key-value metadata. When the manifest records a `schema-id`
    /// and `partition-spec-id` that `table_metadata` contains, the schema and partition spec come
    /// from the table metadata, as in Java's `ManifestReader(specsById)`, because some writers
    /// (DuckDB 1.4) store a non-conformant `schema` key. Otherwise they are parsed from the
    /// manifest's own keys: a manifest without recorded IDs may have been written under any
    /// historical schema.
    pub(crate) fn parse_with(
        meta: &HashMap<String, Vec<u8>>,
        table_metadata: Option<&TableMetadataRef>,
    ) -> Result<Self> {
        // `None` when the writer omitted the key — deliberately NOT defaulted
        // before the table-metadata lookup below.
        let recorded_schema_id: Option<i32> = meta
            .get("schema-id")
            .map(|bs| {
                String::from_utf8_lossy(bs).parse().map_err(|err| {
                    invalid_data!("Fail to parse schema id in manifest metadata").with_source(err)
                })
            })
            .transpose()?;
        let recorded_spec_id: Option<i32> = meta
            .get("partition-spec-id")
            .map(|bs| {
                String::from_utf8_lossy(bs).parse().map_err(|err| {
                    invalid_data!("Fail to parse partition spec id in manifest metadata")
                        .with_source(err)
                })
            })
            .transpose()?;
        let schema_id = recorded_schema_id.unwrap_or(0);
        let spec_id = recorded_spec_id.unwrap_or(0);
        let format_version = if let Some(bs) = meta.get("format-version") {
            serde_json::from_slice::<FormatVersion>(bs).map_err(|err| {
                invalid_data!("Fail to parse format version in manifest metadata").with_source(err)
            })?
        } else {
            FormatVersion::V1
        };
        let content = if let Some(v) = meta.get("content") {
            let v = String::from_utf8_lossy(v);
            v.parse()?
        } else {
            ManifestContentType::Data
        };

        // Prefer the authoritative table schema + partition spec when the
        // manifest RECORDS the ids and the table metadata contains them,
        // bypassing the manifest's redundant (and sometimes non-conformant)
        // `schema` / `partition-spec` metadata keys. Manifests that omit the
        // ids stay on the self-describing path below.
        if let Some(table_metadata) = table_metadata
            && let (Some(schema), Some(partition_spec)) = (
                recorded_schema_id.and_then(|id| table_metadata.schema_by_id(id)),
                recorded_spec_id.and_then(|id| table_metadata.partition_spec_by_id(id)),
            )
        {
            return Ok(ManifestMetadata {
                schema: schema.clone(),
                schema_id,
                partition_spec: partition_spec.as_ref().clone(),
                format_version,
                content,
            });
        }

        // Fallback: parse the schema + partition spec from the manifest's own
        // key-value metadata (the manifest is self-describing).
        let schema = Arc::new({
            let bs = meta.get("schema").ok_or_else(|| {
                invalid_data!("schema is required in manifest metadata but not found")
            })?;
            serde_json::from_slice::<Schema>(bs).map_err(|err| {
                invalid_data!("Fail to parse schema in manifest metadata").with_source(err)
            })?
        });
        let fields = {
            let bs = meta.get("partition-spec").ok_or_else(|| {
                invalid_data!("partition-spec is required in manifest metadata but not found")
            })?;
            serde_json::from_slice::<Vec<PartitionField>>(bs).map_err(|err| {
                invalid_data!("Fail to parse partition spec in manifest metadata").with_source(err)
            })?
        };
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(spec_id)
            .add_unbound_fields(fields.into_iter().map(|f| f.into_unbound()))?
            .build()?;

        Ok(ManifestMetadata {
            schema,
            schema_id,
            partition_spec,
            format_version,
            content,
        })
    }

    /// Get the schema of table at the time manifest was written
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Get the ID of schema used to write the manifest
    pub fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    /// Get the partition spec used to write manifest
    pub fn partition_spec(&self) -> &PartitionSpec {
        &self.partition_spec
    }

    /// Get the table format version
    pub fn format_version(&self) -> &FormatVersion {
        &self.format_version
    }

    /// Get the type of content files tracked by manifest
    pub fn content(&self) -> &ManifestContentType {
        &self.content
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::spec::{
        NestedField, PrimitiveType, SortOrder, TableMetadataBuilder, Type, UnboundPartitionSpec,
    };

    fn table_metadata() -> TableMetadataRef {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            UnboundPartitionSpec::builder().build(),
            SortOrder::unsorted_order(),
            "memory:///table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        Arc::new(metadata)
    }

    #[test]
    fn test_manifest_metadata_parse_prefers_table_metadata_over_bad_schema() {
        let table_metadata = table_metadata();
        // DuckDB 1.4 writes the manifest entry's Avro schema, with Avro type names such as
        // `array`, as the `schema` key.
        let meta = HashMap::from([
            ("schema-id".to_string(), b"0".to_vec()),
            ("partition-spec-id".to_string(), b"0".to_vec()),
            ("format-version".to_string(), b"2".to_vec()),
            ("content".to_string(), b"data".to_vec()),
            (
                "schema".to_string(),
                br#"{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":true,"type":{"type":"array","items":"int"}}]}"#
                    .to_vec(),
            ),
        ]);

        assert!(ManifestMetadata::parse(&meta).is_err());
        assert_eq!(
            ManifestMetadata::parse_with(&meta, Some(&table_metadata)).unwrap(),
            ManifestMetadata {
                schema: table_metadata.current_schema().clone(),
                schema_id: 0,
                partition_spec: table_metadata.default_partition_spec().as_ref().clone(),
                format_version: FormatVersion::V2,
                content: ManifestContentType::Data,
            }
        );
    }

    #[test]
    fn test_manifest_metadata_parse_self_describes_when_ids_not_recorded() {
        let meta = HashMap::from([
            ("format-version".to_string(), b"2".to_vec()),
            ("content".to_string(), b"data".to_vec()),
            (
                "schema".to_string(),
                br#"{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"foo","required":false,"type":"long"}]}"#
                    .to_vec(),
            ),
            ("partition-spec".to_string(), b"[]".to_vec()),
        ]);
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        assert_eq!(
            ManifestMetadata::parse_with(&meta, Some(&table_metadata())).unwrap(),
            ManifestMetadata {
                partition_spec: PartitionSpec::builder(schema.clone()).build().unwrap(),
                schema,
                schema_id: 0,
                format_version: FormatVersion::V2,
                content: ManifestContentType::Data,
            }
        );
    }
}
