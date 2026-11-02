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

//! ManifestList for Iceberg.

mod _const_schema;
pub(super) mod _serde;
mod manifest_file;
mod reader;
mod writer;

use apache_avro::types::Value;
use apache_avro::{Reader, from_value};
pub use manifest_file::*;
pub use reader::*;
pub use serde_bytes::ByteBuf;
pub use writer::*;

use super::FormatVersion;
use crate::error::Result;

/// Placeholder for sequence number. The field with this value must be replaced with the actual sequence number before it write.
pub const UNASSIGNED_SEQUENCE_NUMBER: i64 = -1;

/// Snapshots are embedded in table metadata, but the list of manifests for a
/// snapshot are stored in a separate manifest list file.
///
/// A new manifest list is written for each attempt to commit a snapshot
/// because the list of manifests always changes to produce a new snapshot.
/// When a manifest list is written, the (optimistic) sequence number of the
/// snapshot is written for all new manifest files tracked by the list.
///
/// A manifest list includes summary metadata that can be used to avoid
/// scanning all of the manifests in a snapshot when planning a table scan.
/// This includes the number of added, existing, and deleted files, and a
/// summary of values for each field of the partition spec used to write the
/// manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestList {
    /// Entries in a manifest list.
    entries: Vec<ManifestFile>,
}

impl ManifestList {
    /// Parse manifest list from bytes.
    pub fn parse_with_version(bs: &[u8], version: FormatVersion) -> Result<ManifestList> {
        match version {
            FormatVersion::V1 => {
                // Avro schema resolution matches fields by name, and v1 writers use two names
                // for the file counts; serde aliases on `ManifestFileV1` accept both.
                let reader = Reader::new(bs)?;
                let values = Value::Array(reader.collect::<std::result::Result<Vec<Value>, _>>()?);
                from_value::<_serde::ManifestListV1>(&values)?.try_into()
            }
            FormatVersion::V2 => {
                let reader = Reader::new(bs)?;
                let values = Value::Array(reader.collect::<std::result::Result<Vec<Value>, _>>()?);
                from_value::<_serde::ManifestListV2>(&values)?.try_into()
            }
            FormatVersion::V3 => {
                let reader = Reader::new(bs)?;
                let values = Value::Array(reader.collect::<std::result::Result<Vec<Value>, _>>()?);
                from_value::<_serde::ManifestListV3>(&values)?.try_into()
            }
        }
    }

    /// Get the entries in the manifest list.
    pub fn entries(&self) -> &[ManifestFile] {
        &self.entries
    }

    /// Take ownership of the entries in the manifest list, consuming it
    pub fn consume_entries(self) -> impl IntoIterator<Item = ManifestFile> {
        Box::new(self.entries.into_iter())
    }
}

#[cfg(test)]
mod test {
    use std::fs;

    use apache_avro::types::{Record, Value};
    use apache_avro::{Codec, Schema as AvroSchema, Writer};
    use tempfile::TempDir;

    use super::_const_schema::MANIFEST_LIST_AVRO_SCHEMA_V2;
    use super::_serde::ManifestFileV2;
    use super::*;
    use crate::io::FileIO;
    use crate::spec::{Datum, FieldSummary, ManifestContentType, ManifestFile};

    #[tokio::test]
    async fn test_parse_manifest_list_v1() {
        let manifest_list = ManifestList {
            entries: vec![
                ManifestFile {
                    manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                    manifest_length: 5806,
                    partition_spec_id: 0,
                    content: ManifestContentType::Data,
                    sequence_number: 0,
                    min_sequence_number: 0,
                    added_snapshot_id: 1646658105718557341,
                    added_files_count: Some(3),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(vec![]),
                    key_metadata: None,
                    first_row_id: None,
                }
            ]
        };

        let file_io = FileIO::new_with_fs();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v1.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v1(
            file_io
                .new_output(full_path.clone())
                .unwrap()
                .writer()
                .await
                .unwrap(),
            1646658105718557341,
            Some(1646658105718557341),
        );

        writer
            .add_manifests(manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(full_path).expect("read_file must succeed");

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, FormatVersion::V1).unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }

    #[tokio::test]
    async fn test_parse_manifest_list_v2() {
        let manifest_list = ManifestList {
            entries: vec![
                ManifestFile {
                    manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                    manifest_length: 6926,
                    partition_spec_id: 1,
                    content: ManifestContentType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 377075049360453639,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: None,
                },
                ManifestFile {
                    manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m1.avro".to_string(),
                    manifest_length: 6926,
                    partition_spec_id: 2,
                    content: ManifestContentType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 377075049360453639,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::float(1.1_f32).to_bytes().unwrap()), upper_bound: Some(Datum::float(2.1_f32).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: None,
                }
            ]
        };

        let file_io = FileIO::new_with_fs();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v1.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v2(
            file_io
                .new_output(full_path.clone())
                .unwrap()
                .writer()
                .await
                .unwrap(),
            1646658105718557341,
            Some(1646658105718557341),
            1,
        );

        writer
            .add_manifests(manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(full_path).expect("read_file must succeed");

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, FormatVersion::V2).unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }

    /// A v1 manifest list as Java Iceberg 1.5 and later writes it: the file counts are named
    /// `added_files_count` etc., as in the spec (`V1Metadata.MANIFEST_LIST_SCHEMA`).
    fn java_v1_manifest_list() -> Vec<u8> {
        let opt = |name: &str, ty: &str, id: i32| {
            format!(r#"{{"name":"{name}","type":["null","{ty}"],"default":null,"field-id":{id}}}"#)
        };
        let schema = AvroSchema::parse_str(&format!(
            r#"{{"type":"record","name":"manifest_file","fields":[
                {{"name":"manifest_path","type":"string","field-id":500}},
                {{"name":"manifest_length","type":"long","field-id":501}},
                {{"name":"partition_spec_id","type":"int","field-id":502}},
                {{"name":"added_snapshot_id","type":"long","field-id":503}},
                {},{},{},
                {{"name":"partitions","type":["null",{{"type":"array","items":{{"type":"record",
                    "name":"r508","fields":[
                    {{"name":"contains_null","type":"boolean","field-id":509}},
                    {},{},{}]}},"element-id":508}}],"default":null,"field-id":507}},
                {},{},{},{}]}}"#,
            opt("added_files_count", "int", 504),
            opt("existing_files_count", "int", 505),
            opt("deleted_files_count", "int", 506),
            opt("contains_nan", "boolean", 518),
            opt("lower_bound", "bytes", 510),
            opt("upper_bound", "bytes", 511),
            opt("added_rows_count", "long", 512),
            opt("existing_rows_count", "long", 513),
            opt("deleted_rows_count", "long", 514),
            opt("key_metadata", "bytes", 519),
        ))
        .unwrap();
        let some = |v: Value| Value::Union(1, Box::new(v));
        let bound = |n: i64| some(Value::Bytes(Datum::long(n).to_bytes().unwrap().to_vec()));
        let summary = Value::Record(vec![
            ("contains_null".to_string(), Value::Boolean(false)),
            ("contains_nan".to_string(), some(Value::Boolean(false))),
            ("lower_bound".to_string(), bound(1)),
            ("upper_bound".to_string(), bound(4)),
        ]);
        let mut record = Record::new(&schema).unwrap();
        record.put("manifest_path", "s3://bucket/t/metadata/java-m0.avro");
        record.put("manifest_length", 6000i64);
        record.put("partition_spec_id", 0i32);
        record.put("added_snapshot_id", 77i64);
        record.put("added_files_count", some(Value::Int(3)));
        record.put("existing_files_count", some(Value::Int(2)));
        record.put("deleted_files_count", some(Value::Int(1)));
        record.put("partitions", some(Value::Array(vec![summary])));
        record.put("added_rows_count", some(Value::Long(30)));
        record.put("existing_rows_count", some(Value::Long(20)));
        record.put("deleted_rows_count", some(Value::Long(10)));
        record.put("key_metadata", Value::Union(0, Box::new(Value::Null)));
        let mut writer = Writer::new(&schema, Vec::new()).unwrap();
        writer.append_value(record).unwrap();
        writer.into_inner().unwrap()
    }

    fn java_v1_manifest_file() -> ManifestFile {
        ManifestFile {
            manifest_path: "s3://bucket/t/metadata/java-m0.avro".to_string(),
            manifest_length: 6000,
            partition_spec_id: 0,
            content: ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 77,
            added_files_count: Some(3),
            existing_files_count: Some(2),
            deleted_files_count: Some(1),
            added_rows_count: Some(30),
            existing_rows_count: Some(20),
            deleted_rows_count: Some(10),
            partitions: Some(vec![FieldSummary {
                contains_null: false,
                contains_nan: Some(false),
                lower_bound: Some(Datum::long(1).to_bytes().unwrap()),
                upper_bound: Some(Datum::long(4).to_bytes().unwrap()),
            }]),
            key_metadata: None,
            first_row_id: None,
        }
    }

    #[test]
    fn test_parse_manifest_list_v1_with_java_field_names() {
        let parsed =
            ManifestList::parse_with_version(&java_v1_manifest_list(), FormatVersion::V1).unwrap();

        assert_eq!(parsed.entries(), &[java_v1_manifest_file()]);
    }

    /// A v1 table carries a Java-written manifest into a new manifest list and is then upgraded
    /// to v2, where the file counts are required.
    #[tokio::test]
    async fn test_java_v1_manifest_carried_forward_keeps_counts_after_upgrade() {
        let carried =
            ManifestList::parse_with_version(&java_v1_manifest_list(), FormatVersion::V1).unwrap();
        let tmp_dir = TempDir::new().unwrap();
        let path = format!("{}/carried.avro", tmp_dir.path().to_str().unwrap());
        let output = FileIO::new_with_fs().new_output(path.clone()).unwrap();
        let mut writer = ManifestListWriter::v1(output.writer().await.unwrap(), 78, Some(77));
        writer
            .add_manifests(carried.consume_entries().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let parsed =
            ManifestList::parse_with_version(&fs::read(path).unwrap(), FormatVersion::V2).unwrap();

        assert_eq!(parsed.entries(), &[java_v1_manifest_file()]);
    }

    #[test]
    fn test_parse_snappy_manifest_list_v2() {
        let manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/snappy-m0.avro".to_string(),
                manifest_length: 6926,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: 1,
                min_sequence_number: 1,
                added_snapshot_id: 377075049360453639,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(vec![FieldSummary {
                    contains_null: false,
                    contains_nan: Some(false),
                    lower_bound: Some(Datum::long(1).to_bytes().unwrap()),
                    upper_bound: Some(Datum::long(1).to_bytes().unwrap()),
                }]),
                key_metadata: None,
                first_row_id: None,
            }],
        };

        let manifest_entry: ManifestFileV2 = manifest_list.entries[0].clone().try_into().unwrap();
        let mut writer =
            Writer::with_codec(&MANIFEST_LIST_AVRO_SCHEMA_V2, Vec::new(), Codec::Snappy).unwrap();
        writer.append_ser(manifest_entry).unwrap();
        let bs = writer.into_inner().unwrap();

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, FormatVersion::V2).unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }

    #[tokio::test]
    async fn test_parse_manifest_list_v3() {
        let manifest_list = ManifestList {
            entries: vec![
                ManifestFile {
                    manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                    manifest_length: 6926,
                    partition_spec_id: 1,
                    content: ManifestContentType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 377075049360453639,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: Some(10),
                },
                ManifestFile {
                    manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m1.avro".to_string(),
                    manifest_length: 6926,
                    partition_spec_id: 2,
                    content: ManifestContentType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 377075049360453639,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::float(1.1_f32).to_bytes().unwrap()), upper_bound: Some(Datum::float(2.1_f32).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: Some(13),
                }
            ]
        };

        let file_io = FileIO::new_with_fs();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v3.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v3(
            file_io
                .new_output(full_path.clone())
                .unwrap()
                .writer()
                .await
                .unwrap(),
            377075049360453639,
            Some(377075049360453639),
            1,
            Some(10),
        );

        writer
            .add_manifests(manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();

        let bs = fs::read(full_path).expect("read_file must succeed");

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, FormatVersion::V3).unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }
}
