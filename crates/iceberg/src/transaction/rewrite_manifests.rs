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

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use futures::stream::{self, StreamExt};
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, Literal, MAIN_BRANCH, ManifestContentType,
    ManifestEntry, ManifestFile, ManifestList, ManifestListWriter, ManifestStatus, ManifestWriter,
    ManifestWriterBuilder, Operation, SchemaId, SchemaRef, Snapshot, SnapshotReference, Struct,
    Summary, TableProperties, partition_summaries,
};
use crate::table::Table;
use crate::transaction::snapshot::{generate_unique_snapshot_id, main_branch_retention};
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, TableRequirement, TableUpdate};

/// A transaction action that compacts the data manifests of the current snapshot.
///
/// Live `DataFile` entries from manifests bound to the default partition spec are
/// grouped by the schema their manifest was written with, sorted by partition,
/// and split evenly into `ceil(input bytes / target)` manifests, with the target
/// from [`RewriteManifestsAction::set_target_size_bytes`]. Each entry keeps its
/// snapshot id and sequence numbers. The result is committed as a
/// [`Operation::Replace`](crate::spec::Operation::Replace) snapshot. This
/// mirrors Java's Spark `RewriteManifestsSparkAction`.
///
/// Before it returns the commit, the action reads back what it wrote and fails
/// unless the new manifests hold exactly the live entries of the replaced ones.
/// Tables of format version 3 or higher are refused.
pub struct RewriteManifestsAction {
    target_size_bytes: Option<u64>,
    rewrite_if: Option<ManifestPredicate>,
    snapshot_properties: HashMap<String, String>,
    #[cfg(test)]
    corrupt_output: Option<tests::OutputHook>,
}

type ManifestPredicate = Arc<dyn Fn(&ManifestFile) -> bool + Send + Sync>;

impl RewriteManifestsAction {
    pub(crate) fn new() -> Self {
        Self {
            target_size_bytes: None,
            rewrite_if: None,
            snapshot_properties: HashMap::new(),
            #[cfg(test)]
            corrupt_output: None,
        }
    }

    /// Sets the target size in bytes for each rewritten manifest.
    ///
    /// Defaults to the table property `commit.manifest.target-size-bytes`
    /// (8 MiB when unset).
    pub fn set_target_size_bytes(mut self, target_size_bytes: u64) -> Self {
        self.target_size_bytes = Some(target_size_bytes);
        self
    }

    /// Rewrites only the data manifests of the default partition spec that
    /// match `predicate`, as Java's `rewriteIf`; the others are kept. By
    /// default every such manifest is rewritten.
    pub fn rewrite_if(
        mut self,
        predicate: impl Fn(&ManifestFile) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.rewrite_if = Some(Arc::new(predicate));
        self
    }

    /// Sets additional key/value properties to record in the new snapshot's summary.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

#[async_trait]
impl TransactionAction for RewriteManifestsAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let metadata = table.metadata();
        // Row lineage (first row ids and `next-row-id`) is not maintained here.
        if metadata.format_version() >= FormatVersion::V3 {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "RewriteManifests does not support format {} tables",
                    metadata.format_version()
                ),
            ));
        }
        let Some(current_snapshot) = metadata.current_snapshot() else {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "RewriteManifests requires the table to have a current snapshot",
            ));
        };

        let target_size_bytes = match self.target_size_bytes {
            Some(target_size_bytes) => target_size_bytes,
            None => {
                TableProperties::new(metadata.properties()).commit_manifest_target_size_bytes()?
            }
        };
        let default_spec_id = metadata.default_partition_spec_id();
        let format_version = metadata.format_version();

        let manifest_list = table.manifest_list_reader(current_snapshot).load().await?;

        let (to_rewrite, kept): (Vec<ManifestFile>, Vec<ManifestFile>) =
            manifest_list.entries().iter().cloned().partition(|m| {
                m.content == ManifestContentType::Data
                    && m.partition_spec_id == default_spec_id
                    && (m.has_added_files() || m.has_existing_files())
                    && self
                        .rewrite_if
                        .as_ref()
                        .is_none_or(|predicate| predicate(m))
            });

        if to_rewrite.is_empty() {
            return Ok(ActionCommit::new(vec![], vec![]));
        }

        let total_size: u64 = to_rewrite
            .iter()
            .map(|m| u64::try_from(m.manifest_length).unwrap_or(0))
            .sum();
        if to_rewrite.len() == 1 && total_size <= target_size_bytes {
            return Ok(ActionCommit::new(vec![], vec![]));
        }
        let manifests_replaced = to_rewrite.len();

        let commit_uuid = Uuid::now_v7();
        let snapshot_id = generate_unique_snapshot_id(table);

        let loaded: Vec<_> = stream::iter(to_rewrite)
            .map(|m| {
                let reader = table.manifest_reader();
                async move {
                    let length = u64::try_from(m.manifest_length).unwrap_or(0);
                    reader.read(&m).await.map(|manifest| (length, manifest))
                }
            })
            .buffer_unordered(16)
            .try_collect()
            .await?;

        // Each new manifest keeps the schema its entries were written with, so
        // their partition values keep fitting their types after a promotion or
        // a dropped source column.
        let mut groups: BTreeMap<SchemaId, SchemaGroup> = BTreeMap::new();
        let mut entries_processed: u64 = 0;
        let mut expected_entries = EntryCounts::default();

        for (length, manifest) in loaded {
            let schema = manifest.metadata().schema().clone();
            let group = groups
                .entry(schema.schema_id())
                .or_insert_with(|| SchemaGroup {
                    schema: schema.clone(),
                    input_bytes: 0,
                    entries: Vec::new(),
                });
            group.input_bytes = group.input_bytes.saturating_add(length);
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let entry_snapshot_id = entry.snapshot_id().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Live manifest entry is missing snapshot_id",
                    )
                })?;
                let sequence_number = entry.sequence_number().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Live manifest entry is missing sequence_number",
                    )
                })?;
                group.entries.push(ExistingEntry {
                    data_file: entry.data_file().clone(),
                    snapshot_id: entry_snapshot_id,
                    sequence_number,
                    file_sequence_number: entry.file_sequence_number,
                });
                expected_entries.add(entry);
                entries_processed += 1;
            }
        }

        let mut counter: u64 = 0;
        let mut new_manifests: Vec<ManifestFile> = Vec::new();

        for group in groups.into_values() {
            let SchemaGroup {
                schema,
                input_bytes,
                mut entries,
            } = group;
            if entries.is_empty() {
                continue;
            }
            // Sorted by partition, each manifest covers a narrow partition range,
            // so its partition summary keeps pruning scans.
            entries.sort_by(|a, b| {
                compare_partitions(&a.data_file.partition, &b.data_file.partition)
                    .then_with(|| a.data_file.file_path.cmp(&b.data_file.file_path))
            });
            let manifest_count = usize::try_from(input_bytes.div_ceil(target_size_bytes.max(1)))
                .unwrap_or(usize::MAX)
                .clamp(1, entries.len());
            let base = entries.len() / manifest_count;
            let larger = entries.len() % manifest_count;
            let mut entries = entries.into_iter();
            for i in 0..manifest_count {
                let size = base + usize::from(i < larger);
                let written = write_manifest(
                    table,
                    &schema,
                    &commit_uuid,
                    counter,
                    snapshot_id,
                    entries.by_ref().take(size),
                )
                .await?;
                counter += 1;
                new_manifests.push(written);
            }
        }

        let manifest_list_path = format!(
            "{}/metadata/snap-{}-0-{}.{}",
            metadata.location(),
            snapshot_id,
            commit_uuid,
            DataFileFormat::Avro,
        );
        let next_seq_num = metadata.next_sequence_number();
        let writer = table
            .file_io()
            .new_output(manifest_list_path.clone())?
            .writer()
            .await?;
        let mut list_writer = match format_version {
            FormatVersion::V1 => {
                ManifestListWriter::v1(writer, snapshot_id, metadata.current_snapshot_id())
            }
            FormatVersion::V2 => ManifestListWriter::v2(
                writer,
                snapshot_id,
                metadata.current_snapshot_id(),
                next_seq_num,
            ),
            FormatVersion::V3 => ManifestListWriter::v3(
                writer,
                snapshot_id,
                metadata.current_snapshot_id(),
                next_seq_num,
                Some(metadata.next_row_id()),
            ),
        };
        let manifests_created = new_manifests.len();
        let manifests_kept = kept.len();
        list_writer.add_manifests(new_manifests.into_iter().chain(kept.iter().cloned()))?;
        list_writer.close().await?;

        #[cfg(test)]
        if let Some(corrupt_output) = &self.corrupt_output {
            corrupt_output(table.clone(), manifest_list_path.clone()).await;
        }
        verify_output(
            table,
            &manifest_list_path,
            snapshot_id,
            next_seq_num,
            &kept,
            expected_entries,
        )
        .await?;

        let mut additional_properties: HashMap<String, String> = self.snapshot_properties.clone();
        for k in [
            "total-data-files",
            "total-delete-files",
            "total-records",
            "total-files-size",
            "total-position-deletes",
            "total-equality-deletes",
        ] {
            if let Some(v) = current_snapshot.summary().additional_properties.get(k) {
                additional_properties.insert(k.to_string(), v.clone());
            }
        }
        additional_properties.insert(
            "manifests-created".to_string(),
            manifests_created.to_string(),
        );
        additional_properties.insert(
            "manifests-replaced".to_string(),
            manifests_replaced.to_string(),
        );
        additional_properties.insert("manifests-kept".to_string(), manifests_kept.to_string());
        additional_properties.insert(
            "entries-processed".to_string(),
            entries_processed.to_string(),
        );
        let summary = Summary {
            operation: Operation::Replace,
            additional_properties,
        };

        let commit_ts = chrono::Utc::now().timestamp_millis();
        let new_snapshot = Snapshot::builder()
            .with_manifest_list(manifest_list_path)
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(metadata.current_snapshot_id())
            .with_sequence_number(next_seq_num)
            .with_summary(summary)
            .with_schema_id(metadata.current_schema_id())
            .with_timestamp_ms(commit_ts)
            .build();

        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: new_snapshot,
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(snapshot_id, main_branch_retention(metadata)),
            },
        ];
        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: metadata.current_snapshot_id(),
            },
        ];
        Ok(ActionCommit::new(updates, requirements))
    }
}

/// Live entries of the rewritten manifests written with one schema.
struct SchemaGroup {
    schema: SchemaRef,
    input_bytes: u64,
    entries: Vec<ExistingEntry>,
}

struct ExistingEntry {
    data_file: DataFile,
    snapshot_id: i64,
    sequence_number: i64,
    file_sequence_number: Option<i64>,
}

/// Orders partition tuples field by field, nulls first, as Spark orders
/// `data_file.partition`.
fn compare_partitions(a: &Struct, b: &Struct) -> Ordering {
    a.iter()
        .zip(b.iter())
        .map(|pair| match pair {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(Literal::Primitive(a)), Some(Literal::Primitive(b))) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Some(_), Some(_)) => Ordering::Equal,
        })
        .find(|ordering| ordering.is_ne())
        .unwrap_or_else(|| a.fields().len().cmp(&b.fields().len()))
}

/// Writes `entries` into one new manifest as `EXISTING` entries.
async fn write_manifest(
    table: &Table,
    schema: &SchemaRef,
    commit_uuid: &Uuid,
    n: u64,
    snapshot_id: i64,
    entries: impl Iterator<Item = ExistingEntry>,
) -> Result<ManifestFile> {
    let mut writer = new_manifest_writer(table, schema, commit_uuid, n, snapshot_id)?;
    for entry in entries {
        writer.add_existing_file(
            entry.data_file,
            entry.snapshot_id,
            entry.sequence_number,
            entry.file_sequence_number,
        )?;
    }
    writer.write_manifest_file().await
}

/// What a reader trusts of a live entry: identity, sequence numbers, snapshot
/// id, partition and size.
#[derive(Debug, PartialEq, Eq, Hash)]
struct EntryKey {
    file_path: String,
    content: i32,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    snapshot_id: Option<i64>,
    partition_spec_id: i32,
    partition: Struct,
    record_count: u64,
    file_size_in_bytes: u64,
    equality_ids: Option<Vec<i32>>,
}

/// A multiset of [`EntryKey`]s.
#[derive(Debug, Default, PartialEq)]
struct EntryCounts(HashMap<EntryKey, usize>);

impl EntryCounts {
    fn add(&mut self, entry: &ManifestEntry) {
        let file = entry.data_file();
        let key = EntryKey {
            file_path: file.file_path.clone(),
            content: file.content as i32,
            sequence_number: entry.sequence_number(),
            file_sequence_number: entry.file_sequence_number,
            snapshot_id: entry.snapshot_id(),
            partition_spec_id: file.partition_spec_id,
            partition: file.partition.clone(),
            record_count: file.record_count,
            file_size_in_bytes: file.file_size_in_bytes,
            equality_ids: file.equality_ids.clone(),
        };
        *self.0.entry(key).or_default() += 1;
    }
}

/// Reads back the manifest list and the created manifests, and fails unless
/// the list holds the created manifests followed by the unchanged kept ones,
/// and the created manifests hold exactly `expected`, all as `EXISTING`
/// entries, with counts and partition summaries that match those entries.
async fn verify_output(
    table: &Table,
    manifest_list_path: &str,
    snapshot_id: i64,
    sequence_number: i64,
    kept: &[ManifestFile],
    expected: EntryCounts,
) -> Result<()> {
    let metadata = table.metadata();
    let format_version = metadata.format_version();
    let bytes = table
        .file_io()
        .new_input(manifest_list_path)?
        .read()
        .await?;
    let list = ManifestList::parse_with_version(&bytes, format_version)?;
    let Some(created_count) = list.entries().len().checked_sub(kept.len()) else {
        return Err(output_mismatch("the manifest list lost manifests"));
    };
    let (created, carried) = list.entries().split_at(created_count);
    if carried != kept {
        return Err(output_mismatch("the kept manifests changed"));
    }

    let spec = metadata.default_partition_spec();
    let mut actual = EntryCounts::default();
    for manifest_file in created {
        let manifest = table.manifest_reader().read(manifest_file).await?;
        let entries = manifest.entries();
        if let Some(entry) = entries
            .iter()
            .find(|e| e.status() != ManifestStatus::Existing)
        {
            return Err(output_mismatch(format!(
                "{} has status {:?} in a new manifest",
                entry.file_path(),
                entry.status()
            )));
        }
        let rows = entries.iter().map(|e| e.data_file().record_count()).sum();
        let min_sequence_number = entries.iter().filter_map(|e| e.sequence_number()).min();
        // v1 manifest lists hold no sequence numbers.
        let (sequence_number, min_sequence_number) = match format_version {
            FormatVersion::V1 => (0, 0),
            _ => (
                sequence_number,
                min_sequence_number.unwrap_or(sequence_number),
            ),
        };
        let partition_type = spec.partition_type(manifest.metadata().schema())?;
        let implied = ManifestFile {
            manifest_path: manifest_file.manifest_path.clone(),
            manifest_length: manifest_file.manifest_length,
            partition_spec_id: spec.spec_id(),
            content: ManifestContentType::Data,
            sequence_number,
            min_sequence_number,
            added_snapshot_id: snapshot_id,
            added_files_count: Some(0),
            existing_files_count: Some(u32::try_from(entries.len())?),
            deleted_files_count: Some(0),
            added_rows_count: Some(0),
            existing_rows_count: Some(rows),
            deleted_rows_count: Some(0),
            partitions: Some(partition_summaries(
                &partition_type,
                entries.iter().map(|e| &e.data_file().partition),
            )?),
            key_metadata: manifest_file.key_metadata.clone(),
            first_row_id: None,
        };
        if *manifest_file != implied {
            return Err(output_mismatch(format!(
                "the manifest list entry of {} does not match its entries",
                manifest_file.manifest_path
            )));
        }
        for entry in entries {
            actual.add(entry);
        }
    }
    if actual != expected {
        return Err(output_mismatch(
            "the new manifests do not hold exactly the live entries of the replaced manifests",
        ));
    }
    Ok(())
}

fn output_mismatch(message: impl Into<String>) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        format!(
            "RewriteManifests output failed verification: {}",
            message.into()
        ),
    )
}

fn new_manifest_writer(
    table: &Table,
    schema: &SchemaRef,
    commit_uuid: &Uuid,
    n: u64,
    snapshot_id: i64,
) -> Result<ManifestWriter> {
    let metadata = table.metadata();
    let path = format!(
        "{}/metadata/{}-m{}.{}",
        metadata.location(),
        commit_uuid,
        n,
        DataFileFormat::Avro,
    );
    let output = table.file_io().new_output(path)?;
    let builder = ManifestWriterBuilder::new(
        output,
        Some(snapshot_id),
        schema.clone(),
        metadata.default_partition_spec().as_ref().clone(),
    );
    Ok(match metadata.format_version() {
        FormatVersion::V1 => builder.build_v1(),
        FormatVersion::V2 => builder.build_v2_data(),
        FormatVersion::V3 => builder.build_v3_data(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use futures::future::BoxFuture;

    use super::RewriteManifestsAction;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, FormatVersion, Literal,
        MAIN_BRANCH, ManifestContentType, ManifestEntry, ManifestFile, ManifestList,
        ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Operation, PrimitiveLiteral,
        SnapshotRetention, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::{
        append_files, make_v1_minimal_table_in_catalog, make_v2_minimal_table,
        make_v2_minimal_table_in_catalog, make_v3_minimal_table_in_catalog, set_main_retention,
        stage_delete_files,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};
    use crate::{Catalog, TableUpdate};

    fn data_file(name: &str, partition: i64, size: u64, records: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("test/{name}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(size)
            .record_count(records)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(partition))]))
            .build()
            .unwrap()
    }

    async fn append_one(catalog: &impl Catalog, table: Table, file: DataFile) -> Table {
        let tx = Transaction::new(&table);
        tx.fast_append()
            .add_data_files(vec![file])
            .apply(tx)
            .unwrap()
            .commit(catalog)
            .await
            .unwrap()
    }

    /// Manifests written before `p` was promoted from int to long are merged
    /// with the schema they were written with, apart from the newer ones.
    #[tokio::test]
    async fn test_rewrite_manifests_after_a_partition_source_promotion() {
        use uuid::Uuid;

        use crate::spec::{
            NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionField,
            UnboundPartitionSpec,
        };
        use crate::{NamespaceIdent, TableCommit, TableCreation};

        let catalog = new_memory_catalog().await;
        let namespace = NamespaceIdent::new(format!("ns1-{}", Uuid::new_v4()));
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        let schema = |p_type: PrimitiveType| {
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(2, "p", Type::Primitive(p_type)).into(),
                ])
                .build()
                .unwrap()
        };
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(
                UnboundPartitionField::builder()
                    .source_ids(vec![2])
                    .name("p")
                    .transform(Transform::Identity)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .build();
        let creation = TableCreation::builder()
            .name("events".to_string())
            .schema(schema(PrimitiveType::Int))
            .partition_spec(spec)
            .format_version(FormatVersion::V2)
            .build();
        let table = catalog.create_table(&namespace, creation).await.unwrap();
        let file = |name: &str, value: Literal| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("test/{name}.parquet"))
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(100)
                .record_count(1)
                .partition_spec_id(0)
                .partition(Struct::from_iter([Some(value)]))
                .build()
                .unwrap()
        };
        let table = append_one(&catalog, table, file("a", Literal::int(5))).await;
        let table = append_one(&catalog, table, file("b", Literal::int(5))).await;
        let commit = TableCommit::builder()
            .ident(table.identifier().clone())
            .requirements(vec![])
            .updates(vec![
                TableUpdate::AddSchema {
                    schema: schema(PrimitiveType::Long),
                },
                TableUpdate::SetCurrentSchema { schema_id: -1 },
            ])
            .build();
        let table = catalog.update_table(commit).await.unwrap();
        let table = append_one(&catalog, table, file("c", Literal::long(5))).await;

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Replace);
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        let mut manifests = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            let mut entries: Vec<(String, Struct)> = manifest
                .entries()
                .iter()
                .map(|entry| {
                    (
                        entry.file_path().to_string(),
                        entry.data_file().partition.clone(),
                    )
                })
                .collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            manifests.push((manifest.metadata().schema().schema_id(), entries));
        }
        manifests.sort_by_key(|(schema_id, _)| *schema_id);
        let int_5 = Struct::from_iter([Some(Literal::int(5))]);
        let long_5 = Struct::from_iter([Some(Literal::long(5))]);
        assert_eq!(manifests, vec![
            (0, vec![
                ("test/a.parquet".to_string(), int_5.clone()),
                ("test/b.parquet".to_string(), int_5),
            ]),
            (1, vec![("test/c.parquet".to_string(), long_5)]),
        ]);
    }

    #[tokio::test]
    async fn test_rewrite_keeps_main_branch_retention() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_one(&catalog, table, data_file("a", 1, 100, 1)).await;
        let table = append_one(&catalog, table, data_file("b", 1, 100, 1)).await;
        let retention =
            SnapshotRetention::branch(Some(9), Some(3_456_000_000), Some(2_592_000_000));
        let table = set_main_retention(&catalog, &table, retention.clone()).await;

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .operation,
            Operation::Replace
        );
        assert_eq!(table.metadata().refs()[MAIN_BRANCH].retention, retention);
    }

    #[tokio::test]
    async fn test_no_current_snapshot_errors() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests();
        match Arc::new(action).commit(&table).await {
            Ok(_) => panic!("expected error"),
            Err(e) => assert_eq!(e.kind(), crate::ErrorKind::PreconditionFailed),
        }
    }

    #[tokio::test]
    async fn test_single_small_manifest_is_noop() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_one(&catalog, table, data_file("a", 1, 100, 1)).await;
        let original_snapshot_id = table.metadata().current_snapshot_id();

        let tx = Transaction::new(&table);
        let action = tx.rewrite_manifests();
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert!(updates.is_empty());

        let table = tx
            .rewrite_manifests()
            .apply(Transaction::new(&table))
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        assert_eq!(
            table.metadata().current_snapshot_id(),
            original_snapshot_id,
            "no-op should not change the current snapshot"
        );
    }

    #[tokio::test]
    async fn test_multi_manifest_merge_preserves_sequence_numbers() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        let table = append_one(&catalog, table, data_file("a", 1, 1_000, 10)).await;
        let seq_a = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();
        let table = append_one(&catalog, table, data_file("b", 1, 2_000, 20)).await;
        let seq_b = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();
        let table = append_one(&catalog, table, data_file("c", 2, 3_000, 30)).await;
        let seq_c = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();
        assert!(seq_a < seq_b && seq_b < seq_c);

        let pre_manifest_count = table
            .manifest_list_reader(table.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap()
            .entries()
            .len();
        assert_eq!(pre_manifest_count, 3);

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Replace);

        let post_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        let total_entries: usize = {
            let mut n = 0;
            for m in post_list.entries() {
                let manifest = table.manifest_reader().read(m).await.unwrap();
                n += manifest.entries().len();
            }
            n
        };
        assert_eq!(total_entries, 3, "all entries preserved across rewrite");

        let mut seen_seqs: Vec<i64> = Vec::new();
        for m in post_list.entries() {
            let manifest = table.manifest_reader().read(m).await.unwrap();
            for entry in manifest.entries() {
                seen_seqs.push(entry.sequence_number().unwrap());
            }
        }
        seen_seqs.sort();
        assert_eq!(seen_seqs, vec![seq_a, seq_b, seq_c]);

        assert!(post_list.entries().len() < pre_manifest_count);

        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("total-records").unwrap(), "60");
        assert_eq!(summary.get("total-data-files").unwrap(), "3");
        assert_eq!(summary.get("entries-processed").unwrap(), "3");
        assert_eq!(summary.get("manifests-replaced").unwrap(), "3");
    }

    #[tokio::test]
    async fn test_target_size_from_table_property() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let mut t = table;
        for i in 0..6 {
            t = append_one(&catalog, t, data_file(&format!("f{i}"), 1, 10_000, 1)).await;
        }

        let tx = Transaction::new(&t);
        let t = tx
            .update_table_properties()
            .set(
                "commit.manifest.target-size-bytes".to_string(),
                "400".to_string(),
            )
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let tx = Transaction::new(&t);
        let t = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let post_list = t
            .manifest_list_reader(t.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap();
        assert!(post_list.entries().len() > 1);
    }

    #[tokio::test]
    async fn test_target_size_rolls_multiple_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let mut t = table;
        for i in 0..6 {
            t = append_one(&catalog, t, data_file(&format!("f{i}"), 1, 10_000, 1)).await;
        }

        let tx = Transaction::new(&t);
        let t = tx
            .rewrite_manifests()
            .set_target_size_bytes(400)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let post_list = t
            .manifest_list_reader(t.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap();
        assert!(post_list.entries().len() > 1);

        let mut total = 0;
        for m in post_list.entries() {
            let manifest = t.manifest_reader().read(m).await.unwrap();
            let n = manifest.entries().len();
            assert!(
                n <= 2,
                "each rolled manifest should hold at most ~2 entries when target is just above bytes_per_entry"
            );
            total += n;
        }
        assert_eq!(total, 6);
    }

    #[tokio::test]
    async fn test_oversized_single_manifest_is_split() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let files: Vec<_> = (0..6)
            .map(|i| data_file(&format!("f{i}"), 1, 10_000, 1))
            .collect();

        let tx = Transaction::new(&table);
        let table = tx
            .fast_append()
            .add_data_files(files)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let pre_list = table
            .manifest_list_reader(table.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap();
        assert_eq!(pre_list.entries().len(), 1);

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .set_target_size_bytes(400)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let snap = table.metadata().current_snapshot().unwrap();
        let post_list = table.manifest_list_reader(snap).load().await.unwrap();
        assert!(post_list.entries().len() > 1);
        assert_eq!(
            snap.summary()
                .additional_properties
                .get("manifests-replaced")
                .unwrap(),
            "1"
        );

        let mut total = 0;
        for m in post_list.entries() {
            total += table
                .manifest_reader()
                .read(m)
                .await
                .unwrap()
                .entries()
                .len();
        }
        assert_eq!(total, 6);
    }

    /// Row lineage is not maintained, so v3 tables are refused before any
    /// manifest is read.
    #[tokio::test]
    async fn test_v3_table_is_refused() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let table = append_one(&catalog, table, data_file("a", 1, 100, 30)).await;
        let table = append_one(&catalog, table, data_file("b", 1, 100, 17)).await;

        let err = Arc::new(Transaction::new(&table).rewrite_manifests())
            .commit(&table)
            .await
            .err()
            .unwrap();

        assert_eq!(err.kind(), crate::ErrorKind::FeatureUnsupported);
        assert_eq!(
            err.message(),
            "RewriteManifests does not support format v3 tables"
        );
    }

    #[tokio::test]
    async fn test_summary_and_replace_operation() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_one(&catalog, table, data_file("a", 1, 100, 10)).await;
        let table = append_one(&catalog, table, data_file("b", 2, 200, 20)).await;

        let pre_total_files_size = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties
            .get("total-files-size")
            .cloned();

        let tx = Transaction::new(&table);
        let mut commit = Arc::new(
            tx.rewrite_manifests()
                .set_snapshot_properties(HashMap::from([(
                    "trigger".to_string(),
                    "manual".to_string(),
                )])),
        )
        .commit(&table)
        .await
        .unwrap();
        let updates = commit.take_updates();
        let TableUpdate::AddSnapshot { snapshot: snap } = &updates[0] else {
            unreachable!()
        };
        let s = &snap.summary().additional_properties;
        assert_eq!(snap.summary().operation, Operation::Replace);
        assert_eq!(s.get("trigger").unwrap(), "manual");
        assert_eq!(s.get("entries-processed").unwrap(), "2");
        assert_eq!(s.get("manifests-replaced").unwrap(), "2");
        assert_eq!(s.get("manifests-kept").unwrap(), "0");
        assert_eq!(s.get("manifests-created").unwrap(), "1");
        assert_eq!(s.get("total-records").unwrap(), "30");
        assert_eq!(s.get("total-files-size").cloned(), pre_total_files_size);
    }

    #[tokio::test]
    async fn test_idempotent_after_consolidation() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let mut t = table;
        for i in 0..3 {
            t = append_one(&catalog, t, data_file(&format!("f{i}"), 1, 100, 1)).await;
        }

        let tx = Transaction::new(&t);
        let t = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let post_count = t
            .manifest_list_reader(t.metadata().current_snapshot().unwrap())
            .load()
            .await
            .unwrap()
            .entries()
            .len();
        assert_eq!(post_count, 1, "single partition consolidates to 1 manifest");

        let mut commit2 = Arc::new(Transaction::new(&t).rewrite_manifests())
            .commit(&t)
            .await
            .unwrap();
        assert!(
            commit2.take_updates().is_empty(),
            "second rewrite must no-op when input already fits in one manifest"
        );
    }

    #[tokio::test]
    async fn test_partitions_share_a_manifest_and_catalog_commit() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let table = append_one(&catalog, table, data_file("a", 1, 100, 1)).await;
        let table = append_one(&catalog, table, data_file("b", 1, 100, 1)).await;
        let table = append_one(&catalog, table, data_file("c", 2, 100, 1)).await;
        let table = append_one(&catalog, table, data_file("d", 2, 100, 1)).await;

        let pre_id = table.metadata().current_snapshot_id().unwrap();
        let pre_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        let snap = table.metadata().current_snapshot().unwrap();
        assert_eq!(snap.parent_snapshot_id(), Some(pre_id));
        assert_eq!(snap.sequence_number(), pre_seq + 1);
        assert_ne!(snap.snapshot_id(), pre_id);

        assert_eq!(data_manifest_partitions(&table).await, vec![vec![
            1, 1, 2, 2
        ]]);
    }

    async fn rewrite(catalog: &impl Catalog, table: &Table, target_size_bytes: u64) -> Table {
        let tx = Transaction::new(table);
        tx.rewrite_manifests()
            .set_target_size_bytes(target_size_bytes)
            .apply(tx)
            .unwrap()
            .commit(catalog)
            .await
            .unwrap()
    }

    fn summary_value(table: &Table, key: &str) -> String {
        table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties[key]
            .clone()
    }

    async fn data_manifests(table: &Table) -> Vec<ManifestFile> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let list = table.manifest_list_reader(snapshot).load().await.unwrap();
        list.entries()
            .iter()
            .filter(|m| m.content == ManifestContentType::Data)
            .cloned()
            .collect()
    }

    async fn data_manifest_bytes(table: &Table) -> u64 {
        data_manifests(table)
            .await
            .iter()
            .map(|m| u64::try_from(m.manifest_length).unwrap())
            .sum()
    }

    fn partition_value(file: &DataFile) -> i64 {
        match &file.partition[0] {
            Some(Literal::Primitive(PrimitiveLiteral::Long(v))) => *v,
            other => panic!("unexpected partition value {other:?}"),
        }
    }

    /// The partition value of each live entry, per data manifest in manifest-list order.
    async fn data_manifest_partitions(table: &Table) -> Vec<Vec<i64>> {
        let mut out = Vec::new();
        for m in data_manifests(table).await {
            let manifest = table.manifest_reader().read(&m).await.unwrap();
            out.push(
                manifest
                    .entries()
                    .iter()
                    .filter(|e| e.is_alive())
                    .map(|e| partition_value(e.data_file()))
                    .collect(),
            );
        }
        out
    }

    /// 30 appends, append `i` writing partitions `i` and `i + 30`.
    async fn table_with_60_partitions(catalog: &impl Catalog) -> Table {
        let mut table = make_v2_minimal_table_in_catalog(catalog).await;
        for i in 0..30 {
            table = append_files(catalog, &table, vec![
                data_file(&format!("a{i}"), i, 100, 1),
                data_file(&format!("b{i}"), i + 30, 100, 1),
            ])
            .await;
        }
        table
    }

    /// Entries of many partitions share manifests: the rewrite writes
    /// `ceil(input bytes / target)` manifests, each holding a contiguous run of
    /// partitions in order, whatever order the appends wrote them in.
    #[tokio::test]
    async fn test_many_partitions_pack_into_target_count() {
        let catalog = new_memory_catalog().await;
        let table = table_with_60_partitions(&catalog).await;
        assert_eq!(data_manifests(&table).await.len(), 30);
        let target = data_manifest_bytes(&table).await.div_ceil(4);

        let table = rewrite(&catalog, &table, target).await;

        assert_eq!(summary_value(&table, "manifests-replaced"), "30");
        assert_eq!(summary_value(&table, "manifests-created"), "4");
        assert_eq!(data_manifest_partitions(&table).await, vec![
            (0..15).collect::<Vec<_>>(),
            (15..30).collect(),
            (30..45).collect(),
            (45..60).collect(),
        ]);
    }

    /// A daily table over two years fits one manifest at the default target,
    /// and rewriting it again is a no-op.
    #[tokio::test]
    async fn test_second_rewrite_of_packed_table_is_noop() {
        let catalog = new_memory_catalog().await;
        let mut table = make_v2_minimal_table_in_catalog(&catalog).await;
        for batch in 0..24 {
            let files = (batch * 30..(batch + 1) * 30)
                .map(|day| data_file(&format!("d{day}"), day, 100, 1))
                .collect();
            table = append_files(&catalog, &table, files).await;
        }

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        assert_eq!(summary_value(&table, "manifests-replaced"), "24");
        assert_eq!(data_manifest_partitions(&table).await, vec![
            (0..720).collect::<Vec<_>>()
        ]);

        let mut commit = Arc::new(Transaction::new(&table).rewrite_manifests())
            .commit(&table)
            .await
            .unwrap();
        assert!(commit.take_updates().is_empty());
    }

    /// Rewriting packed manifests again at the same target never writes more
    /// manifests than the first rewrite did.
    #[tokio::test]
    async fn test_second_rewrite_does_not_add_manifests() {
        let catalog = new_memory_catalog().await;
        let table = table_with_60_partitions(&catalog).await;
        let target = data_manifest_bytes(&table).await.div_ceil(4);
        let table = rewrite(&catalog, &table, target).await;
        assert_eq!(data_manifests(&table).await.len(), 4);
        let expected = data_manifest_bytes(&table).await.div_ceil(target);
        assert!(expected <= 4);

        let table = rewrite(&catalog, &table, target).await;

        assert_eq!(summary_value(&table, "manifests-replaced"), "4");
        assert_eq!(
            summary_value(&table, "manifests-created"),
            expected.to_string()
        );
        let partitions: Vec<i64> = data_manifest_partitions(&table)
            .await
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(partitions, (0..60).collect::<Vec<_>>());
    }

    /// With `rewrite_if`, only the matching manifests are rewritten: small
    /// manifests of new appends pack together, and full ones are kept.
    #[tokio::test]
    async fn test_rewrite_if_rewrites_only_matching_manifests() {
        let catalog = new_memory_catalog().await;
        let table = table_with_60_partitions(&catalog).await;
        let target = data_manifest_bytes(&table).await.div_ceil(4);
        let mut table = rewrite(&catalog, &table, target).await;
        let packed = data_manifests(&table).await;
        assert_eq!(packed.len(), 4);
        let smallest_packed = packed.iter().map(|m| m.manifest_length).min().unwrap();
        for i in 60..65 {
            table = append_one(&catalog, table, data_file(&format!("n{i}"), i, 100, 1)).await;
        }

        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_manifests()
            .set_target_size_bytes(target)
            .rewrite_if(move |m| m.manifest_length < smallest_packed)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();

        assert_eq!(summary_value(&table, "manifests-replaced"), "5");
        assert_eq!(summary_value(&table, "manifests-created"), "1");
        assert_eq!(summary_value(&table, "manifests-kept"), "4");
        let mut partitions = data_manifest_partitions(&table).await;
        partitions.sort();
        assert_eq!(partitions, vec![
            (0..15).collect::<Vec<_>>(),
            (15..30).collect(),
            (30..45).collect(),
            (45..60).collect(),
            (60..65).collect(),
        ]);
    }

    /// Every live data entry keeps its snapshot id, data and file sequence
    /// numbers, partition and record count, and becomes `EXISTING`; delete
    /// manifests are kept as they are.
    #[tokio::test]
    async fn test_packing_preserves_every_entry() {
        let catalog = new_memory_catalog().await;
        let mut table = make_v2_minimal_table_in_catalog(&catalog).await;
        for i in 0..10 {
            table = append_files(&catalog, &table, vec![
                data_file(&format!("a{i}"), 9 - i, 100, 1 + i as u64),
                data_file(&format!("b{i}"), i, 100, 20 + i as u64),
            ])
            .await;
        }
        // A compaction whose output keeps the data sequence number of the
        // snapshot it started from, below its own file sequence number.
        let replaced: Vec<DataFile> = {
            let mut files = Vec::new();
            for m in data_manifests(&table).await {
                for e in table.manifest_reader().read(&m).await.unwrap().entries() {
                    if ["test/a0.parquet", "test/b9.parquet"].contains(&e.file_path()) {
                        files.push(e.data_file().clone());
                    }
                }
            }
            files
        };
        assert_eq!(replaced.len(), 2);
        let start_seq = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();
        let tx = Transaction::new(&table);
        let mut action = tx
            .rewrite_files()
            .data_sequence_number(start_seq)
            .add_file(data_file("c", 9, 100, 30));
        for file in replaced {
            action = action.delete_file(file);
        }
        table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();
        let mut deletes = data_file("c-deletes", 9, 100, 2);
        deletes.content = DataContentType::PositionDeletes;
        table = stage_delete_files(&catalog, &table, vec![deletes]).await;

        async fn live_entries(
            table: &Table,
        ) -> Vec<(String, i64, u64, Option<i64>, Option<i64>, Option<i64>)> {
            let mut out = Vec::new();
            for m in data_manifests(table).await {
                for e in table.manifest_reader().read(&m).await.unwrap().entries() {
                    if e.is_alive() {
                        out.push((
                            e.file_path().to_string(),
                            partition_value(e.data_file()),
                            e.data_file().record_count(),
                            e.snapshot_id(),
                            e.sequence_number(),
                            e.file_sequence_number,
                        ));
                    }
                }
            }
            out.sort();
            out
        }
        async fn delete_manifest_paths(table: &Table) -> Vec<String> {
            let snapshot = table.metadata().current_snapshot().unwrap();
            let list = table.manifest_list_reader(snapshot).load().await.unwrap();
            list.entries()
                .iter()
                .filter(|m| m.content == ManifestContentType::Deletes)
                .map(|m| m.manifest_path.clone())
                .collect()
        }
        let before = live_entries(&table).await;
        assert_eq!(before.len(), 19);
        let compacted = before.iter().find(|e| e.0 == "test/c.parquet").unwrap();
        assert_eq!(compacted.4, Some(start_seq));
        assert_eq!(compacted.5, Some(start_seq + 1));
        let delete_manifests = delete_manifest_paths(&table).await;
        assert_eq!(delete_manifests.len(), 1);
        let target = data_manifest_bytes(&table).await.div_ceil(2);

        let table = rewrite(&catalog, &table, target).await;

        assert_eq!(summary_value(&table, "manifests-created"), "2");
        assert_eq!(summary_value(&table, "manifests-kept"), "1");
        assert_eq!(live_entries(&table).await, before);
        assert_eq!(delete_manifest_paths(&table).await, delete_manifests);
        for m in data_manifests(&table).await {
            for e in table.manifest_reader().read(&m).await.unwrap().entries() {
                assert_eq!(e.status(), ManifestStatus::Existing);
            }
        }
    }

    /// The output of a rewrite of a v1 table passes the verification and
    /// keeps every entry.
    #[tokio::test]
    async fn test_v1_table_keeps_every_entry() {
        let catalog = new_memory_catalog().await;
        let mut table = make_v1_minimal_table_in_catalog(&catalog).await;
        for i in 0..6 {
            table = append_files(&catalog, &table, vec![
                data_file(&format!("a{i}"), i, 100, 1 + i as u64),
                data_file(&format!("b{i}"), i + 6, 100, 10 + i as u64),
            ])
            .await;
        }
        let before = data_manifest_partitions(&table).await;
        assert_eq!(before.len(), 6);
        let target = data_manifest_bytes(&table).await.div_ceil(2);

        let table = rewrite(&catalog, &table, target).await;

        assert_eq!(summary_value(&table, "manifests-created"), "2");
        assert_eq!(data_manifest_partitions(&table).await, vec![
            (0..6).collect::<Vec<_>>(),
            (6..12).collect(),
        ]);
    }

    pub(super) type OutputHook = Arc<dyn Fn(Table, String) -> BoxFuture<'static, ()> + Send + Sync>;

    impl RewriteManifestsAction {
        /// Runs `hook` on the written output before it is verified.
        fn corrupt_output(mut self, hook: OutputHook) -> Self {
            self.corrupt_output = Some(hook);
            self
        }
    }

    /// 4 appends of partitions `i` and `i + 10` and one delete manifest, which
    /// a rewrite keeps.
    async fn table_with_a_delete_manifest(catalog: &impl Catalog) -> Table {
        let mut table = make_v2_minimal_table_in_catalog(catalog).await;
        for i in 0..4 {
            table = append_files(catalog, &table, vec![
                data_file(&format!("a{i}"), i, 100, 1 + i as u64),
                data_file(&format!("b{i}"), i + 10, 100, 10 + i as u64),
            ])
            .await;
        }
        let mut deletes = data_file("deletes", 0, 100, 1);
        deletes.content = DataContentType::PositionDeletes;
        stage_delete_files(catalog, &table, vec![deletes]).await
    }

    /// Rewrites the first manifest of the written list, which the action
    /// created, with `edit_entries` applied, and then the list with
    /// `edit_list` applied. Counts and partition summaries of the rewritten
    /// manifest match its edited entries.
    async fn rewrite_created_manifest(
        table: Table,
        list_path: String,
        edit_entries: fn(&mut Vec<ManifestEntry>),
        edit_list: fn(&mut Vec<ManifestFile>),
    ) {
        let file_io = table.file_io();
        let metadata = table.metadata();
        let bytes = file_io.new_input(&list_path).unwrap().read().await.unwrap();
        let mut manifests: Vec<ManifestFile> =
            ManifestList::parse_with_version(&bytes, FormatVersion::V2)
                .unwrap()
                .consume_entries()
                .into_iter()
                .collect();
        let created = manifests[0].clone();
        let mut entries: Vec<ManifestEntry> = table
            .manifest_reader()
            .read(&created)
            .await
            .unwrap()
            .entries()
            .iter()
            .map(|e| e.as_ref().clone())
            .collect();
        edit_entries(&mut entries);

        let output = file_io
            .new_output(format!(
                "{}/metadata/corrupted-m0.avro",
                metadata.location()
            ))
            .unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output,
            Some(created.added_snapshot_id),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        for e in entries {
            let sequence_number = e.sequence_number.unwrap();
            match e.status {
                ManifestStatus::Existing => writer.add_existing_file(
                    e.data_file,
                    e.snapshot_id.unwrap(),
                    sequence_number,
                    e.file_sequence_number,
                ),
                ManifestStatus::Deleted => {
                    writer.add_delete_file(e.data_file, sequence_number, e.file_sequence_number)
                }
                ManifestStatus::Added => writer.add_file(e.data_file, sequence_number),
            }
            .unwrap();
        }
        let mut rewritten = writer.write_manifest_file().await.unwrap();
        rewritten.sequence_number = created.sequence_number;
        manifests[0] = rewritten;
        edit_list(&mut manifests);

        let mut list_writer = ManifestListWriter::v2(
            file_io
                .new_output(&list_path)
                .unwrap()
                .writer()
                .await
                .unwrap(),
            created.added_snapshot_id,
            metadata.current_snapshot_id(),
            created.sequence_number,
        );
        list_writer.add_manifests(manifests.into_iter()).unwrap();
        list_writer.close().await.unwrap();
    }

    /// Rewrites [`table_with_a_delete_manifest`] into one manifest, edits the
    /// output with [`rewrite_created_manifest`], and returns the table and the
    /// result of the action.
    async fn rewrite_with_edited_output(
        edit_entries: fn(&mut Vec<ManifestEntry>),
        edit_list: fn(&mut Vec<ManifestFile>),
    ) -> (Table, crate::Result<Vec<TableUpdate>>) {
        let catalog = new_memory_catalog().await;
        let table = table_with_a_delete_manifest(&catalog).await;
        let hook: OutputHook = Arc::new(move |table, list_path| {
            Box::pin(rewrite_created_manifest(
                table,
                list_path,
                edit_entries,
                edit_list,
            ))
        });
        let action = Transaction::new(&table)
            .rewrite_manifests()
            .corrupt_output(hook);
        let result = Arc::new(action)
            .commit(&table)
            .await
            .map(|mut commit| commit.take_updates());
        (table, result)
    }

    async fn verification_error(
        edit_entries: fn(&mut Vec<ManifestEntry>),
        edit_list: fn(&mut Vec<ManifestFile>),
    ) -> (Table, String) {
        let (table, result) = rewrite_with_edited_output(edit_entries, edit_list).await;
        let err = result.err().unwrap();
        assert_eq!(err.kind(), crate::ErrorKind::Unexpected);
        let message = err
            .message()
            .strip_prefix("RewriteManifests output failed verification: ")
            .unwrap()
            .to_string();
        (table, message)
    }

    const ENTRIES_DIFFER: &str =
        "the new manifests do not hold exactly the live entries of the replaced manifests";

    fn manifest_list_entry_differs(table: &Table) -> String {
        format!(
            "the manifest list entry of {}/metadata/corrupted-m0.avro does not match its entries",
            table.metadata().location()
        )
    }

    /// The output rewritten without edits passes, so each failure below comes
    /// from its edit alone.
    #[tokio::test]
    async fn test_verification_passes_an_unedited_output() {
        let (_, result) = rewrite_with_edited_output(|_| {}, |_| {}).await;

        assert_eq!(result.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_verification_fails_on_a_dropped_entry() {
        let (_, message) = verification_error(
            |entries| {
                entries.pop();
            },
            |_| {},
        )
        .await;

        assert_eq!(message, ENTRIES_DIFFER);
    }

    #[tokio::test]
    async fn test_verification_fails_on_a_duplicated_entry() {
        let (_, message) =
            verification_error(|entries| entries.push(entries[0].clone()), |_| {}).await;

        assert_eq!(message, ENTRIES_DIFFER);
    }

    #[tokio::test]
    async fn test_verification_fails_on_a_changed_sequence_number() {
        let (_, message) = verification_error(
            |entries| entries[0].sequence_number = entries[0].sequence_number.map(|n| n + 1),
            |_| {},
        )
        .await;

        assert_eq!(message, ENTRIES_DIFFER);
    }

    #[tokio::test]
    async fn test_verification_fails_on_a_changed_partition() {
        let (_, message) = verification_error(
            |entries| entries[0].data_file.partition = Struct::from_iter([Some(Literal::long(5))]),
            |_| {},
        )
        .await;

        assert_eq!(message, ENTRIES_DIFFER);
    }

    #[tokio::test]
    async fn test_verification_fails_on_a_deleted_entry() {
        let (_, message) = verification_error(
            |entries| entries[0].status = ManifestStatus::Deleted,
            |_| {},
        )
        .await;

        assert_eq!(
            message,
            "test/a0.parquet has status Deleted in a new manifest"
        );
    }

    #[tokio::test]
    async fn test_verification_fails_on_a_changed_file_count() {
        let (table, message) = verification_error(
            |_| {},
            |list| list[0].existing_files_count = list[0].existing_files_count.map(|n| n + 1),
        )
        .await;

        assert_eq!(message, manifest_list_entry_differs(&table));
    }

    #[tokio::test]
    async fn test_verification_fails_on_a_changed_partition_summary() {
        let (table, message) = verification_error(
            |_| {},
            |list| {
                list[0].partitions.as_mut().unwrap()[0].upper_bound =
                    Some(Datum::long(5).to_bytes().unwrap());
            },
        )
        .await;

        assert_eq!(message, manifest_list_entry_differs(&table));
    }

    #[tokio::test]
    async fn test_verification_fails_on_a_lost_kept_manifest() {
        let (_, message) = verification_error(
            |_| {},
            |list| {
                list.pop();
            },
        )
        .await;

        assert_eq!(message, "the kept manifests changed");
    }
}
