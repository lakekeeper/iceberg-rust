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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use futures::stream::{self, StreamExt};
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, Literal, MAIN_BRANCH, ManifestContentType,
    ManifestEntry, ManifestFile, ManifestList, ManifestListWriter, ManifestStatus, ManifestWriter,
    ManifestWriterBuilder, Operation, PrimitiveLiteral, SchemaId, SchemaRef, Snapshot,
    SnapshotReference, Struct, Summary, TableProperties, UNASSIGNED_SEQUENCE_NUMBER,
    partition_summaries,
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

    /// Re-applies `planned`, a rewrite snapshot this action planned on `base`, on the current
    /// snapshot of `table`, as Java's `BaseRewriteManifests` does when it retries: the new
    /// manifests are kept, not read again, and only a new manifest list is written and verified.
    ///
    /// Returns `None` when the rewrite does not apply to `table` as planned: a replaced
    /// manifest is no longer in its current snapshot, its format version, default partition
    /// spec or current schema changed, or it already holds the planned snapshot.
    ///
    /// # Errors
    /// Fails when `planned` is not a rewrite (`replace`) snapshot that follows `base`'s current
    /// snapshot, when `base` is a format v3 table, when a manifest list cannot be read or written,
    /// or when the new list fails verification.
    pub async fn rebase(
        base: &Table,
        planned: &Snapshot,
        table: &Table,
    ) -> Result<Option<ActionCommit>> {
        let (base_metadata, metadata) = (base.metadata(), table.metadata());
        let Some(base_snapshot) = base_metadata.current_snapshot() else {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "RewriteManifests rebase requires the base to have a current snapshot",
            ));
        };
        if base_metadata.format_version() >= FormatVersion::V3 {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "RewriteManifests does not support format {} tables",
                    base_metadata.format_version()
                ),
            ));
        }
        if planned.parent_snapshot_id() != Some(base_snapshot.snapshot_id())
            || planned.summary().operation != Operation::Replace
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "RewriteManifests rebase requires a rewrite snapshot planned on the base",
            ));
        }
        let Some(head) = metadata.current_snapshot() else {
            return Ok(None);
        };
        if metadata.format_version() != base_metadata.format_version()
            || metadata.default_partition_spec_id() != base_metadata.default_partition_spec_id()
            || metadata.current_schema_id() != base_metadata.current_schema_id()
            || metadata.snapshot_by_id(planned.snapshot_id()).is_some()
        {
            return Ok(None);
        }

        let snapshot_id = planned.snapshot_id();
        let planned_list = base
            .manifest_list_reader(&Arc::new(planned.clone()))
            .load()
            .await?;
        let (created, carried): (Vec<&ManifestFile>, Vec<&ManifestFile>) = planned_list
            .entries()
            .iter()
            .partition(|m| m.added_snapshot_id == snapshot_id);
        let carried: HashSet<&str> = carried.iter().map(|m| m.manifest_path.as_str()).collect();
        let base_list = base.manifest_list_reader(base_snapshot).load().await?;
        let replaced: HashSet<&str> = base_list
            .entries()
            .iter()
            .map(|m| m.manifest_path.as_str())
            .filter(|path| !carried.contains(path))
            .collect();
        let head_list = table.manifest_list_reader(head).load().await?;
        let (gone, kept): (Vec<&ManifestFile>, Vec<&ManifestFile>) = head_list
            .entries()
            .iter()
            .partition(|m| replaced.contains(m.manifest_path.as_str()));
        // Every replaced path must still be listed: manifest files never change, so it holds the
        // same entries. Paths, not entries, as a list may name a manifest twice.
        let gone: HashSet<&str> = gone.iter().map(|m| m.manifest_path.as_str()).collect();
        if gone.len() != replaced.len() {
            return Ok(None);
        }
        let kept: Vec<ManifestFile> = kept.into_iter().cloned().collect();

        let manifest_list_path = write_manifest_list(
            table,
            snapshot_id,
            &Uuid::now_v7(),
            created
                .iter()
                .map(|m| ManifestFile {
                    sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                    ..(*m).clone()
                })
                .chain(kept.iter().cloned()),
        )
        .await?;
        #[cfg(test)]
        if let Some(hook) = tests::REBASE_OUTPUT.with(|hook| hook.borrow().clone()) {
            hook(table.clone(), manifest_list_path.clone()).await;
        }
        // The new manifests were verified against the replaced ones when planned and their files
        // do not change: the list must hold them as planned but for the sequence number.
        let expected: Vec<ManifestFile> = created
            .into_iter()
            .map(|m| ManifestFile {
                sequence_number: metadata.next_sequence_number(),
                ..m.clone()
            })
            .collect();
        let verified = match read_list_layout(table, &manifest_list_path, &kept).await {
            Ok(created) if created == expected => Ok(()),
            Ok(_) => Err(output_mismatch("the new manifests changed")),
            Err(error) => Err(error),
        };
        if let Err(error) = verified {
            // No catalog saw the list.
            delete_unsent(table, vec![manifest_list_path]).await;
            return Err(error);
        }

        let mut properties = planned.summary().additional_properties.clone();
        properties.retain(|key, _| !SUMMARY_TOTALS.contains(&key.as_str()));
        properties.insert("manifests-kept".to_string(), kept.len().to_string());
        Ok(Some(rewrite_commit(
            table,
            snapshot_id,
            manifest_list_path,
            properties,
        )))
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

        // Manifests are consumed as they arrive, so at most `MANIFEST_READ_CONCURRENCY`
        // parsed manifests are held besides the live entries taken from them.
        let mut loaded = stream::iter(to_rewrite)
            .map(|m| {
                let reader = table.manifest_reader();
                async move {
                    let length = u64::try_from(m.manifest_length).unwrap_or(0);
                    reader.read(&m).await.map(|manifest| (length, manifest))
                }
            })
            .buffer_unordered(MANIFEST_READ_CONCURRENCY);

        // Each new manifest keeps the schema its entries were written with, so
        // their partition values keep fitting their types after a promotion or
        // a dropped source column.
        let mut groups: BTreeMap<SchemaId, SchemaGroup> = BTreeMap::new();
        let mut entries_processed: u64 = 0;
        let mut expected_entries = EntryDigest::default();

        while let Some((length, manifest)) = loaded.try_next().await? {
            let schema = manifest.metadata().schema().clone();
            let group = groups
                .entry(schema.schema_id())
                .or_insert_with(|| SchemaGroup {
                    schema: schema.clone(),
                    input_bytes: 0,
                    entries: Vec::new(),
                });
            group.input_bytes = group.input_bytes.saturating_add(length);
            let (entries, _) = manifest.into_parts();
            for entry in entries {
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
                expected_entries.add(&entry);
                let entry = Arc::unwrap_or_clone(entry);
                group.entries.push(ExistingEntry {
                    data_file: entry.data_file,
                    snapshot_id: entry_snapshot_id,
                    sequence_number,
                    file_sequence_number: entry.file_sequence_number,
                });
                entries_processed += 1;
            }
        }

        // Files of an output that fails are deleted: no catalog saw them.
        let mut written_paths: Vec<String> = Vec::new();
        let output = async {
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
                let manifest_count =
                    usize::try_from(input_bytes.div_ceil(target_size_bytes.max(1)))
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
                    written_paths.push(written.manifest_path.clone());
                    counter += 1;
                    new_manifests.push(written);
                }
            }

            let manifest_list_path = write_manifest_list(
                table,
                snapshot_id,
                &commit_uuid,
                new_manifests.iter().cloned().chain(kept.iter().cloned()),
            )
            .await?;
            written_paths.push(manifest_list_path.clone());

            #[cfg(test)]
            if let Some(corrupt_output) = &self.corrupt_output {
                corrupt_output(table.clone(), manifest_list_path.clone()).await;
            }
            verify_output(
                table,
                &manifest_list_path,
                snapshot_id,
                metadata.next_sequence_number(),
                &kept,
                expected_entries,
            )
            .await?;
            Ok::<_, Error>((manifest_list_path, new_manifests.len()))
        }
        .await;
        let (manifest_list_path, manifests_created) = match output {
            Ok(output) => output,
            Err(error) => {
                delete_unsent(table, written_paths).await;
                return Err(error);
            }
        };
        let manifests_kept = kept.len();

        let mut properties: HashMap<String, String> = self.snapshot_properties.clone();
        properties.insert(
            "manifests-created".to_string(),
            manifests_created.to_string(),
        );
        properties.insert(
            "manifests-replaced".to_string(),
            manifests_replaced.to_string(),
        );
        properties.insert("manifests-kept".to_string(), manifests_kept.to_string());
        properties.insert(
            "entries-processed".to_string(),
            entries_processed.to_string(),
        );
        Ok(rewrite_commit(
            table,
            snapshot_id,
            manifest_list_path,
            properties,
        ))
    }
}

/// Totals of the parent's summary a rewrite snapshot repeats, as it changes no file.
const SUMMARY_TOTALS: [&str; 6] = [
    "total-data-files",
    "total-delete-files",
    "total-records",
    "total-files-size",
    "total-position-deletes",
    "total-equality-deletes",
];

/// Deletes files of an output that failed before it was returned, so no catalog saw them.
async fn delete_unsent(table: &Table, paths: Vec<String>) {
    for path in &paths {
        if let Err(error) = table.file_io().delete(path).await {
            tracing::warn!(%error, path, "RewriteManifests failed to delete an unused file");
        }
    }
}

/// Writes the manifest list of a rewrite snapshot that follows `table`'s current snapshot.
async fn write_manifest_list(
    table: &Table,
    snapshot_id: i64,
    commit_uuid: &Uuid,
    manifests: impl Iterator<Item = ManifestFile>,
) -> Result<String> {
    let metadata = table.metadata();
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
    let mut list_writer = match metadata.format_version() {
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
    list_writer.add_manifests(manifests)?;
    list_writer.close().await?;
    Ok(manifest_list_path)
}

/// The commit of a rewrite snapshot with `manifest_list_path` on `table`'s current snapshot:
/// a `Replace` with `properties` and the current snapshot's totals in its summary.
fn rewrite_commit(
    table: &Table,
    snapshot_id: i64,
    manifest_list_path: String,
    mut properties: HashMap<String, String>,
) -> ActionCommit {
    let metadata = table.metadata();
    if let Some(current_snapshot) = metadata.current_snapshot() {
        for key in SUMMARY_TOTALS {
            if let Some(value) = current_snapshot.summary().additional_properties.get(key) {
                properties.insert(key.to_string(), value.clone());
            }
        }
    }
    let summary = Summary {
        operation: Operation::Replace,
        additional_properties: properties,
    };

    let commit_ts = chrono::Utc::now().timestamp_millis();
    let new_snapshot = Snapshot::builder()
        .with_manifest_list(manifest_list_path)
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(metadata.current_snapshot_id())
        .with_sequence_number(metadata.next_sequence_number())
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
    ActionCommit::new(updates, requirements)
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

/// Live entries read in parallel at most this many manifests at a time.
const MANIFEST_READ_CONCURRENCY: usize = 4;

/// A multiset of live entries as a reader sees them: the whole data file,
/// including its column metrics, with sequence numbers and snapshot id.
/// It keeps only the number of entries and the wrapping sum of a 128-bit
/// hash of each, so its size does not depend on the entries.
#[derive(Debug, Default, PartialEq, Eq)]
struct EntryDigest {
    count: u64,
    sum: u128,
}

impl EntryDigest {
    fn add(&mut self, entry: &ManifestEntry) {
        self.count += 1;
        self.sum = self.sum.wrapping_add(entry_hash(entry));
    }
}

/// A 128-bit hash of everything [`EntryDigest`] compares, from two SipHash
/// runs told apart by a leading byte.
fn entry_hash(entry: &ManifestEntry) -> u128 {
    let half = |seed: u8| {
        let mut hasher = std::hash::DefaultHasher::new();
        seed.hash(&mut hasher);
        hash_entry(entry, &mut hasher);
        hasher.finish()
    };
    (u128::from(half(0)) << 64) | u128::from(half(1))
}

/// Feeds a live entry into `hasher`: its sequence numbers, snapshot id and every
/// field of its data file. Each value goes in with its variant or length, maps in
/// key order, and floats as their bits with one pattern for every NaN, so two
/// entries feed the same input exactly when they are equal.
fn hash_entry(entry: &ManifestEntry, hasher: &mut impl Hasher) {
    // No `..`: a new field must be decided on here and in `hash_data_file`.
    let ManifestEntry {
        // Every new manifest holds `EXISTING` entries; `verify_output` checks that.
        status: _,
        snapshot_id,
        sequence_number,
        file_sequence_number,
        data_file,
    } = entry;
    snapshot_id.hash(hasher);
    sequence_number.hash(hasher);
    file_sequence_number.hash(hasher);
    hash_data_file(data_file, hasher);
}

fn hash_data_file(data_file: &DataFile, hasher: &mut impl Hasher) {
    let DataFile {
        content,
        file_path,
        file_format,
        partition,
        record_count,
        file_size_in_bytes,
        column_sizes,
        value_counts,
        null_value_counts,
        nan_value_counts,
        lower_bounds,
        upper_bounds,
        key_metadata,
        split_offsets,
        equality_ids,
        sort_order_id,
        first_row_id,
        partition_spec_id,
        referenced_data_file,
        content_offset,
        content_size_in_bytes,
    } = data_file;
    std::mem::discriminant(content).hash(hasher);
    file_path.hash(hasher);
    std::mem::discriminant(file_format).hash(hasher);
    hash_struct(partition, hasher);
    record_count.hash(hasher);
    file_size_in_bytes.hash(hasher);
    for counts in [
        column_sizes,
        value_counts,
        null_value_counts,
        nan_value_counts,
    ] {
        hash_by_key(counts, hasher, |count, hasher| count.hash(hasher));
    }
    for bounds in [lower_bounds, upper_bounds] {
        hash_by_key(bounds, hasher, |bound, hasher| {
            bound.data_type().hash(hasher);
            hash_primitive(bound.literal(), hasher);
        });
    }
    key_metadata.hash(hasher);
    split_offsets.hash(hasher);
    equality_ids.hash(hasher);
    sort_order_id.hash(hasher);
    first_row_id.hash(hasher);
    partition_spec_id.hash(hasher);
    referenced_data_file.hash(hasher);
    content_offset.hash(hasher);
    content_size_in_bytes.hash(hasher);
}

fn hash_by_key<V, H: Hasher>(map: &HashMap<i32, V>, hasher: &mut H, value: impl Fn(&V, &mut H)) {
    let mut pairs: Vec<_> = map.iter().collect();
    pairs.sort_unstable_by_key(|(key, _)| **key);
    pairs.len().hash(hasher);
    for (key, v) in pairs {
        key.hash(hasher);
        value(v, hasher);
    }
}

fn hash_struct(value: &Struct, hasher: &mut impl Hasher) {
    value.fields().len().hash(hasher);
    for field in value.iter() {
        hash_optional_literal(field, hasher);
    }
}

fn hash_optional_literal(value: Option<&Literal>, hasher: &mut impl Hasher) {
    value.is_some().hash(hasher);
    let Some(value) = value else {
        return;
    };
    std::mem::discriminant(value).hash(hasher);
    match value {
        Literal::Primitive(primitive) => hash_primitive(primitive, hasher),
        Literal::Struct(fields) => hash_struct(fields, hasher),
        Literal::List(items) => {
            items.len().hash(hasher);
            for item in items {
                hash_optional_literal(item.as_ref(), hasher);
            }
        }
        Literal::Map(map) => {
            map.len().hash(hasher);
            for (key, item) in map.clone() {
                hash_optional_literal(Some(&key), hasher);
                hash_optional_literal(item.as_ref(), hasher);
            }
        }
    }
}

/// Floats go in as bits, so `-0.0` and `0.0` differ, as they do for
/// [`PrimitiveLiteral`]'s equality, which also takes every NaN as one value.
fn hash_primitive(value: &PrimitiveLiteral, hasher: &mut impl Hasher) {
    std::mem::discriminant(value).hash(hasher);
    match value {
        PrimitiveLiteral::Boolean(v) => v.hash(hasher),
        PrimitiveLiteral::Int(v) => v.hash(hasher),
        PrimitiveLiteral::Long(v) => v.hash(hasher),
        PrimitiveLiteral::Float(v) => {
            let v = if v.is_nan() { f32::NAN } else { v.into_inner() };
            v.to_bits().hash(hasher);
        }
        PrimitiveLiteral::Double(v) => {
            let v = if v.is_nan() { f64::NAN } else { v.into_inner() };
            v.to_bits().hash(hasher);
        }
        PrimitiveLiteral::String(v) => v.hash(hasher),
        PrimitiveLiteral::Binary(v) => v.hash(hasher),
        PrimitiveLiteral::Int128(v) => v.hash(hasher),
        PrimitiveLiteral::UInt128(v) => v.hash(hasher),
        PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => {}
    }
}

/// Reads back the manifest list and fails unless it ends with the unchanged
/// `kept` manifests; returns the manifests before them.
async fn read_list_layout(
    table: &Table,
    manifest_list_path: &str,
    kept: &[ManifestFile],
) -> Result<Vec<ManifestFile>> {
    let bytes = table
        .file_io()
        .new_input(manifest_list_path)?
        .read()
        .await?;
    let list = ManifestList::parse_with_version(&bytes, table.metadata().format_version())?;
    let Some(created_count) = list.entries().len().checked_sub(kept.len()) else {
        return Err(output_mismatch("the manifest list lost manifests"));
    };
    let (created, carried) = list.entries().split_at(created_count);
    if carried != kept {
        return Err(output_mismatch("the kept manifests changed"));
    }
    Ok(created.to_vec())
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
    expected: EntryDigest,
) -> Result<()> {
    let metadata = table.metadata();
    let format_version = metadata.format_version();
    let created = read_list_layout(table, manifest_list_path, kept).await?;

    let spec = metadata.default_partition_spec();
    let mut actual = EntryDigest::default();
    for manifest_file in &created {
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

    thread_local! {
        /// Runs on a rebased manifest list before it is checked, for rebases on this thread.
        pub(super) static REBASE_OUTPUT: std::cell::RefCell<Option<OutputHook>> =
            const { std::cell::RefCell::new(None) };
    }

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

    /// An output that fails verification is deleted, as no catalog saw it: the new manifest and
    /// the manifest list.
    #[tokio::test]
    async fn test_an_output_that_fails_verification_is_deleted() {
        let catalog = new_memory_catalog().await;
        let table = table_with_a_delete_manifest(&catalog).await;
        let written = Arc::new(std::sync::Mutex::new(Vec::new()));
        let hook: OutputHook = {
            let written = written.clone();
            Arc::new(move |table: Table, list_path: String| {
                let written = written.clone();
                Box::pin(async move {
                    let bytes = table.file_io().new_input(&list_path).unwrap();
                    let bytes = bytes.read().await.unwrap();
                    let list = ManifestList::parse_with_version(&bytes, FormatVersion::V2).unwrap();
                    let created = list.entries()[0].manifest_path.clone();
                    written.lock().unwrap().extend([created, list_path.clone()]);
                    let drop_one = |entries: &mut Vec<ManifestEntry>| {
                        entries.pop();
                    };
                    rewrite_created_manifest(table, list_path, drop_one, |_| {}).await;
                })
            })
        };
        let action = Transaction::new(&table)
            .rewrite_manifests()
            .corrupt_output(hook);

        assert!(Arc::new(action).commit(&table).await.is_err());
        let written = written.lock().unwrap().clone();
        assert_eq!(written.len(), 2);
        for path in written {
            assert!(!table.file_io().exists(&path).await.unwrap(), "{path}");
        }
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

    /// Bounds that exclude every value make readers skip the file.
    #[tokio::test]
    async fn test_verification_fails_on_changed_column_bounds() {
        let (_, message) = verification_error(
            |entries| {
                entries[0]
                    .data_file
                    .lower_bounds
                    .insert(1, Datum::long(1000));
                entries[0].data_file.upper_bounds.insert(1, Datum::long(0));
            },
            |_| {},
        )
        .await;

        assert_eq!(message, ENTRIES_DIFFER);
    }

    #[tokio::test]
    async fn test_verification_fails_on_changed_null_counts() {
        let (_, message) = verification_error(
            |entries| {
                let record_count = entries[0].data_file.record_count;
                entries[0]
                    .data_file
                    .null_value_counts
                    .insert(1, record_count);
            },
            |_| {},
        )
        .await;

        assert_eq!(message, ENTRIES_DIFFER);
    }

    #[tokio::test]
    async fn test_verification_fails_on_changed_file_metadata() {
        let (_, message) = verification_error(
            |entries| {
                entries[0].data_file.key_metadata = Some(vec![1, 2, 3]);
                entries[0].data_file.split_offsets = Some(vec![4, 999]);
                entries[0].data_file.sort_order_id = Some(77);
            },
            |_| {},
        )
        .await;

        assert_eq!(message, ENTRIES_DIFFER);
    }

    /// A partition of the four values, with the last value nested in a struct.
    fn partition_of(
        first: Option<Literal>,
        second: Option<Literal>,
        third: Option<Literal>,
        nested: Literal,
    ) -> Struct {
        Struct::from_iter([
            first,
            second,
            third,
            Some(Literal::Struct(Struct::from_iter([Some(nested)]))),
        ])
    }

    /// A live entry with every field of its data file set, NaN bounds, and 64 column sizes.
    fn entry_with_every_field() -> ManifestEntry {
        let data_file = DataFile {
            content: DataContentType::PositionDeletes,
            file_path: "test/a.parquet".to_string(),
            file_format: DataFileFormat::Parquet,
            partition: partition_of(
                Some(Literal::double(0.0)),
                None,
                Some(Literal::float(0.0_f32)),
                Literal::long(1),
            ),
            record_count: 10,
            file_size_in_bytes: 100,
            column_sizes: (1..=64_u8)
                .map(|id| (i32::from(id), 40 + u64::from(id)))
                .collect(),
            value_counts: HashMap::from([(1, 10), (2, 10)]),
            null_value_counts: HashMap::from([(1, 0)]),
            nan_value_counts: HashMap::from([(2, 0)]),
            lower_bounds: HashMap::from([
                (1, Datum::long(1)),
                (2, Datum::double(0.0)),
                (3, Datum::int(1)),
            ]),
            upper_bounds: HashMap::from([
                (1, Datum::long(9)),
                (2, Datum::double(f64::NAN)),
                (4, Datum::float(f32::NAN)),
            ]),
            key_metadata: Some(vec![1, 2]),
            split_offsets: Some(vec![4, 60]),
            equality_ids: Some(vec![1]),
            sort_order_id: Some(1),
            first_row_id: Some(7),
            partition_spec_id: 0,
            referenced_data_file: Some("test/d.parquet".to_string()),
            content_offset: Some(4),
            content_size_in_bytes: Some(20),
        };
        ManifestEntry {
            status: ManifestStatus::Existing,
            snapshot_id: Some(1),
            sequence_number: Some(2),
            file_sequence_number: Some(3),
            data_file,
        }
    }

    /// Two entries hash alike exactly when they are equal: a change to any field
    /// changes the hash, `-0.0` differs from `0.0` in either width, a bound's type
    /// counts beside its literal, a value cannot move to a neighbouring map, null or
    /// struct, a value's variant counts beside its bits, and neither the NaN's bits
    /// nor the order of a map matter.
    #[test]
    fn test_entry_hash_follows_entry_equality() {
        type Edit = fn(&mut ManifestEntry);
        let base = entry_with_every_field();
        let edits: &[(&str, Edit)] = &[
            ("snapshot_id", |e| e.snapshot_id = None),
            ("sequence_number", |e| e.sequence_number = Some(3)),
            ("file_sequence_number", |e| e.file_sequence_number = Some(2)),
            ("content", |e| {
                e.data_file.content = DataContentType::EqualityDeletes;
            }),
            ("file_path", |e| e.data_file.file_path.push('x')),
            ("file_format", |e| {
                e.data_file.file_format = DataFileFormat::Orc
            }),
            ("partition -0.0", |e| {
                e.data_file.partition = partition_of(
                    Some(Literal::double(-0.0)),
                    None,
                    Some(Literal::float(0.0_f32)),
                    Literal::long(1),
                );
            }),
            ("partition float -0.0", |e| {
                e.data_file.partition = partition_of(
                    Some(Literal::double(0.0)),
                    None,
                    Some(Literal::float(-0.0_f32)),
                    Literal::long(1),
                );
            }),
            ("partition null", |e| {
                e.data_file.partition =
                    partition_of(Some(Literal::double(0.0)), None, None, Literal::long(1));
            }),
            ("partition value and null swapped", |e| {
                e.data_file.partition = partition_of(
                    None,
                    Some(Literal::double(0.0)),
                    Some(Literal::float(0.0_f32)),
                    Literal::long(1),
                );
            }),
            ("partition int with the float's bits", |e| {
                e.data_file.partition = partition_of(
                    Some(Literal::double(0.0)),
                    None,
                    Some(Literal::int(0)),
                    Literal::long(1),
                );
            }),
            ("partition list with the struct's items", |e| {
                e.data_file.partition = Struct::from_iter([
                    Some(Literal::double(0.0)),
                    None,
                    Some(Literal::float(0.0_f32)),
                    Some(Literal::List(vec![Some(Literal::long(1))])),
                ]);
            }),
            ("partition value moved out of the nested struct", |e| {
                e.data_file.partition = Struct::from_iter([
                    Some(Literal::double(0.0)),
                    None,
                    Some(Literal::float(0.0_f32)),
                    Some(Literal::Struct(Struct::from_iter([]))),
                    Some(Literal::long(1)),
                ]);
            }),
            ("partition fields", |e| {
                e.data_file.partition = Struct::from_iter([Some(Literal::double(0.0)), None]);
            }),
            ("record_count", |e| e.data_file.record_count = 11),
            ("file_size_in_bytes", |e| {
                e.data_file.file_size_in_bytes = 99
            }),
            ("column_sizes", |e| {
                e.data_file.column_sizes.insert(3, 0);
            }),
            ("value_counts", |e| {
                e.data_file.value_counts.insert(2, 9);
            }),
            ("null_value_counts", |e| {
                e.data_file.null_value_counts.remove(&1);
            }),
            ("nan_value_counts", |e| {
                e.data_file.nan_value_counts.insert(2, 1);
            }),
            ("null count moved to the nan counts", |e| {
                e.data_file.null_value_counts.remove(&1);
                e.data_file.nan_value_counts.insert(1, 0);
            }),
            ("lower bound -0.0", |e| {
                e.data_file.lower_bounds.insert(2, Datum::double(-0.0));
            }),
            ("lower bound type", |e| {
                e.data_file.lower_bounds.insert(1, Datum::int(1));
            }),
            ("lower bound type, same literal", |e| {
                e.data_file.lower_bounds.insert(3, Datum::date(1));
            }),
            ("upper_bounds", |e| {
                e.data_file.upper_bounds.insert(1, Datum::long(10));
            }),
            ("key_metadata", |e| e.data_file.key_metadata = None),
            ("split_offsets", |e| {
                e.data_file.split_offsets = Some(vec![4])
            }),
            ("equality_ids", |e| e.data_file.equality_ids = Some(vec![2])),
            ("sort_order_id", |e| e.data_file.sort_order_id = Some(0)),
            ("first_row_id", |e| e.data_file.first_row_id = None),
            ("referenced_data_file", |e| {
                e.data_file.referenced_data_file = None;
            }),
            ("partition_spec_id", |e| e.data_file.partition_spec_id = 1),
            ("content_offset", |e| e.data_file.content_offset = Some(5)),
            ("content_size_in_bytes", |e| {
                e.data_file.content_size_in_bytes = Some(19);
            }),
        ];
        for (field, edit) in edits {
            let mut edited = base.clone();
            edit(&mut edited);
            assert_ne!(edited, base, "{field}");
            assert_ne!(
                super::entry_hash(&edited),
                super::entry_hash(&base),
                "{field}"
            );
        }

        let mut same = base.clone();
        same.status = ManifestStatus::Added;
        let other_nan = f64::from_bits(f64::NAN.to_bits() | 1);
        same.data_file
            .upper_bounds
            .insert(2, Datum::double(other_nan));
        let other_float_nan = f32::from_bits(f32::NAN.to_bits() | 1);
        same.data_file
            .upper_bounds
            .insert(4, Datum::float(other_float_nan));
        let mut sizes: Vec<(i32, u64)> = base
            .data_file
            .column_sizes
            .iter()
            .map(|(id, size)| (*id, *size))
            .collect();
        sizes.reverse();
        same.data_file.column_sizes = sizes.into_iter().collect();
        // Each map has its own hasher seed, so 64 keys come out in another order.
        assert!(
            !same
                .data_file
                .column_sizes
                .keys()
                .eq(base.data_file.column_sizes.keys())
        );
        assert_eq!(same.data_file, base.data_file);
        assert_eq!(super::entry_hash(&same), super::entry_hash(&base));
    }

    /// Commits `commit` to `table` through `catalog`.
    async fn commit_to(
        catalog: &impl Catalog,
        table: &Table,
        mut commit: crate::transaction::ActionCommit,
    ) -> crate::Result<Table> {
        catalog
            .update_table(
                crate::TableCommit::builder()
                    .ident(table.identifier().clone())
                    .updates(commit.take_updates())
                    .requirements(commit.take_requirements())
                    .build(),
            )
            .await
    }

    /// The five files of the table [`planned_rewrite`] builds.
    const ALL: [&str; 5] = ["a", "b", "c", "d", "e"];

    /// A table of five single-file appends, a rewrite planned on it that replaces the manifests
    /// holding the files named in `select`, the plan's commit, and its snapshot.
    async fn planned_rewrite(
        catalog: &impl Catalog,
        format_version: FormatVersion,
        select: &[&str],
    ) -> (
        Table,
        crate::transaction::ActionCommit,
        crate::spec::Snapshot,
    ) {
        let mut table = match format_version {
            FormatVersion::V1 => make_v1_minimal_table_in_catalog(catalog).await,
            _ => make_v2_minimal_table_in_catalog(catalog).await,
        };
        for name in ALL {
            table = append_one(catalog, table, data_file(name, 1, 100, 1)).await;
        }
        let paths = manifest_paths_holding(&table, select).await;
        let mut planned = Arc::new(
            Transaction::new(&table)
                .rewrite_manifests()
                .rewrite_if(move |m| paths.contains(&m.manifest_path)),
        )
        .commit(&table)
        .await
        .unwrap();
        let updates = planned.take_updates();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            panic!("the plan adds a snapshot")
        };
        let snapshot = snapshot.clone();
        let planned = crate::transaction::ActionCommit::new(updates, planned.take_requirements());
        (table, planned, snapshot)
    }

    /// Rewrites the manifest list of `table`'s current snapshot in place so that it lists the
    /// manifest at `index` twice, and returns the table reloaded from `catalog`.
    async fn list_a_manifest_twice(catalog: &impl Catalog, table: &Table, index: usize) -> Table {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let mut manifests = table
            .manifest_list_reader(snapshot)
            .load()
            .await
            .unwrap()
            .entries()
            .to_vec();
        manifests.push(manifests[index].clone());
        let count = manifests.len();
        let mut writer = ManifestListWriter::v2(
            table
                .file_io()
                .new_output(snapshot.manifest_list())
                .unwrap()
                .writer()
                .await
                .unwrap(),
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
        );
        writer.add_manifests(manifests.into_iter()).unwrap();
        writer.close().await.unwrap();
        let reloaded = catalog.load_table(table.identifier()).await.unwrap();
        assert_eq!(data_manifests(&reloaded).await.len(), count);
        reloaded
    }

    /// Deletes the data file of the manifest at `index` of `table`'s list with an overwrite.
    async fn delete_the_file_of(catalog: &impl Catalog, table: &Table, index: usize) -> Table {
        let manifest = data_manifests(table).await[index].clone();
        let file = table
            .manifest_reader()
            .read(&manifest)
            .await
            .unwrap()
            .entries()[0]
            .data_file()
            .clone();
        let tx = Transaction::new(table);
        tx.overwrite()
            .delete_data_files([file])
            .apply(tx)
            .unwrap()
            .commit(catalog)
            .await
            .unwrap()
    }

    async fn live_paths_of(table: &Table) -> Vec<String> {
        all_live_entries(table)
            .await
            .into_iter()
            .map(|e| e.1)
            .collect()
    }

    /// A writer deleted the file of a replaced manifest, so that manifest is gone from the head,
    /// while the head lists another replaced manifest twice: the rebase is refused, as Java's
    /// per-manifest check refuses it. Once with the duplicate already in the base (the plan
    /// replaced it twice) and once with the duplicate only in the head.
    #[tokio::test]
    async fn test_rebase_is_refused_when_a_replaced_manifest_is_gone_and_another_is_listed_twice() {
        let mut outcomes = Vec::new();
        for duplicate_in_base in [true, false] {
            let catalog = new_memory_catalog().await;
            let mut table = make_v2_minimal_table_in_catalog(&catalog).await;
            for name in ["a", "b", "c", "d", "e"] {
                table = append_one(&catalog, table, data_file(name, 1, 100, 1)).await;
            }
            let base = if duplicate_in_base {
                list_a_manifest_twice(&catalog, &table, 0).await
            } else {
                table
            };
            let mut planned = Arc::new(Transaction::new(&base).rewrite_manifests())
                .commit(&base)
                .await
                .unwrap();
            let updates = planned.take_updates();
            let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
                panic!("the plan adds a snapshot")
            };
            let snapshot = snapshot.clone();
            let replaced: Vec<String> = data_manifests(&base)
                .await
                .into_iter()
                .map(|m| m.manifest_path)
                .collect();

            let head = delete_the_file_of(&catalog, &base, 1).await;
            let head = if duplicate_in_base {
                head
            } else {
                list_a_manifest_twice(&catalog, &head, 0).await
            };
            let head_paths: Vec<String> = data_manifests(&head)
                .await
                .into_iter()
                .map(|m| m.manifest_path)
                .collect();
            assert!(
                !head_paths.contains(&replaced[1]),
                "B is gone from the head"
            );
            let rebased = RewriteManifestsAction::rebase(&base, &snapshot, &head)
                .await
                .unwrap();
            let outcome = match rebased {
                None => "refused".to_string(),
                Some(rebased) => {
                    let after = commit_to(&catalog, &head, rebased).await.unwrap();
                    format!(
                        "rebased: head live {:?}, after live {:?}",
                        live_paths_of(&head).await,
                        live_paths_of(&after).await
                    )
                }
            };
            outcomes.push((duplicate_in_base, outcome));
        }
        assert_eq!(outcomes, vec![
            (true, "refused".to_string()),
            (false, "refused".to_string())
        ]);
    }

    /// A rebased list that fails its check is refused and deleted, as no catalog saw it.
    #[tokio::test]
    async fn test_a_rebased_list_that_fails_its_check_is_deleted() {
        let catalog = new_memory_catalog().await;
        let (base, _, snapshot) = planned_rewrite(&catalog, FormatVersion::V2, &ALL).await;
        let head = append_one(&catalog, base.clone(), data_file("f", 1, 100, 1)).await;
        let parent = head.metadata().current_snapshot_id();
        let sequence_number = head.metadata().next_sequence_number();
        let snapshot_id = snapshot.snapshot_id();
        let written = Arc::new(std::sync::Mutex::new(Vec::new()));
        let hook: OutputHook = {
            let written = written.clone();
            Arc::new(move |table: Table, list_path: String| {
                let written = written.clone();
                Box::pin(async move {
                    written.lock().unwrap().push(list_path.clone());
                    let file_io = table.file_io();
                    let bytes = file_io.new_input(&list_path).unwrap().read().await.unwrap();
                    let mut manifests: Vec<ManifestFile> =
                        ManifestList::parse_with_version(&bytes, FormatVersion::V2)
                            .unwrap()
                            .consume_entries()
                            .into_iter()
                            .collect();
                    // The created manifest is no longer first.
                    manifests.reverse();
                    let output = file_io.new_output(&list_path).unwrap();
                    let mut writer = ManifestListWriter::v2(
                        output.writer().await.unwrap(),
                        snapshot_id,
                        parent,
                        sequence_number,
                    );
                    writer.add_manifests(manifests.into_iter()).unwrap();
                    writer.close().await.unwrap();
                })
            })
        };
        REBASE_OUTPUT.with(|output| *output.borrow_mut() = Some(hook));
        let rebased = RewriteManifestsAction::rebase(&base, &snapshot, &head).await;
        REBASE_OUTPUT.with(|output| *output.borrow_mut() = None);

        let error = rebased.err().expect("the check refuses the list");
        assert_eq!(
            error.message(),
            "RewriteManifests output failed verification: the kept manifests changed"
        );
        let written = written.lock().unwrap().clone();
        assert_eq!(written.len(), 1);
        assert!(!head.file_io().exists(&written[0]).await.unwrap());
    }

    /// The rebase is refused when the head holds the planned snapshot, or its default spec,
    /// current schema or format version differs from the base.
    #[tokio::test]
    async fn test_rebase_is_refused_when_the_head_does_not_fit_the_plan() {
        use crate::spec::{NestedField, PrimitiveType, Schema, Type, UnboundPartitionSpec};

        let catalog = new_memory_catalog().await;
        fn wider(table: &Table) -> Schema {
            let schema = table.metadata().current_schema();
            Schema::builder()
                .with_fields(
                    schema
                        .as_struct()
                        .fields()
                        .iter()
                        .cloned()
                        .chain([NestedField::optional(
                            100,
                            "extra",
                            Type::Primitive(PrimitiveType::Long),
                        )
                        .into()]),
                )
                .build()
                .unwrap()
        }
        let changes: [fn(&Table) -> Vec<TableUpdate>; 3] = [
            |_| {
                vec![
                    TableUpdate::AddSpec {
                        spec: UnboundPartitionSpec::builder().build(),
                    },
                    TableUpdate::SetDefaultSpec { spec_id: -1 },
                ]
            },
            |table| {
                vec![
                    TableUpdate::AddSchema {
                        schema: wider(table),
                    },
                    TableUpdate::SetCurrentSchema { schema_id: -1 },
                ]
            },
            |_| {
                vec![TableUpdate::UpgradeFormatVersion {
                    format_version: FormatVersion::V3,
                }]
            },
        ];
        for (i, change) in changes.iter().enumerate() {
            let (base, _, snapshot) = planned_rewrite(&catalog, FormatVersion::V2, &ALL).await;
            let updates = change(&base);
            let head = commit_to(
                &catalog,
                &base,
                crate::transaction::ActionCommit::new(updates, vec![]),
            )
            .await
            .unwrap();
            assert_eq!(
                head.metadata().current_snapshot_id(),
                base.metadata().current_snapshot_id()
            );
            let rebased = RewriteManifestsAction::rebase(&base, &snapshot, &head)
                .await
                .unwrap();
            assert!(rebased.is_none(), "change {i}");
        }

        // The planned rewrite landed, and `main` was rolled back to its parent.
        let (base, planned, snapshot) = planned_rewrite(&catalog, FormatVersion::V2, &ALL).await;
        let landed = commit_to(&catalog, &base, planned).await.unwrap();
        let rollback = TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: crate::spec::SnapshotReference::new(
                base.metadata().current_snapshot_id().unwrap(),
                SnapshotRetention::branch(None, None, None),
            ),
        };
        let head = commit_to(
            &catalog,
            &landed,
            crate::transaction::ActionCommit::new(vec![rollback], vec![]),
        )
        .await
        .unwrap();
        assert!(
            head.metadata()
                .snapshot_by_id(snapshot.snapshot_id())
                .is_some()
        );
        let rebased = RewriteManifestsAction::rebase(&base, &snapshot, &head)
            .await
            .unwrap();
        assert!(rebased.is_none());
    }

    /// Every live entry of the current snapshot, data and deletes: content, path, snapshot id and
    /// sequence numbers.
    async fn all_live_entries(
        table: &Table,
    ) -> Vec<(String, String, Option<i64>, Option<i64>, Option<i64>)> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let list = table.manifest_list_reader(snapshot).load().await.unwrap();
        let mut entries = Vec::new();
        for m in list.entries() {
            for e in table.manifest_reader().read(m).await.unwrap().entries() {
                if e.is_alive() {
                    entries.push((
                        format!("{:?}", e.content_type()),
                        e.file_path().to_string(),
                        e.snapshot_id(),
                        e.sequence_number(),
                        e.file_sequence_number,
                    ));
                }
            }
        }
        entries.sort();
        entries
    }

    /// Every manifest of the current snapshot, in list order.
    async fn all_manifests(table: &Table) -> Vec<ManifestFile> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        table
            .manifest_list_reader(snapshot)
            .load()
            .await
            .unwrap()
            .entries()
            .to_vec()
    }

    /// The manifests `snapshot` created and the paths of `base`'s manifests it replaced.
    async fn created_and_replaced(
        base: &Table,
        snapshot: &crate::spec::Snapshot,
    ) -> (Vec<ManifestFile>, Vec<String>) {
        let planned = base
            .manifest_list_reader(&Arc::new(snapshot.clone()))
            .load()
            .await
            .unwrap()
            .entries()
            .to_vec();
        let planned_paths: Vec<&str> = planned.iter().map(|m| m.manifest_path.as_str()).collect();
        let replaced = all_manifests(base)
            .await
            .into_iter()
            .map(|m| m.manifest_path)
            .filter(|p| !planned_paths.contains(&p.as_str()))
            .collect();
        let created = planned
            .into_iter()
            .filter(|m| m.added_snapshot_id == snapshot.snapshot_id())
            .collect();
        (created, replaced)
    }

    const TOTALS: [&str; 6] = [
        "total-data-files",
        "total-delete-files",
        "total-records",
        "total-files-size",
        "total-position-deletes",
        "total-equality-deletes",
    ];

    /// Rebases `snapshot`, planned on `base`, onto `head`, commits it and checks the result
    /// against `head`: the planned snapshot id, `head` as parent and its next sequence number,
    /// the planned manifests (with the new sequence number) followed by `head`'s other manifests
    /// in `head`'s order, `head`'s live entries, the planned counts, `head`'s totals.
    async fn rebase_and_check(
        catalog: &impl Catalog,
        base: &Table,
        snapshot: &crate::spec::Snapshot,
        head: &Table,
    ) -> Table {
        let (created, replaced) = created_and_replaced(base, snapshot).await;
        let kept: Vec<ManifestFile> = all_manifests(head)
            .await
            .into_iter()
            .filter(|m| !replaced.contains(&m.manifest_path))
            .collect();
        let rebased = RewriteManifestsAction::rebase(base, snapshot, head)
            .await
            .unwrap()
            .expect("every replaced manifest is in the head");
        let after = commit_to(catalog, head, rebased).await.unwrap();
        let current = after.metadata().current_snapshot().unwrap();
        let parent = head.metadata().current_snapshot().unwrap();
        assert_eq!(current.snapshot_id(), snapshot.snapshot_id());
        assert_eq!(current.parent_snapshot_id(), Some(parent.snapshot_id()));
        assert_eq!(
            current.sequence_number(),
            head.metadata().next_sequence_number()
        );
        assert_eq!(current.summary().operation, Operation::Replace);
        let v1 = head.metadata().format_version() == FormatVersion::V1;
        let expected: Vec<ManifestFile> = created
            .iter()
            .map(|m| ManifestFile {
                sequence_number: if v1 { 0 } else { current.sequence_number() },
                ..m.clone()
            })
            .chain(kept.iter().cloned())
            .collect();
        assert_eq!(all_manifests(&after).await, expected);
        assert_eq!(all_live_entries(&after).await, all_live_entries(head).await);
        let props = &current.summary().additional_properties;
        let planned_props = &snapshot.summary().additional_properties;
        for key in [
            "manifests-created",
            "manifests-replaced",
            "entries-processed",
        ] {
            assert_eq!(props.get(key), planned_props.get(key), "{key}");
        }
        assert_eq!(props["manifests-kept"], kept.len().to_string());
        let head_props = &parent.summary().additional_properties;
        for key in TOTALS {
            assert_eq!(props.get(key), head_props.get(key), "{key}");
        }
        after
    }

    /// Paths of the data manifests that hold a file named in `names`.
    async fn manifest_paths_holding(table: &Table, names: &[&str]) -> Vec<String> {
        let mut paths = Vec::new();
        for m in data_manifests(table).await {
            let entries = table.manifest_reader().read(&m).await.unwrap();
            if entries.entries().iter().any(|e| {
                names
                    .iter()
                    .any(|n| e.file_path() == format!("test/{n}.parquet"))
            }) {
                paths.push(m.manifest_path.clone());
            }
        }
        paths
    }

    /// The live data file named `name`.
    async fn live_file(table: &Table, name: &str) -> DataFile {
        for m in data_manifests(table).await {
            for e in table.manifest_reader().read(&m).await.unwrap().entries() {
                if e.is_alive() && e.file_path() == format!("test/{name}.parquet") {
                    return e.data_file().clone();
                }
            }
        }
        panic!("no live file {name}")
    }

    async fn overwrite(
        catalog: &impl Catalog,
        table: &Table,
        add: Vec<DataFile>,
        delete: Vec<DataFile>,
    ) -> Table {
        let tx = Transaction::new(table);
        tx.overwrite()
            .add_data_files(add)
            .delete_data_files(delete)
            .apply(tx)
            .unwrap()
            .commit(catalog)
            .await
            .unwrap()
    }

    async fn update(catalog: &impl Catalog, table: &Table, updates: Vec<TableUpdate>) -> Table {
        commit_to(
            catalog,
            table,
            crate::transaction::ActionCommit::new(updates, vec![]),
        )
        .await
        .unwrap()
    }

    fn set_ref(name: &str, snapshot_id: i64, retention: SnapshotRetention) -> TableUpdate {
        TableUpdate::SetSnapshotRef {
            ref_name: name.to_string(),
            reference: crate::spec::SnapshotReference::new(snapshot_id, retention),
        }
    }

    fn delete_of(content: DataContentType, name: &str, records: u64) -> DataFile {
        let mut file = data_file(name, 1, 50, records);
        file.content = content;
        if content == DataContentType::EqualityDeletes {
            file.equality_ids = Some(vec![2]);
        }
        file
    }

    /// Writer commits that keep every manifest the plan replaces: the rewrite is re-applied on
    /// each, on v1 and v2 tables, and keeps the writer's manifests and entries exactly.
    #[tokio::test]
    async fn test_rebase_applies_on_writer_commits_that_keep_the_replaced_manifests() {
        for format_version in [FormatVersion::V1, FormatVersion::V2] {
            let catalog = new_memory_catalog().await;

            // An append: the planned manifest, then the appended one, and the plan's counts.
            let (base, _, snapshot) = planned_rewrite(&catalog, format_version, &ALL).await;
            let head = append_one(&catalog, base.clone(), data_file("f", 1, 100, 1)).await;
            let after = rebase_and_check(&catalog, &base, &snapshot, &head).await;
            let current = after.metadata().current_snapshot().unwrap();
            let summary = |key: &str| current.summary().additional_properties[key].clone();
            assert_eq!(
                [
                    "manifests-created",
                    "manifests-replaced",
                    "manifests-kept",
                    "entries-processed",
                    "total-data-files",
                    "total-records",
                ]
                .map(summary),
                ["1", "5", "1", "5", "6", "6"].map(String::from)
            );

            // An overwrite that only adds files.
            let (base, _, snapshot) = planned_rewrite(&catalog, format_version, &ALL).await;
            let head = overwrite(&catalog, &base, vec![data_file("f", 1, 100, 7)], vec![]).await;
            let after = rebase_and_check(&catalog, &base, &snapshot, &head).await;
            assert_eq!(all_manifests(&after).await.len(), 2);

            // An overwrite that deletes a file of a manifest the plan carries.
            let (base, _, snapshot) =
                planned_rewrite(&catalog, format_version, &["a", "b", "c"]).await;
            let e = live_file(&base, "e").await;
            let head = overwrite(&catalog, &base, vec![data_file("g", 1, 100, 3)], vec![e]).await;
            let after = rebase_and_check(&catalog, &base, &snapshot, &head).await;
            let names: Vec<String> = all_live_entries(&after)
                .await
                .into_iter()
                .map(|e| e.1)
                .collect();
            assert_eq!(
                names,
                ["a", "b", "c", "d", "g"].map(|n| format!("test/{n}.parquet"))
            );

            // A concurrent rewrite of the manifests the plan carries.
            let (base, _, snapshot) =
                planned_rewrite(&catalog, format_version, &["a", "b", "c"]).await;
            let carried = manifest_paths_holding(&base, &["d", "e"]).await;
            let tx = Transaction::new(&base);
            let head = tx
                .rewrite_manifests()
                .rewrite_if(move |m| carried.contains(&m.manifest_path))
                .apply(tx)
                .unwrap()
                .commit(&catalog)
                .await
                .unwrap();
            assert_eq!(data_manifests(&head).await.len(), 4);
            let after = rebase_and_check(&catalog, &base, &snapshot, &head).await;
            assert_eq!(data_manifests(&after).await.len(), 2);

            // A property change: the head's snapshot is the base's.
            let (base, _, snapshot) = planned_rewrite(&catalog, format_version, &ALL).await;
            let head = update(&catalog, &base, vec![TableUpdate::SetProperties {
                updates: HashMap::from([("k".to_string(), "v".to_string())]),
            }])
            .await;
            rebase_and_check(&catalog, &base, &snapshot, &head).await;

            // An append, then the base's snapshot expired.
            let (base, _, snapshot) = planned_rewrite(&catalog, format_version, &ALL).await;
            let head = append_one(&catalog, base.clone(), data_file("f", 1, 100, 1)).await;
            let base_id = base.metadata().current_snapshot_id().unwrap();
            let head = update(&catalog, &head, vec![TableUpdate::RemoveSnapshots {
                snapshot_ids: vec![base_id],
            }])
            .await;
            assert!(head.metadata().snapshot_by_id(base_id).is_none());
            rebase_and_check(&catalog, &base, &snapshot, &head).await;

            // `main` rolled back to the third append; the plan replaces only its manifests.
            let (base, _, snapshot) =
                planned_rewrite(&catalog, format_version, &["a", "b", "c"]).await;
            let current = base.metadata().current_snapshot().unwrap();
            let parent = base
                .metadata()
                .snapshot_by_id(current.parent_snapshot_id().unwrap())
                .unwrap();
            let third = parent.parent_snapshot_id().unwrap();
            let head = update(&catalog, &base, vec![set_ref(
                MAIN_BRANCH,
                third,
                SnapshotRetention::branch(None, None, None),
            )])
            .await;
            let after = rebase_and_check(&catalog, &base, &snapshot, &head).await;
            assert_eq!(all_manifests(&after).await.len(), 1);
            let names: Vec<String> = all_live_entries(&after)
                .await
                .into_iter()
                .map(|e| e.1)
                .collect();
            assert_eq!(names, ["a", "b", "c"].map(|n| format!("test/{n}.parquet")));

            // A tag and a branch on the base, then an append to `main`: both refs stay.
            let (base, _, snapshot) = planned_rewrite(&catalog, format_version, &ALL).await;
            let base_id = base.metadata().current_snapshot_id().unwrap();
            let head = update(&catalog, &base, vec![
                set_ref("t", base_id, SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                }),
                set_ref("b", base_id, SnapshotRetention::branch(None, None, None)),
            ])
            .await;
            let head = append_one(&catalog, head, data_file("f", 1, 100, 1)).await;
            let after = rebase_and_check(&catalog, &base, &snapshot, &head).await;
            assert_eq!(after.metadata().refs()["t"].snapshot_id, base_id);
            assert_eq!(after.metadata().refs()["b"].snapshot_id, base_id);
        }
    }

    /// Delete files of a merge-on-read writer land between the plan and its commit: the rewrite
    /// is re-applied, keeps the delete manifest, the deletes' sequence numbers, and the data
    /// entries' sequence numbers that decide which rows the deletes remove.
    #[tokio::test]
    async fn test_rebase_keeps_position_and_equality_deletes_and_their_sequence_numbers() {
        let catalog = new_memory_catalog().await;
        let (base, _, snapshot) = planned_rewrite(&catalog, FormatVersion::V2, &ALL).await;
        let mut position = delete_of(DataContentType::PositionDeletes, "pos", 2);
        position.referenced_data_file = Some("test/a.parquet".to_string());
        let equality = delete_of(DataContentType::EqualityDeletes, "eq", 1);
        let head = stage_delete_files(&catalog, &base, vec![position, equality]).await;
        let head = append_one(&catalog, head, data_file("f", 1, 100, 1)).await;
        let delete_seq = head.metadata().next_sequence_number() - 2;

        let after = rebase_and_check(&catalog, &base, &snapshot, &head).await;

        let manifests = all_manifests(&after).await;
        assert_eq!(manifests.iter().map(|m| m.content).collect::<Vec<_>>(), [
            ManifestContentType::Data,
            ManifestContentType::Deletes,
            ManifestContentType::Data
        ]);
        let created = &manifests[0];
        let base_seqs: Vec<i64> = all_live_entries(&base)
            .await
            .into_iter()
            .map(|e| e.3.unwrap())
            .collect();
        assert_eq!(
            created.min_sequence_number,
            *base_seqs.iter().min().unwrap()
        );
        assert!(created.sequence_number > delete_seq);
        let deletes: Vec<(String, Option<i64>)> = all_live_entries(&after)
            .await
            .into_iter()
            .filter(|e| e.0 != "Data")
            .map(|e| (e.1, e.3))
            .collect();
        assert_eq!(deletes, vec![
            ("test/eq.parquet".to_string(), Some(delete_seq)),
            ("test/pos.parquet".to_string(), Some(delete_seq)),
        ]);
        // The data entries keep the sequence numbers that decide which rows the deletes remove.
        let data: Vec<i64> = all_live_entries(&after)
            .await
            .into_iter()
            .filter(|e| e.0 == "Data")
            .map(|e| e.3.unwrap())
            .collect();
        let mut data = data;
        data.sort_unstable();
        assert_eq!(
            data,
            (delete_seq - 5..delete_seq)
                .chain([delete_seq + 1])
                .collect::<Vec<_>>()
        );
    }

    /// Writer commits that remove a manifest the plan replaces, or change what the plan was
    /// written for: the rebase is refused, on v1 and v2 tables.
    #[tokio::test]
    async fn test_rebase_is_refused_after_writer_commits_that_drop_a_replaced_manifest() {
        for format_version in [FormatVersion::V1, FormatVersion::V2] {
            let catalog = new_memory_catalog().await;

            // An overwrite deleting a file of a replaced manifest rewrites that manifest.
            let (base, _, snapshot) = planned_rewrite(&catalog, format_version, &ALL).await;
            let c = live_file(&base, "c").await;
            let head = overwrite(&catalog, &base, vec![], vec![c]).await;
            assert!(
                RewriteManifestsAction::rebase(&base, &snapshot, &head)
                    .await
                    .unwrap()
                    .is_none()
            );

            // A copy-on-write row delta removing a file of a replaced manifest.
            let (base, _, snapshot) = planned_rewrite(&catalog, format_version, &ALL).await;
            let a = live_file(&base, "a").await;
            let tx = Transaction::new(&base);
            let head = tx
                .row_delta()
                .remove_data_files(vec![a])
                .apply(tx)
                .unwrap()
                .commit(&catalog)
                .await
                .unwrap();
            assert!(
                RewriteManifestsAction::rebase(&base, &snapshot, &head)
                    .await
                    .unwrap()
                    .is_none()
            );

            // A concurrent rewrite that merges two replaced manifests with a carried one.
            let (base, _, snapshot) =
                planned_rewrite(&catalog, format_version, &["a", "b", "c"]).await;
            let mixed = manifest_paths_holding(&base, &["c", "d"]).await;
            let tx = Transaction::new(&base);
            let head = tx
                .rewrite_manifests()
                .rewrite_if(move |m| mixed.contains(&m.manifest_path))
                .apply(tx)
                .unwrap()
                .commit(&catalog)
                .await
                .unwrap();
            assert!(
                RewriteManifestsAction::rebase(&base, &snapshot, &head)
                    .await
                    .unwrap()
                    .is_none()
            );

            // `main` rolled back to before the last replaced manifest was added.
            let (base, _, snapshot) = planned_rewrite(&catalog, format_version, &ALL).await;
            let parent = base
                .metadata()
                .current_snapshot()
                .unwrap()
                .parent_snapshot_id()
                .unwrap();
            let head = update(&catalog, &base, vec![set_ref(
                MAIN_BRANCH,
                parent,
                SnapshotRetention::branch(None, None, None),
            )])
            .await;
            assert!(
                RewriteManifestsAction::rebase(&base, &snapshot, &head)
                    .await
                    .unwrap()
                    .is_none()
            );

            // `main` removed: the head has no current snapshot.
            let (base, _, snapshot) = planned_rewrite(&catalog, format_version, &ALL).await;
            let head = update(&catalog, &base, vec![TableUpdate::RemoveSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
            }])
            .await;
            assert!(head.metadata().current_snapshot().is_none());
            assert!(
                RewriteManifestsAction::rebase(&base, &snapshot, &head)
                    .await
                    .unwrap()
                    .is_none()
            );
        }

        // A v1 table upgraded to v2.
        let catalog = new_memory_catalog().await;
        let (base, _, snapshot) = planned_rewrite(&catalog, FormatVersion::V1, &ALL).await;
        let head = update(&catalog, &base, vec![TableUpdate::UpgradeFormatVersion {
            format_version: FormatVersion::V2,
        }])
        .await;
        assert_eq!(
            head.metadata().current_snapshot_id(),
            base.metadata().current_snapshot_id()
        );
        assert!(
            RewriteManifestsAction::rebase(&base, &snapshot, &head)
                .await
                .unwrap()
                .is_none()
        );
    }

    /// Writer commits land between each rebase and its commit: the stale rebase loses its
    /// commit, the next rebase follows it with the same snapshot id and planned manifest, and the
    /// last one commits on the third writer commit with a new manifest list each time.
    #[tokio::test]
    async fn test_rebases_chain_over_several_writer_commits() {
        for format_version in [FormatVersion::V1, FormatVersion::V2] {
            let catalog = new_memory_catalog().await;
            let (base, _, planned) = planned_rewrite(&catalog, format_version, &ALL).await;
            let (created, _) = created_and_replaced(&base, &planned).await;
            let mut previous = (base.clone(), planned.clone());
            let mut lists = vec![planned.manifest_list().to_string()];
            let mut head = append_one(&catalog, base.clone(), data_file("f", 1, 100, 1)).await;
            for name in ["g", "h"] {
                let mut rebased = RewriteManifestsAction::rebase(&previous.0, &previous.1, &head)
                    .await
                    .unwrap()
                    .unwrap();
                let updates = rebased.take_updates();
                let requirements = rebased.take_requirements();
                let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
                    panic!("a snapshot")
                };
                let snapshot = snapshot.clone();
                assert_eq!(snapshot.snapshot_id(), planned.snapshot_id());
                assert_eq!(
                    snapshot.parent_snapshot_id(),
                    head.metadata().current_snapshot_id()
                );
                lists.push(snapshot.manifest_list().to_string());
                let next = append_one(&catalog, head.clone(), data_file(name, 1, 100, 1)).await;
                let error = commit_to(
                    &catalog,
                    &head,
                    crate::transaction::ActionCommit::new(updates, requirements),
                )
                .await
                .unwrap_err();
                assert_eq!(error.kind(), crate::ErrorKind::CatalogCommitConflicts);
                previous = (head, snapshot);
                head = next;
            }
            let after = rebase_and_check(&catalog, &previous.0, &previous.1, &head).await;
            let manifests = all_manifests(&after).await;
            assert_eq!(manifests.len(), 4);
            assert_eq!(manifests[0].manifest_path, created[0].manifest_path);
            let current = after.metadata().current_snapshot().unwrap();
            lists.push(current.manifest_list().to_string());
            let distinct: std::collections::HashSet<&String> = lists.iter().collect();
            assert_eq!(distinct.len(), 4);
            assert_eq!(all_live_entries(&after).await.len(), 8);
            assert_eq!(
                after
                    .metadata()
                    .snapshots()
                    .filter(|s| s.summary().operation == Operation::Replace)
                    .count(),
                1
            );
        }
    }

    /// A plan that writes two manifests keeps both, in order, when re-applied.
    #[tokio::test]
    async fn test_rebase_keeps_several_created_manifests_in_order() {
        let catalog = new_memory_catalog().await;
        let mut table = make_v2_minimal_table_in_catalog(&catalog).await;
        for i in 0..8 {
            table = append_one(&catalog, table, data_file(&format!("p{i}"), i, 100, 1)).await;
        }
        let target = data_manifest_bytes(&table).await.div_ceil(2);
        let mut planned = Arc::new(
            Transaction::new(&table)
                .rewrite_manifests()
                .set_target_size_bytes(target),
        )
        .commit(&table)
        .await
        .unwrap();
        let updates = planned.take_updates();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            panic!("a snapshot")
        };
        let (created, _) = created_and_replaced(&table, snapshot).await;
        assert_eq!(created.len(), 2);
        let head = append_one(&catalog, table.clone(), data_file("q", 9, 100, 1)).await;
        let after = rebase_and_check(&catalog, &table, snapshot, &head).await;
        let paths: Vec<String> = all_manifests(&after)
            .await
            .into_iter()
            .map(|m| m.manifest_path)
            .collect();
        assert_eq!(&paths[..2], &[
            created[0].manifest_path.clone(),
            created[1].manifest_path.clone()
        ]);
        assert_eq!(paths.len(), 3);
    }

    /// The re-applied rewrite keeps the plan's snapshot properties, and `main`'s retention as the
    /// head has it.
    #[tokio::test]
    async fn test_rebase_keeps_snapshot_properties_and_the_heads_retention() {
        let catalog = new_memory_catalog().await;
        let mut table = make_v2_minimal_table_in_catalog(&catalog).await;
        for name in ["a", "b", "c"] {
            table = append_one(&catalog, table, data_file(name, 1, 100, 1)).await;
        }
        let mut planned = Arc::new(
            Transaction::new(&table)
                .rewrite_manifests()
                .set_snapshot_properties(HashMap::from([("app".to_string(), "x".to_string())])),
        )
        .commit(&table)
        .await
        .unwrap();
        let updates = planned.take_updates();
        let TableUpdate::AddSnapshot { snapshot } = &updates[0] else {
            panic!("a snapshot")
        };
        let head = append_one(&catalog, table.clone(), data_file("d", 1, 100, 1)).await;
        let retention = SnapshotRetention::branch(Some(7), Some(1_000_000), None);
        let head = set_main_retention(&catalog, &head, retention.clone()).await;
        let after = rebase_and_check(&catalog, &table, snapshot, &head).await;
        let current = after.metadata().current_snapshot().unwrap();
        assert_eq!(current.summary().additional_properties["app"], "x");
        assert_eq!(after.metadata().refs()[MAIN_BRANCH].retention, retention);
    }

    /// A rebase needs a base below format v3 with a current snapshot, and a rewrite snapshot
    /// planned on it.
    #[tokio::test]
    async fn test_rebase_requires_a_snapshot_planned_on_the_base() {
        let catalog = new_memory_catalog().await;
        let (base, _, snapshot) = planned_rewrite(&catalog, FormatVersion::V2, &ALL).await;
        let head = append_one(&catalog, base.clone(), data_file("f", 1, 100, 1)).await;
        let Err(error) = RewriteManifestsAction::rebase(&head, &snapshot, &head).await else {
            panic!("the snapshot was not planned on the head")
        };
        assert_eq!(error.kind(), crate::ErrorKind::PreconditionFailed);
        let empty = make_v2_minimal_table_in_catalog(&catalog).await;
        let Err(error) = RewriteManifestsAction::rebase(&empty, &snapshot, &head).await else {
            panic!("the base has no snapshot")
        };
        assert_eq!(error.kind(), crate::ErrorKind::PreconditionFailed);
        let append = head.metadata().current_snapshot().unwrap();
        let Err(error) = RewriteManifestsAction::rebase(&base, append, &head).await else {
            panic!("an append is not a rewrite")
        };
        assert_eq!(error.kind(), crate::ErrorKind::PreconditionFailed);

        let v3 = make_v3_minimal_table_in_catalog(&catalog).await;
        let v3 = append_one(&catalog, v3, data_file("a", 1, 100, 1)).await;
        let v3_head = append_one(&catalog, v3.clone(), data_file("b", 1, 100, 1)).await;
        let next = v3_head.metadata().current_snapshot().unwrap();
        let Err(error) = RewriteManifestsAction::rebase(&v3, next, &v3_head).await else {
            panic!("a v3 base is refused")
        };
        assert_eq!(error.kind(), crate::ErrorKind::FeatureUnsupported);
    }

    /// The base's manifest list is gone (its snapshot expired and its files removed): the rebase
    /// fails, it does not guess the replaced manifests.
    #[tokio::test]
    async fn test_rebase_fails_when_the_base_list_is_gone() {
        let catalog = new_memory_catalog().await;
        let (base, _, snapshot) = planned_rewrite(&catalog, FormatVersion::V2, &ALL).await;
        let head = append_one(&catalog, base.clone(), data_file("f", 1, 100, 1)).await;
        let base_list = base
            .metadata()
            .current_snapshot()
            .unwrap()
            .manifest_list()
            .to_string();
        base.file_io().delete(&base_list).await.unwrap();
        assert!(
            RewriteManifestsAction::rebase(&base, &snapshot, &head)
                .await
                .is_err()
        );
    }
}
