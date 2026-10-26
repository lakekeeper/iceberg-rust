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

//! Shared engine for snapshot-producing operations that both add and delete files.
//!
//! [`MergingSnapshotProducer`] is the Rust equivalent of Java's
//! `MergingSnapshotProducer`. It handles manifest filtering, new manifest
//! creation, summary computation, and delegates the final snapshot commit to
//! [`SnapshotProducer`].

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use uuid::Uuid;

use crate::error::{Result, invalid_data};
use crate::io::OutputFile;
use crate::spec::{
    DataFile, DataFileFormat, FormatVersion, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestStatus, ManifestWriter, ManifestWriterBuilder, Operation, PartitionSpec, SchemaRef,
    SnapshotSummaryCollector, Summary, TableProperties, update_snapshot_summaries,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::transaction::snapshot::SnapshotProducer;

/// Create a manifest writer that handles encryption when available.
fn new_manifest_writer(
    table: &Table,
    output_file: OutputFile,
    snapshot_id: Option<i64>,
    content: ManifestContentType,
    schema: SchemaRef,
    partition_spec: PartitionSpec,
) -> Result<ManifestWriter> {
    let builder = if let Some(em) = table.encryption_manager() {
        ManifestWriterBuilder::new_from_encrypted(
            em.encrypt(output_file),
            snapshot_id,
            schema,
            partition_spec,
        )?
    } else {
        ManifestWriterBuilder::new(output_file, snapshot_id, schema, partition_spec)
    };

    match table.metadata().format_version() {
        FormatVersion::V1 => Ok(builder.build_v1()),
        FormatVersion::V2 => match content {
            ManifestContentType::Data => Ok(builder.build_v2_data()),
            ManifestContentType::Deletes => Ok(builder.build_v2_deletes()),
        },
        FormatVersion::V3 => match content {
            ManifestContentType::Data => Ok(builder.build_v3_data()),
            ManifestContentType::Deletes => Ok(builder.build_v3_deletes()),
        },
    }
}

/// Names a file to remove, as Java's `DataFileWrapper` and `DeleteFileWrapper`
/// do. Data files are matched by path in data manifests. Delete files are
/// matched in delete manifests by path, content offset and size, because a
/// deletion vector is one blob of a Puffin file that other vectors can share.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RemovedFileKey {
    manifest_content: ManifestContentType,
    path: String,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
}

impl RemovedFileKey {
    fn new(manifest_content: ManifestContentType, file: &DataFile) -> Self {
        let (content_offset, content_size_in_bytes) = match manifest_content {
            ManifestContentType::Data => (None, None),
            ManifestContentType::Deletes => (file.content_offset, file.content_size_in_bytes),
        };
        Self {
            manifest_content,
            path: file.file_path.clone(),
            content_offset,
            content_size_in_bytes,
        }
    }
}

/// Filters existing manifests by removing entries for deleted files.
///
/// This is equivalent to Java's `ManifestFilterManager`. When a rewrite or
/// overwrite operation deletes files, the filter manager rewrites affected
/// manifests so that deleted entries are dropped and surviving entries are
/// re-emitted with [`ManifestStatus::Existing`].
pub(crate) struct ManifestFilterManager {
    deleted_files: HashSet<RemovedFileKey>,
    fail_missing_delete_paths: bool,
}

impl ManifestFilterManager {
    pub(crate) fn new(fail_missing_delete_paths: bool) -> Self {
        Self {
            deleted_files: HashSet::new(),
            fail_missing_delete_paths,
        }
    }

    pub(crate) fn delete_data_file(&mut self, file: &DataFile) {
        self.deleted_files
            .insert(RemovedFileKey::new(ManifestContentType::Data, file));
    }

    pub(crate) fn delete_delete_file(&mut self, file: &DataFile) {
        self.deleted_files
            .insert(RemovedFileKey::new(ManifestContentType::Deletes, file));
    }

    /// Filter `manifests` by removing the live entries of the files to delete,
    /// each from the manifests of its own content type. Returns the surviving
    /// manifests plus a [`SnapshotSummaryCollector`] that recorded metrics for
    /// every removed file.
    pub(crate) async fn filter_manifests(
        &self,
        table: &Table,
        manifests: Vec<ManifestFile>,
        snapshot_id: i64,
        commit_uuid: Uuid,
        manifest_counter: &AtomicU64,
    ) -> Result<(Vec<ManifestFile>, SnapshotSummaryCollector)> {
        if self.deleted_files.is_empty() {
            return Ok((manifests, SnapshotSummaryCollector::default()));
        }

        let mut result: Vec<ManifestFile> = Vec::with_capacity(manifests.len());
        let mut removed_collector = SnapshotSummaryCollector::default();
        let mut found_files: HashSet<RemovedFileKey> = HashSet::new();
        let target_contents: HashSet<ManifestContentType> = self
            .deleted_files
            .iter()
            .map(|key| key.manifest_content)
            .collect();

        for manifest_file in &manifests {
            // A manifest that cannot hold any file to delete is not read.
            if !target_contents.contains(&manifest_file.content) {
                result.push(manifest_file.clone());
                continue;
            }

            let manifest = table.manifest_reader().read(manifest_file).await?;
            let is_removed = |entry: &ManifestEntry| {
                entry.is_alive()
                    && self.deleted_files.contains(&RemovedFileKey::new(
                        manifest_file.content,
                        entry.data_file(),
                    ))
            };

            // Check whether this manifest contains any files we want to delete.
            let has_deletes = manifest.entries().iter().any(|e| is_removed(e));

            if !has_deletes {
                // Manifest is unaffected — pass through verbatim.
                result.push(manifest_file.clone());
                continue;
            }

            // Resolve the partition spec for this manifest. After partition
            // evolution, old manifests may carry a non-default spec — we must
            // use the manifest's own spec so the rewritten manifest stays valid.
            let manifest_spec = table
                .metadata()
                .partition_spec_by_id(manifest_file.partition_spec_id)
                .ok_or_else(|| {
                    invalid_data!(
                        "Manifest references unknown partition spec {}",
                        manifest_file.partition_spec_id,
                    )
                })?
                .clone();
            // The rewritten manifest keeps the schema it was written with, so its stored
            // partition values keep fitting their types after a promotion or a dropped column.
            let schema = manifest.metadata().schema.clone();

            // Rewrite: keep surviving entries as EXISTING, and record the
            // removed ones as DELETED so that the file they name can be
            // cleaned up when this snapshot expires.
            let mut surviving_entries: Vec<ManifestEntry> = Vec::new();
            let mut removed_entries: Vec<ManifestEntry> = Vec::new();
            for entry in manifest.entries() {
                if is_removed(entry) {
                    // Record removal metrics.
                    found_files.insert(RemovedFileKey::new(
                        manifest_file.content,
                        entry.data_file(),
                    ));
                    removed_collector.remove_file(
                        entry.data_file(),
                        schema.clone(),
                        manifest_spec.clone(),
                    );
                    removed_entries.push(entry.as_ref().clone());
                } else if entry.is_alive() {
                    // Surviving entry — re-emit as EXISTING with original ids preserved.
                    let seq = entry.sequence_number().ok_or_else(|| {
                        invalid_data!(
                            "Manifest entry for {} is missing sequence_number",
                            entry.file_path(),
                        )
                    })?;
                    let file_seq = entry.file_sequence_number.ok_or_else(|| {
                        invalid_data!(
                            "Manifest entry for {} is missing file_sequence_number",
                            entry.file_path(),
                        )
                    })?;
                    let existing = ManifestEntry::builder()
                        .status(ManifestStatus::Existing)
                        .snapshot_id(
                            entry
                                .snapshot_id()
                                .unwrap_or(manifest_file.added_snapshot_id),
                        )
                        .sequence_number(seq)
                        .file_sequence_number(file_seq)
                        .data_file(entry.data_file().clone())
                        .build();
                    surviving_entries.push(existing);
                }
                // Already-deleted entries (status == Deleted) are dropped.
            }

            if surviving_entries.is_empty() && removed_entries.is_empty() {
                // Nothing left to say about this manifest — omit entirely.
                continue;
            }

            // Write the filtered manifest using the manifest's own partition spec.
            let new_manifest_path = format!(
                "{}/{}-m{}.{}",
                table.metadata().metadata_location()?,
                commit_uuid,
                manifest_counter.fetch_add(1, Ordering::Relaxed),
                DataFileFormat::Avro,
            );
            let output_file = table.file_io().new_output(new_manifest_path)?;
            // Use the current snapshot_id so that the manifest list writer
            // can assign sequence numbers to this rewritten manifest.
            let mut writer = new_manifest_writer(
                table,
                output_file,
                Some(snapshot_id),
                manifest_file.content,
                schema.clone(),
                manifest_spec.as_ref().clone(),
            )?;
            for entry in surviving_entries {
                writer.add_existing_entry(entry)?;
            }
            for entry in removed_entries {
                // `add_delete_entry` stamps the removing snapshot on the entry
                // and keeps the file's own sequence numbers, which is what an
                // expiring reader needs to attribute the removal.
                writer.add_delete_entry(entry)?;
            }
            let new_manifest = writer.write_manifest_file().await?;
            result.push(new_manifest);
        }

        // Validate that every delete target was found.
        if self.fail_missing_delete_paths {
            let missing: Vec<&str> = self
                .deleted_files
                .difference(&found_files)
                .map(|key| key.path.as_str())
                .collect();
            if !missing.is_empty() {
                return Err(invalid_data!(
                    "Failed to find the following files to delete in the current snapshot: {}",
                    missing.join(", "),
                ));
            }
        }

        Ok((result, removed_collector))
    }
}

/// Shared engine for operations that both add and remove files.
///
/// This struct is the Rust equivalent of Java's `MergingSnapshotProducer`.
/// It manages:
/// - Tracking files to add and delete
/// - Filtering existing manifests to remove deleted files
/// - Writing new manifests for added files
/// - Computing snapshot summaries that account for both additions and removals
/// - Delegating final snapshot creation to [`SnapshotProducer`]
///
/// Concrete transaction actions like [`RewriteFilesAction`] own an instance
/// of this struct and configure it with the appropriate [`Operation`] type
/// and validation rules.
pub(crate) struct MergingSnapshotProducer {
    operation: Operation,
    added_data_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
    deleted_delete_files: Vec<DataFile>,
    filter_manager: ManifestFilterManager,
    commit_uuid: Uuid,
    /// Names the manifests this producer writes. It is never reset, so a
    /// commit retry cannot overwrite a manifest an earlier attempt wrote
    /// under the same commit uuid.
    manifest_counter: AtomicU64,
}

impl MergingSnapshotProducer {
    pub(crate) fn new(operation: Operation) -> Self {
        Self {
            operation,
            added_data_files: Vec::new(),
            deleted_data_files: Vec::new(),
            deleted_delete_files: Vec::new(),
            filter_manager: ManifestFilterManager::new(true),
            commit_uuid: Uuid::now_v7(),
            manifest_counter: AtomicU64::new(0),
        }
    }

    pub(crate) fn add_data_file(&mut self, file: DataFile) {
        self.added_data_files.push(file);
    }

    pub(crate) fn delete_data_file(&mut self, file: DataFile) {
        self.filter_manager.delete_data_file(&file);
        self.deleted_data_files.push(file);
    }

    pub(crate) fn delete_delete_file(&mut self, file: DataFile) {
        self.filter_manager.delete_delete_file(&file);
        self.deleted_delete_files.push(file);
    }

    pub(crate) fn added_data_files(&self) -> &[DataFile] {
        &self.added_data_files
    }

    pub(crate) fn deleted_data_files(&self) -> &[DataFile] {
        &self.deleted_data_files
    }

    pub(crate) fn deleted_delete_files(&self) -> &[DataFile] {
        &self.deleted_delete_files
    }

    /// Reject a data sequence number above the one the new snapshot will carry.
    fn validate_data_sequence_number(
        table: &Table,
        data_sequence_number: Option<i64>,
    ) -> Result<()> {
        let Some(sequence_number) = data_sequence_number else {
            return Ok(());
        };
        let snapshot_sequence_number = table.metadata().next_sequence_number();
        if sequence_number > snapshot_sequence_number {
            return Err(invalid_data!(
                "Data sequence number {sequence_number} is greater than the snapshot's {snapshot_sequence_number}."
            ));
        }
        Ok(())
    }

    /// Produce manifests, compute summary, and commit a new snapshot.
    ///
    /// Added files get `data_sequence_number` if set, and otherwise inherit
    /// the new snapshot's.
    pub(crate) async fn commit_snapshot(
        &self,
        table: &Table,
        data_sequence_number: Option<i64>,
    ) -> Result<ActionCommit> {
        Self::validate_data_sequence_number(table, data_sequence_number)?;
        // Create the SnapshotProducer first so we can use its snapshot_id
        // for new manifests. This ensures the manifest list writer can
        // assign sequence numbers to manifests from this snapshot.
        let snapshot_producer = SnapshotProducer::builder()
            .with_table(table)
            .with_commit_uuid(self.commit_uuid)
            .with_snapshot_properties(HashMap::new())
            .with_added_data_files(self.added_data_files.clone())
            .build();
        snapshot_producer.validate_added_data_files()?;
        snapshot_producer.validate_duplicate_files().await?;
        let snapshot_id = snapshot_producer.snapshot_id;

        // 1. Load existing manifests from the current snapshot.
        let existing_manifests = match table.metadata().current_snapshot() {
            Some(snapshot) => {
                let manifest_list = table.manifest_list_reader(snapshot).load().await?;
                manifest_list
                    .entries()
                    .iter()
                    // Java's third arm is the new snapshot's id, which no existing manifest has.
                    .filter(|e| e.has_added_files() || e.has_existing_files())
                    .cloned()
                    .collect()
            }
            None => Vec::new(),
        };

        // 2. Filter existing manifests — remove deleted file entries.
        let (mut filtered_manifests, removed_collector) = self
            .filter_manager
            .filter_manifests(
                table,
                existing_manifests,
                snapshot_id,
                self.commit_uuid,
                &self.manifest_counter,
            )
            .await?;

        // 3. Write a new manifest for added files.
        if !self.added_data_files.is_empty() {
            let added_manifest = self
                .write_added_manifest(
                    table,
                    snapshot_id,
                    data_sequence_number,
                    &self.manifest_counter,
                )
                .await?;
            filtered_manifests.push(added_manifest);
        }

        // 4. Compute summary (added + removed).
        let summary = self.build_summary(table, removed_collector)?;

        // 5. Delegate to SnapshotProducer for manifest list + snapshot creation.
        snapshot_producer
            .commit_with_manifests(filtered_manifests, summary)
            .await
    }

    async fn write_added_manifest(
        &self,
        table: &Table,
        snapshot_id: i64,
        data_sequence_number: Option<i64>,
        manifest_counter: &AtomicU64,
    ) -> Result<ManifestFile> {
        let new_manifest_path = format!(
            "{}/{}-m{}.{}",
            table.metadata().metadata_location()?,
            self.commit_uuid,
            manifest_counter.fetch_add(1, Ordering::Relaxed),
            DataFileFormat::Avro,
        );
        let output_file = table.file_io().new_output(new_manifest_path)?;
        let schema = table.metadata().current_schema().clone();
        let partition_spec = table.metadata().default_partition_spec().as_ref().clone();

        let mut writer = new_manifest_writer(
            table,
            output_file,
            Some(snapshot_id),
            ManifestContentType::Data,
            schema,
            partition_spec,
        )?;

        let format_version = table.metadata().format_version();
        for data_file in &self.added_data_files {
            if let Some(sequence_number) = data_sequence_number {
                writer.add_file(data_file.clone(), sequence_number)?;
                continue;
            }
            let entry_builder = ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .data_file(data_file.clone());
            let entry = if format_version == FormatVersion::V1 {
                // V1 requires snapshot_id on each entry.
                entry_builder.snapshot_id(snapshot_id).build()
            } else {
                entry_builder.build()
            };
            writer.add_entry(entry)?;
        }

        writer.write_manifest_file().await
    }

    fn build_summary(
        &self,
        table: &Table,
        removed_collector: SnapshotSummaryCollector,
    ) -> Result<Summary> {
        let table_metadata = table.metadata_ref();
        let schema = table_metadata.current_schema().clone();
        let partition_spec = table_metadata.default_partition_spec().clone();

        // Add to the filter's collector rather than merging two: `merge` drops
        // partition metrics unless both sides trust them, and neither does.
        let mut collector = removed_collector;
        collector.set_partition_summary_limit(
            table_metadata
                .properties()
                .get(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT)
                .and_then(|limit| limit.parse::<u64>().ok())
                .unwrap_or(TableProperties::PROPERTY_WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT),
        );
        for file in &self.added_data_files {
            collector.add_file(file, schema.clone(), partition_spec.clone());
        }

        let summary = Summary {
            operation: self.operation.clone(),
            additional_properties: collector.build(),
        };

        let previous_snapshot = table_metadata.current_snapshot();
        update_snapshot_summaries(summary, previous_snapshot.map(|s| s.summary()), false)
    }
}
