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

//! Transaction action for rewriting data files (compaction).
//!
//! [`RewriteFilesAction`] replaces a set of data files with a new set while
//! keeping the logical table contents unchanged. This is used for compaction —
//! merging many small files into fewer large ones — and for removing delete
//! files that no longer apply to any live data file.
//!
//! The resulting snapshot uses [`Operation::Replace`] to indicate that files
//! were reorganised without changing the data.

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::{Result, invalid_data};
use crate::spec::{DataContentType, DataFile, Literal, ManifestContentType, Operation, Struct};
use crate::table::Table;
use crate::transaction::merging::MergingSnapshotProducer;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::util::snapshot::ancestors_of;

/// A transaction action that rewrites (replaces) data files and removes
/// delete files.
///
/// This is the Rust equivalent of Java's `BaseRewriteFiles`. It uses
/// `MergingSnapshotProducer` to handle manifest filtering and creation,
/// and commits a snapshot with [`Operation::Replace`].
///
/// # Example
///
/// ```ignore
/// let tx = Transaction::new(&table);
/// let action = tx.rewrite_files()
///     .delete_file(old_file_1)
///     .delete_file(old_file_2)
///     .add_file(merged_file);
/// let tx = action.apply(tx)?;
/// let table = tx.commit(&catalog).await?;
/// ```
///
/// # Concurrent deletes
///
/// The action records the table's current snapshot when it is created; a
/// delete committed after that snapshot is new. The commit is refused with
/// [`ErrorKind::DataInvalid`](crate::ErrorKind::DataInvalid) in these cases:
///
/// 1. a new position delete or deletion vector names a replaced file;
/// 2. a new partition-scoped position delete is in a replaced file's partition;
/// 3. a new equality delete is in a replaced file's partition, or in an
///    unpartitioned spec. It is skipped when
///    [`data_sequence_number`](RewriteFilesAction::data_sequence_number) is
///    set, the delete is newer than that number, and the delete is in an
///    unpartitioned spec or every added file is in its spec, because the
///    delete then still applies to the added files. A delete whose spec is
///    not in the table metadata is never skipped;
/// 4. always, with or without new deletes, when the rewrite was planned
///    against a table that had no snapshot yet, or its starting snapshot has
///    since been expired or is no longer an ancestor of the current one.
///
/// This follows Java's `validateNoNewDeletesForDataFiles`, but does not prune
/// by column bounds and refuses equality deletes whose added files left their
/// spec. A rewrite that only removes delete files is checked only for its
/// starting snapshot. Replan a refused rewrite against the current table and
/// run it again.
///
/// Deletes that were already committed when the rewrite was planned are not
/// covered by this check. Either apply them while rewriting, or keep them
/// applicable to the new file with
/// [`data_sequence_number`](RewriteFilesAction::data_sequence_number).
pub struct RewriteFilesAction {
    producer: MergingSnapshotProducer,
    /// The snapshot the rewrite was planned against, if the table had one.
    /// Deletes newer than it that can apply to a replaced file fail the commit.
    starting_snapshot_id: Option<i64>,
}

impl RewriteFilesAction {
    pub(crate) fn new(starting_snapshot_id: Option<i64>) -> Self {
        Self {
            producer: MergingSnapshotProducer::new(Operation::Replace),
            starting_snapshot_id,
        }
    }

    /// Register a data file to be removed from the table.
    ///
    /// The file must exist in the current snapshot; otherwise the commit
    /// will fail with a validation error. Remove delete files with
    /// [`delete_delete_file`](Self::delete_delete_file).
    pub fn delete_file(mut self, file: DataFile) -> Self {
        self.producer.delete_data_file(file);
        self
    }

    /// Register a delete file to be removed from the table: a position or
    /// equality delete file, or a deletion vector.
    ///
    /// The file must be live in the current snapshot; otherwise the commit
    /// will fail with a validation error. A deletion vector is matched by
    /// path, content offset and size, so other vectors in the same Puffin
    /// file stay.
    ///
    /// The action does not check that the delete still applies to no live
    /// data file. [`Transaction::commit`](crate::transaction::Transaction::commit)
    /// retries a conflict on the newer snapshot without re-checking. A caller
    /// that removes deletes because they no longer apply should commit the
    /// action's updates once and re-plan on conflict.
    pub fn delete_delete_file(mut self, file: DataFile) -> Self {
        self.producer.delete_delete_file(file);
        self
    }

    /// Register a data file to be added to the table.
    ///
    /// Typically this is the merged output of the files being deleted.
    pub fn add_file(mut self, file: DataFile) -> Self {
        self.producer.add_data_file(file);
        self
    }

    /// Set the data sequence number recorded for every added file.
    ///
    /// Without this the added files inherit the sequence number of the new
    /// snapshot. A compaction that must not shadow concurrently written
    /// deletes sets the sequence number of the snapshot it was planned
    /// against.
    /// V1 manifest entries carry no sequence number, so this has no effect on
    /// a V1 table.
    pub fn data_sequence_number(mut self, sequence_number: i64) -> Self {
        self.producer.set_data_sequence_number(sequence_number);
        self
    }

    fn validate(&self) -> Result<()> {
        let deleted_data_files = self.producer.deleted_data_files();
        let deleted_delete_files = self.producer.deleted_delete_files();
        if let Some(file) = deleted_data_files
            .iter()
            .find(|file| file.content_type() != DataContentType::Data)
        {
            return Err(invalid_data!(
                "Cannot rewrite files: {} is a {:?} file, remove it with delete_delete_file",
                file.file_path,
                file.content_type()
            ));
        }
        if let Some(file) = deleted_delete_files
            .iter()
            .find(|file| file.content_type() == DataContentType::Data)
        {
            return Err(invalid_data!(
                "Cannot rewrite files: {} is a data file, remove it with delete_file",
                file.file_path
            ));
        }
        if deleted_data_files.is_empty() && deleted_delete_files.is_empty() {
            return Err(invalid_data!(
                "Rewrite files requires at least one file to delete"
            ));
        }
        // As in Java: data added by a `replace` snapshot that replaces no data
        // would be skipped by incremental readers.
        if deleted_data_files.is_empty() && !self.producer.added_data_files().is_empty() {
            return Err(invalid_data!(
                "Data files to add must be empty because there's no data file to be rewritten"
            ));
        }
        Ok(())
    }

    /// Reject the rewrite when a delete committed after the snapshot it was
    /// planned against can apply to a replaced data file.
    async fn validate_no_new_deletes(&self, table: &Table) -> Result<()> {
        // Nothing has been committed, so there is nothing to conflict with.
        // The filter rejects the rewrite for its missing sources instead.
        let Some(current_snapshot) = table.metadata().current_snapshot() else {
            return Ok(());
        };
        // The rewrite was planned against a table without a snapshot, so every
        // snapshot it has now, deletes included, landed after that.
        let Some(starting_snapshot_id) = self.starting_snapshot_id else {
            return Err(invalid_data!(
                "Cannot rewrite files: the rewrite was planned against a table with no snapshot, and the table has been committed to since."
            ));
        };
        let Some(starting_snapshot) = table.metadata().snapshot_by_id(starting_snapshot_id) else {
            // Without the starting snapshot there is no sequence number to
            // compare against, so no delete can be ruled out.
            return Err(invalid_data!(
                "Cannot rewrite files: the starting snapshot {starting_snapshot_id} is no longer in the table, so deletes added since it cannot be ruled out."
            ));
        };
        // Sequence numbers tell new deletes from old ones only along the
        // starting snapshot's own history.
        if !ancestors_of(&table.metadata_ref(), current_snapshot.snapshot_id())
            .any(|snapshot| snapshot.snapshot_id() == starting_snapshot_id)
        {
            return Err(invalid_data!(
                "Cannot rewrite files: the starting snapshot {starting_snapshot_id} is not an ancestor of the current snapshot {}, so deletes added since it cannot be ruled out.",
                current_snapshot.snapshot_id()
            ));
        }
        // A rewrite that replaces no data file cannot lose a delete.
        if self.producer.deleted_data_files().is_empty() {
            return Ok(());
        }
        let starting_sequence_number = starting_snapshot.sequence_number();

        // A delete committed after the start has a data sequence number above
        // the start's, in a manifest written after the start. It stays so when
        // a later snapshot rewrites that manifest and lists it as `Existing`.
        let manifest_list = table.manifest_list_reader(current_snapshot).load().await?;
        let new_delete_manifests = manifest_list.entries().iter().filter(|manifest_file| {
            manifest_file.content == ManifestContentType::Deletes
                && manifest_file.sequence_number > starting_sequence_number
        });
        for manifest_file in new_delete_manifests {
            let manifest = table.manifest_reader().read(manifest_file).await?;
            let new_deletes = manifest.entries().iter().filter(|entry| {
                entry.is_alive()
                    && entry
                        .sequence_number()
                        .is_none_or(|sequence_number| sequence_number > starting_sequence_number)
            });
            for entry in new_deletes {
                let delete = entry.data_file();
                let delete_sequence_number = entry
                    .sequence_number()
                    .unwrap_or(manifest_file.sequence_number);
                if let Some(replaced) =
                    self.replaced_file_affected_by(table, delete, delete_sequence_number)
                {
                    return Err(invalid_data!(
                        "Cannot rewrite files: the {:?} file {} was added after the starting snapshot {starting_snapshot_id} and can apply to the replaced data file {}.",
                        delete.content_type(),
                        delete.file_path,
                        replaced.file_path
                    ));
                }
            }
        }
        Ok(())
    }

    /// The first replaced data file that `delete` can apply to.
    fn replaced_file_affected_by(
        &self,
        table: &Table,
        delete: &DataFile,
        delete_sequence_number: i64,
    ) -> Option<&DataFile> {
        let mut replaced = self.producer.deleted_data_files().iter();
        let same_partition = |file: &&DataFile| {
            file.partition_spec_id == delete.partition_spec_id
                && may_be_same_partition(&file.partition, &delete.partition)
        };
        match delete.content_type() {
            DataContentType::EqualityDeletes => {
                // Without its spec the delete's scope is unknown.
                let Some(spec) = table
                    .metadata()
                    .partition_spec_by_id(delete.partition_spec_id)
                else {
                    return replaced.next();
                };
                let global = spec.is_unpartitioned();
                // The delete still applies to the added files when they are
                // older than it and in its spec.
                let applies_to_added = self
                    .producer
                    .data_sequence_number()
                    .is_some_and(|sequence_number| delete_sequence_number > sequence_number)
                    && (global
                        || self
                            .producer
                            .added_data_files()
                            .iter()
                            .all(|file| file.partition_spec_id == delete.partition_spec_id));
                if applies_to_added {
                    return None;
                }
                replaced.find(|file| global || same_partition(file))
            }
            DataContentType::PositionDeletes => match delete.single_referenced_data_file() {
                Some(path) => replaced.find(|file| file.file_path == path),
                None => replaced.find(same_partition),
            },
            DataContentType::Data => None,
        }
    }
}

/// False only when the partition tuples certainly differ. Values of different
/// types (int to long, float to double promotion) count as equal.
fn may_be_same_partition(a: &Struct, b: &Struct) -> bool {
    a.fields().len() != b.fields().len()
        || a.iter().zip(b.iter()).all(|pair| match pair {
            (None, None) => true,
            (None, Some(_)) | (Some(_), None) => false,
            (Some(Literal::Primitive(x)), Some(Literal::Primitive(y))) => {
                std::mem::discriminant(x) != std::mem::discriminant(y) || x == y
            }
            (Some(_), Some(_)) => true,
        })
}

#[async_trait]
impl TransactionAction for RewriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        self.validate()?;
        self.validate_no_new_deletes(table).await?;
        self.producer.commit_snapshot(table).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use uuid::Uuid;

    use super::*;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, FormatVersion, Literal,
        MAIN_BRANCH, ManifestEntry, ManifestEntryRef, ManifestFile, ManifestListWriter,
        ManifestStatus, ManifestWriterBuilder, NestedField, Operation, PrimitiveType, Schema,
        SnapshotRef, SnapshotReference, SnapshotRetention, Struct, Transform, Type,
        UnboundPartitionField, UnboundPartitionSpec,
    };
    use crate::table::Table;
    use crate::transaction::snapshot::{
        DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
    };
    use crate::transaction::tests::{
        append_files, make_data_file, make_v1_minimal_table_in_catalog,
        make_v2_minimal_table_in_catalog, make_v3_minimal_table_in_catalog,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, ErrorKind, NamespaceIdent, TableCommit, TableCreation, TableUpdate};

    /// Read back the manifest entry for `path` from the manifests of `snapshot`.
    async fn find_entry(
        table: &Table,
        snapshot: &SnapshotRef,
        path: &str,
    ) -> Option<ManifestEntryRef> {
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            if let Some(entry) = manifest.entries().iter().find(|e| e.file_path() == path) {
                return Some(entry.clone());
            }
        }
        None
    }

    /// E2E: Compact 3 small files into 1 merged file.
    /// Verify: operation=Replace, file counts, record counts.
    #[tokio::test]
    async fn test_rewrite_files_compaction() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        rewrite_files_compaction(&catalog, table).await;
    }

    #[tokio::test]
    async fn test_rewrite_files_compaction_on_v1_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v1_minimal_table_in_catalog(&catalog).await;
        rewrite_files_compaction(&catalog, table).await;
    }

    #[tokio::test]
    async fn test_rewrite_files_compaction_on_v2_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        rewrite_files_compaction(&catalog, table).await;
    }

    async fn rewrite_files_compaction(catalog: &impl Catalog, table: Table) {
        // Append 3 small files (10 records each).
        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let f3 = make_data_file(&table, "test/3.parquet", 10, 100);
        let table = append_files(catalog, &table, vec![f1.clone(), f2.clone(), f3.clone()]).await;

        // Verify pre-compaction state.
        let summary = &table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties;
        assert_eq!(summary.get("total-data-files").unwrap(), "3");
        assert_eq!(summary.get("total-records").unwrap(), "30");

        // Rewrite: delete 3 files, add 1 merged file (30 records).
        let merged = make_data_file(&table, "test/merged.parquet", 30, 300);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_file(f1)
            .delete_file(f2)
            .delete_file(f3)
            .add_file(merged);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(catalog).await.unwrap();

        // Verify post-compaction snapshot.
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Replace);

        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("total-data-files").unwrap(), "1");
        assert_eq!(summary.get("total-records").unwrap(), "30");
        assert_eq!(summary.get("added-data-files").unwrap(), "1");
        assert_eq!(summary.get("deleted-data-files").unwrap(), "3");

        // Verify manifest list: merged file is the only live file.
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        let mut live_files: Vec<String> = Vec::new();
        for manifest_entry in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_entry).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() {
                    live_files.push(entry.file_path().to_string());
                }
            }
        }
        assert_eq!(live_files, vec!["test/merged.parquet"]);
    }

    /// Rewrite with a non-existent delete target should fail.
    #[tokio::test]
    async fn test_rewrite_files_missing_delete_target() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append 1 file.
        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1]).await;

        // Try to delete a file that doesn't exist.
        let ghost = make_data_file(&table, "test/ghost.parquet", 10, 100);
        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(ghost).add_file(merged);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.message()
                .contains("Failed to find the following files to delete"),
            "unexpected error: {}",
            err.message()
        );
    }

    /// Rewrite with no deletes should fail validation.
    #[tokio::test]
    async fn test_rewrite_files_no_deletes() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().add_file(merged);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("at least one file to delete")
        );
    }

    /// Partial rewrite: delete 2 of 3 files, keeping one.
    /// Verifies that the rewritten manifest for the surviving file
    /// has the correct snapshot_id for sequence number assignment.
    #[tokio::test]
    async fn test_rewrite_files_partial() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        rewrite_files_partial(&catalog, table).await;
    }

    #[tokio::test]
    async fn test_rewrite_files_partial_on_v1_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v1_minimal_table_in_catalog(&catalog).await;
        rewrite_files_partial(&catalog, table).await;
    }

    #[tokio::test]
    async fn test_rewrite_files_partial_on_v2_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        rewrite_files_partial(&catalog, table).await;
    }

    async fn rewrite_files_partial(catalog: &impl Catalog, table: Table) {
        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let f3 = make_data_file(&table, "test/3.parquet", 10, 100);
        let table = append_files(catalog, &table, vec![f1.clone(), f2.clone(), f3.clone()]).await;
        let append_snapshot = table.metadata().current_snapshot().unwrap().clone();

        // Rewrite only f1 and f2, keep f3.
        let merged = make_data_file(&table, "test/merged.parquet", 20, 200);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_file(f1)
            .delete_file(f2)
            .add_file(merged);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Replace);

        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("total-data-files").unwrap(), "2");
        assert_eq!(summary.get("total-records").unwrap(), "30");
        assert_eq!(summary.get("deleted-data-files").unwrap(), "2");
        assert_eq!(summary.get("added-data-files").unwrap(), "1");

        // Verify live files: f3 (surviving) + merged (new).
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        let mut live_files: Vec<String> = Vec::new();
        for manifest_entry in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_entry).await.unwrap();
            for entry in manifest.entries() {
                if entry.is_alive() {
                    live_files.push(entry.file_path().to_string());
                }
            }
        }
        live_files.sort();
        assert_eq!(live_files, vec!["test/3.parquet", "test/merged.parquet"]);

        let survivor = find_entry(&table, snapshot, "test/3.parquet")
            .await
            .expect("surviving file should still be in a manifest");
        assert_eq!(survivor.status(), ManifestStatus::Existing);
        assert_eq!(survivor.snapshot_id(), Some(append_snapshot.snapshot_id()));
        assert_eq!(
            survivor.sequence_number(),
            Some(append_snapshot.sequence_number())
        );
        assert_eq!(
            survivor.file_sequence_number,
            Some(append_snapshot.sequence_number())
        );
    }

    /// An explicit data sequence number is recorded on the added file instead
    /// of the one it would inherit from the rewrite snapshot.
    #[tokio::test]
    async fn test_rewrite_files_applies_data_sequence_number() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;
        let append_sequence_number = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_file(f1)
            .add_file(merged)
            .data_sequence_number(append_sequence_number);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_ne!(snapshot.sequence_number(), append_sequence_number);

        let merged_entry = find_entry(&table, snapshot, "test/merged.parquet")
            .await
            .expect("added file should be in a manifest");
        assert_eq!(merged_entry.sequence_number(), Some(append_sequence_number));
    }

    /// A data sequence number above the one the new snapshot will carry is
    /// rejected.
    #[tokio::test]
    async fn test_rewrite_files_rejects_data_sequence_number_above_the_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;
        let snapshot_sequence_number = table.metadata().next_sequence_number();

        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_file(f1)
            .add_file(merged)
            .data_sequence_number(snapshot_sequence_number + 1);
        let tx = action.apply(tx).unwrap();
        let err = tx.commit(&catalog).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string().contains(&format!(
                "Data sequence number {} is greater than the snapshot's {snapshot_sequence_number}.",
                snapshot_sequence_number + 1
            )),
            "{err}"
        );
    }

    /// Rewrite on an empty table (no snapshot) should fail because
    /// the delete target doesn't exist.
    #[tokio::test]
    async fn test_rewrite_files_empty_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(f1).add_file(merged);
        let tx = action.apply(tx).unwrap();
        let result = tx.commit(&catalog).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("Failed to find the following files to delete"),
        );
    }

    /// Rewrite where all files in a manifest are deleted should omit
    /// the manifest entirely (not leave an empty one).
    #[tokio::test]
    async fn test_rewrite_files_removes_empty_manifest() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Append 1 file → 1 manifest.
        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;

        // Rewrite: delete f1, add merged. f1's manifest keeps no live entry,
        // but it still has to record the removal for this snapshot.
        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(f1.clone()).add_file(merged);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let emptied = find_entry(&table, snapshot, "test/1.parquet")
            .await
            .expect("the removal has to be recorded somewhere");
        assert_eq!(emptied.status(), ManifestStatus::Deleted);
        assert_eq!(emptied.snapshot_id(), Some(snapshot.snapshot_id()));
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        assert_eq!(manifest_list.entries().len(), 2);
        assert!(
            manifest_list
                .entries()
                .iter()
                .any(|m| !m.has_added_files() && !m.has_existing_files()),
            "the emptied manifest holds only the deleted entry"
        );

        // A later rewrite drops it: it has nothing live, and no removal of its
        // own left to record. An append would carry it along, as in Java,
        // where only a merging commit rebuilds the manifest list.
        let merged = find_entry(&table, snapshot, "test/merged.parquet")
            .await
            .unwrap()
            .data_file()
            .clone();
        let merged_again = make_data_file(&table, "test/merged-2.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx
            .rewrite_files()
            .delete_file(merged)
            .add_file(merged_again);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert!(
            find_entry(&table, snapshot, "test/1.parquet")
                .await
                .is_none(),
            "an all-deleted manifest should not survive a later rewrite"
        );
    }

    /// A manifest left with only deleted entries records nothing live, whether
    /// the current snapshot or an older one left it that way.
    #[tokio::test]
    async fn test_rewrite_files_drops_all_deleted_manifests() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;
        let table = append_files(&catalog, &table, vec![f2]).await;
        let current_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();
        let earlier_snapshot_id = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .parent_snapshot_id()
            .unwrap();
        let older = add_delete_only_manifest(&table, earlier_snapshot_id).await;
        let current = add_delete_only_manifest(&table, current_snapshot_id).await;

        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(f1).add_file(merged);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        for path in [older, current] {
            assert!(
                !manifest_list
                    .entries()
                    .iter()
                    .any(|m| m.manifest_path == path),
                "{path} holds no live entry and should have been dropped"
            );
        }
    }

    /// Add a delete-only manifest, attributed to `added_snapshot_id`, to the
    /// manifest list of the table's current snapshot, and return its path.
    async fn add_delete_only_manifest(table: &Table, added_snapshot_id: i64) -> String {
        let added_sequence_number = table
            .metadata()
            .snapshot_by_id(added_snapshot_id)
            .unwrap()
            .sequence_number();
        let output = table
            .file_io()
            .new_output(format!(
                "{}/delete-only-{}.avro",
                table.metadata().metadata_location().unwrap(),
                Uuid::new_v4()
            ))
            .unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output,
            Some(added_snapshot_id),
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
        )
        .build_v3_data();
        writer
            .add_delete_file(
                make_data_file(
                    table,
                    &format!("test/removed-{added_snapshot_id}.parquet"),
                    10,
                    100,
                ),
                added_sequence_number,
                Some(added_sequence_number),
            )
            .unwrap();
        let mut delete_manifest = writer.write_manifest_file().await.unwrap();
        // The manifest list writer only assigns sequence numbers to manifests of
        // the snapshot being written; this one belongs to an earlier snapshot.
        delete_manifest.sequence_number = added_sequence_number;
        delete_manifest.min_sequence_number = added_sequence_number;
        let delete_manifest_path = delete_manifest.manifest_path.clone();
        assert!(!delete_manifest.has_added_files());
        assert!(!delete_manifest.has_existing_files());

        append_to_manifest_list(table, delete_manifest).await;
        delete_manifest_path
    }

    /// Rewrite the manifest list of the table's current snapshot so it also
    /// contains `manifest`.
    async fn append_to_manifest_list(table: &Table, manifest: ManifestFile) {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let mut entries: Vec<ManifestFile> = table
            .manifest_list_reader(snapshot)
            .load()
            .await
            .unwrap()
            .consume_entries()
            .into_iter()
            .collect();
        entries.push(manifest);

        let mut manifest_list_writer = ManifestListWriter::v3(
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
            Some(table.metadata().next_row_id()),
        );
        manifest_list_writer
            .add_manifests(entries.into_iter())
            .unwrap();
        manifest_list_writer.close().await.unwrap();
    }

    /// Sequence numbers order deletes only along one history, so a starting
    /// snapshot that a rollback took off the branch rules nothing out.
    #[tokio::test]
    async fn test_rewrite_files_rejects_starting_snapshot_off_the_branch() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;
        let rolled_back_to = table.metadata().current_snapshot().unwrap().snapshot_id();
        let table = append_files(&catalog, &table, vec![f2]).await;
        let starting_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        let tx = Transaction::new(&table);
        let action = Arc::new(tx.rewrite_files().delete_file(f1));

        let rollback = TableCommit::builder()
            .ident(table.identifier().clone())
            .requirements(vec![])
            .updates(vec![TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    rolled_back_to,
                    SnapshotRetention::branch(None, None, None),
                ),
            }])
            .build();
        let table = catalog.update_table(rollback).await.unwrap();

        let Err(err) = action.commit(&table).await else {
            panic!("a starting snapshot that is not an ancestor must be rejected");
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.message(),
            format!(
                "Cannot rewrite files: the starting snapshot {starting_snapshot_id} is not an ancestor of the current snapshot {rolled_back_to}, so deletes added since it cannot be ruled out."
            )
        );
    }

    /// A rewrite planned against a table with no snapshot knows nothing about
    /// what the table holds once something is committed to it.
    #[tokio::test]
    async fn test_rewrite_files_rejects_commit_after_empty_starting_table() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Planned before the table had any snapshot.
        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = Arc::new(tx.rewrite_files().delete_file(f1.clone()).add_file(merged));

        // The file it wants to replace only exists because of a later commit.
        let table = append_files(&catalog, &table, vec![f1]).await;

        let Err(err) = action.commit(&table).await else {
            panic!("a rewrite planned against an empty table must be rejected");
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.message(),
            "Cannot rewrite files: the rewrite was planned against a table with no snapshot, and the table has been committed to since."
        );
    }

    /// A rewrite whose starting snapshot has been expired cannot rule out
    /// deletes added since, and says so rather than comparing against zero.
    #[tokio::test]
    async fn test_rewrite_files_rejects_expired_starting_snapshot() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;
        let starting_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // Plan the rewrite against this snapshot, then expire it behind us.
        let tx = Transaction::new(&table);
        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let action = Arc::new(tx.rewrite_files().delete_file(f1).add_file(merged));

        let table = append_files(&catalog, &table, vec![f2]).await;
        let tx = Transaction::new(&table);
        let tx = tx
            .expire_snapshots()
            .expire_snapshot_ids([starting_snapshot_id])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();
        assert!(
            table
                .metadata()
                .snapshot_by_id(starting_snapshot_id)
                .is_none(),
            "the starting snapshot should have been expired"
        );

        let Err(err) = action.commit(&table).await else {
            panic!("an expired starting snapshot must be rejected");
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.message(),
            format!(
                "Cannot rewrite files: the starting snapshot {starting_snapshot_id} is no longer in the table, so deletes added since it cannot be ruled out."
            )
        );
    }

    /// A retried commit must not reuse the manifest paths of the attempt
    /// before it: the commit uuid is fixed, so only the counter separates them.
    #[tokio::test]
    async fn test_rewrite_files_writes_distinct_manifests_per_attempt() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone(), f2.clone()]).await;

        let merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        let tx = Transaction::new(&table);
        let action = Arc::new(tx.rewrite_files().delete_file(f1).add_file(merged));

        let first = manifest_paths(&table, Arc::clone(&action)).await;
        let second = manifest_paths(&table, action).await;

        assert_eq!(first.len(), 2, "{first:?}");
        assert_eq!(first.len(), second.len());
        for path in &first {
            assert!(!second.contains(path), "{path} was written twice");
        }
    }

    /// Commit `action` against `table` and return the paths of the manifests
    /// the resulting snapshot points at.
    async fn manifest_paths(table: &Table, action: Arc<RewriteFilesAction>) -> Vec<String> {
        let mut commit = action.commit(table).await.unwrap();
        let snapshot = commit
            .take_updates()
            .into_iter()
            .find_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                _ => None,
            })
            .expect("commit should add a snapshot");

        let manifest_list = table
            .manifest_list_reader(&Arc::new(snapshot))
            .load()
            .await
            .unwrap();
        manifest_list
            .entries()
            .iter()
            .map(|entry| entry.manifest_path.clone())
            .collect()
    }

    /// An added file whose partition value does not fit the default spec is
    /// rejected before anything is written.
    #[tokio::test]
    async fn test_rewrite_files_rejects_incompatible_partition_value() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;

        let mut merged = make_data_file(&table, "test/merged.parquet", 10, 100);
        merged.partition = Struct::from_iter([Some(Literal::string("not-a-long"))]);
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(f1).add_file(merged);
        let tx = action.apply(tx).unwrap();
        let err = tx.commit(&catalog).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string()
                .contains("Partition value is not compatible partition type"),
            "{err}"
        );
    }

    /// A delete file handed to `add_file` is rejected: a rewrite replaces data
    /// files, and the producer validation names the offending file.
    #[tokio::test]
    async fn test_rewrite_files_rejects_non_data_content_type() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone()]).await;

        let mut deletes = make_data_file(&table, "test/1-deletes.parquet", 1, 100);
        deletes.content = DataContentType::PositionDeletes;
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(f1).add_file(deletes);
        let tx = action.apply(tx).unwrap();
        let err = tx.commit(&catalog).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string().contains(
                "Only data content type is allowed in added data files, but test/1-deletes.parquet is PositionDeletes"
            ),
            "{err}"
        );
    }

    /// An added file that is already live in the current snapshot is rejected.
    #[tokio::test]
    async fn test_rewrite_files_rejects_already_referenced_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        let f1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let f2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![f1.clone(), f2.clone()]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(f1).add_file(f2);
        let tx = action.apply(tx).unwrap();
        let err = tx.commit(&catalog).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.to_string().contains(
                "Cannot add files that are already referenced by table, files: test/2.parquet"
            ),
            "{err}"
        );
    }

    #[tokio::test]
    async fn test_rewrite_files_without_output_files() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let removed = make_data_file(&table, "test/removed.parquet", 10, 100);
        let kept = make_data_file(&table, "test/kept.parquet", 5, 50);
        let table = append_files(&catalog, &table, vec![removed.clone(), kept]).await;
        let tx = Transaction::new(&table);
        let table = tx
            .rewrite_files()
            .delete_file(removed)
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap();
        let snapshot = table.metadata().current_snapshot().unwrap();
        let summary = &snapshot.summary().additional_properties;
        assert_eq!(summary.get("deleted-data-files").unwrap(), "1");
        assert_eq!(summary.get("total-data-files").unwrap(), "1");
        assert_eq!(summary.get("total-records").unwrap(), "5");
        assert_eq!(summary.get("total-files-size").unwrap(), "50");
        assert_eq!(
            find_entry(&table, snapshot, "test/removed.parquet")
                .await
                .unwrap()
                .status(),
            ManifestStatus::Deleted
        );
        assert_eq!(
            find_entry(&table, snapshot, "test/kept.parquet")
                .await
                .unwrap()
                .status(),
            ManifestStatus::Existing
        );
    }

    // Partition-spec evolution: `ts` is identity-partitioned under spec 0, then
    // `day(ts)` becomes the default spec. Old files keep spec 0 and a
    // timestamptz partition value; new files carry spec 1 and a date.

    /// 2026-08-24T09:30:00Z and 2026-08-24T17:45:00Z, in microseconds.
    const TS_MORNING: i64 = 1_787_563_800_000_000;
    const TS_EVENING: i64 = 1_787_593_500_000_000;
    /// 2026-08-24, in days since the epoch.
    const DAY: i32 = 20_689;

    /// A table partitioned by `identity(ts)`, still on its first spec.
    async fn make_identity_ts_table(
        catalog: &impl Catalog,
        format_version: FormatVersion,
    ) -> Table {
        let namespace = NamespaceIdent::new(format!("ns1-{}", Uuid::new_v4()));
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "ts", Type::Primitive(PrimitiveType::Timestamptz)).into(),
            ])
            .build()
            .unwrap();
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(
                UnboundPartitionField::builder()
                    .source_ids(vec![2])
                    .name("ts")
                    .transform(Transform::Identity)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .build();
        let creation = TableCreation::builder()
            .name("events".to_string())
            .schema(schema)
            .partition_spec(spec)
            .format_version(format_version)
            .build();
        catalog.create_table(&namespace, creation).await.unwrap()
    }

    /// Replace `identity(ts)` with `day(ts)` as the default spec.
    async fn evolve_to_day_ts(catalog: &impl Catalog, table: &Table) -> Table {
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(
                UnboundPartitionField::builder()
                    .source_ids(vec![2])
                    .name("ts_day")
                    .transform(Transform::Day)
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .build();
        let commit = TableCommit::builder()
            .ident(table.identifier().clone())
            .requirements(vec![])
            .updates(vec![
                TableUpdate::AddSpec { spec },
                TableUpdate::SetDefaultSpec { spec_id: -1 },
            ])
            .build();
        let table = catalog.update_table(commit).await.unwrap();
        assert_eq!(table.metadata().default_partition_spec_id(), 1);
        table
    }

    fn make_file_in_spec(
        path: &str,
        records: u64,
        spec_id: i32,
        partition_value: Literal,
    ) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(records * 10)
            .record_count(records)
            .partition(Struct::from_iter([Some(partition_value)]))
            .partition_spec_id(spec_id)
            .build()
            .unwrap()
    }

    /// Live files of the current snapshot, keyed by path, with the spec id of
    /// the manifest that lists them.
    async fn live_files_by_manifest_spec(table: &Table) -> Vec<(String, i32)> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        let mut live = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            for entry in manifest.entries().iter().filter(|e| e.is_alive()) {
                assert_eq!(
                    entry.data_file().partition_spec_id,
                    manifest_file.partition_spec_id,
                    "{} is listed by a manifest of another spec",
                    entry.file_path()
                );
                live.push((
                    entry.file_path().to_string(),
                    manifest_file.partition_spec_id,
                ));
            }
        }
        live.sort();
        live
    }

    /// Two old-spec files and one new-spec file; returns the table and the
    /// snapshot that appended the old-spec files.
    async fn spec_evolved_table_with_files(
        catalog: &impl Catalog,
        format_version: FormatVersion,
    ) -> (Table, SnapshotRef, [DataFile; 3]) {
        let table = make_identity_ts_table(catalog, format_version).await;
        let old_1 = make_file_in_spec(
            "test/old-1.parquet",
            10,
            0,
            Literal::timestamptz(TS_MORNING),
        );
        let old_2 = make_file_in_spec(
            "test/old-2.parquet",
            10,
            0,
            Literal::timestamptz(TS_EVENING),
        );
        let table = append_files(catalog, &table, vec![old_1.clone(), old_2.clone()]).await;
        let old_snapshot = table.metadata().current_snapshot().unwrap().clone();

        let table = evolve_to_day_ts(catalog, &table).await;
        let new_1 = make_file_in_spec("test/new-1.parquet", 10, 1, Literal::date(DAY));
        let table = append_files(catalog, &table, vec![new_1.clone()]).await;
        (table, old_snapshot, [old_1, old_2, new_1])
    }

    /// Compacting one old-spec file keeps the other in a manifest written under
    /// spec 0: its entry, partition value and the manifest's partition summary
    /// are still those of `identity(ts)`, not re-encoded under `day(ts)`.
    #[tokio::test]
    async fn test_rewrite_files_keeps_survivors_on_their_partition_spec() {
        for format_version in [FormatVersion::V2, FormatVersion::V3] {
            let catalog = new_memory_catalog().await;
            let (table, old_snapshot, [old_1, _old_2, _new_1]) =
                spec_evolved_table_with_files(&catalog, format_version).await;

            let merged = make_file_in_spec("test/merged.parquet", 10, 1, Literal::date(DAY));
            let tx = Transaction::new(&table);
            let action = tx.rewrite_files().delete_file(old_1).add_file(merged);
            let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

            assert_eq!(
                live_files_by_manifest_spec(&table).await,
                vec![
                    ("test/merged.parquet".to_string(), 1),
                    ("test/new-1.parquet".to_string(), 1),
                    ("test/old-2.parquet".to_string(), 0),
                ],
                "{format_version}"
            );

            let snapshot = table.metadata().current_snapshot().unwrap();
            let survivor = find_entry(&table, snapshot, "test/old-2.parquet")
                .await
                .unwrap();
            assert_eq!(survivor.status(), ManifestStatus::Existing);
            assert_eq!(survivor.snapshot_id(), Some(old_snapshot.snapshot_id()));
            assert_eq!(
                survivor.sequence_number(),
                Some(old_snapshot.sequence_number())
            );
            assert_eq!(
                survivor.data_file().partition,
                Struct::from_iter([Some(Literal::timestamptz(TS_EVENING))])
            );

            let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
            let old_spec_manifest = manifest_list
                .entries()
                .iter()
                .find(|m| m.partition_spec_id == 0)
                .unwrap();
            // The summary spans the deleted entry as well as the survivor,
            // as it does in Java, where every entry updates it.
            let summary = &old_spec_manifest.partitions.as_ref().unwrap()[0];
            let morning = Datum::timestamptz_micros(TS_MORNING).to_bytes().unwrap();
            let evening = Datum::timestamptz_micros(TS_EVENING).to_bytes().unwrap();
            assert_eq!(summary.lower_bound.as_ref(), Some(&morning));
            assert_eq!(summary.upper_bound.as_ref(), Some(&evening));

            let totals = &snapshot.summary().additional_properties;
            assert_eq!(totals.get("total-data-files").unwrap(), "3");
            assert_eq!(totals.get("total-records").unwrap(), "30");
        }
    }

    /// One rewrite removing a file from each spec: both manifests are filtered
    /// under their own spec and the merged file lands under the default spec.
    #[tokio::test]
    async fn test_rewrite_files_across_partition_specs() {
        for format_version in [FormatVersion::V2, FormatVersion::V3] {
            let catalog = new_memory_catalog().await;
            let (table, _, [old_1, _old_2, new_1]) =
                spec_evolved_table_with_files(&catalog, format_version).await;

            let merged = make_file_in_spec("test/merged.parquet", 20, 1, Literal::date(DAY));
            let tx = Transaction::new(&table);
            let action = tx
                .rewrite_files()
                .delete_file(old_1)
                .delete_file(new_1)
                .add_file(merged);
            let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

            assert_eq!(
                live_files_by_manifest_spec(&table).await,
                vec![
                    ("test/merged.parquet".to_string(), 1),
                    ("test/old-2.parquet".to_string(), 0),
                ],
                "{format_version}"
            );
            let totals = &table
                .metadata()
                .current_snapshot()
                .unwrap()
                .summary()
                .additional_properties;
            assert_eq!(totals.get("deleted-data-files").unwrap(), "2");
            assert_eq!(totals.get("total-data-files").unwrap(), "2");
            assert_eq!(totals.get("total-records").unwrap(), "30");
        }
    }

    /// A second compaction that removes the last old-spec file drops the spec-0
    /// manifest, and the totals chained through both rewrites stay exact.
    #[tokio::test]
    async fn test_rewrite_files_retires_the_old_spec() {
        for format_version in [FormatVersion::V2, FormatVersion::V3] {
            let catalog = new_memory_catalog().await;
            let (table, _, [old_1, old_2, new_1]) =
                spec_evolved_table_with_files(&catalog, format_version).await;

            let merged = make_file_in_spec("test/merged-1.parquet", 10, 1, Literal::date(DAY));
            let tx = Transaction::new(&table);
            let action = tx.rewrite_files().delete_file(old_1).add_file(merged);
            let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

            let merged = make_file_in_spec("test/merged-2.parquet", 20, 1, Literal::date(DAY));
            let tx = Transaction::new(&table);
            let action = tx
                .rewrite_files()
                .delete_file(old_2)
                .delete_file(new_1)
                .add_file(merged);
            let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

            assert_eq!(
                live_files_by_manifest_spec(&table).await,
                vec![
                    ("test/merged-1.parquet".to_string(), 1),
                    ("test/merged-2.parquet".to_string(), 1),
                ],
                "{format_version}"
            );
            let snapshot = table.metadata().current_snapshot().unwrap();
            let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
            assert!(
                manifest_list
                    .entries()
                    .iter()
                    .filter(|m| m.partition_spec_id == 0)
                    .all(|m| !m.has_added_files() && !m.has_existing_files()),
                "{format_version}: a spec-0 manifest kept a live file"
            );

            let totals = &snapshot.summary().additional_properties;
            assert_eq!(totals.get("total-data-files").unwrap(), "2");
            assert_eq!(totals.get("total-records").unwrap(), "30");

            // The spec-0 manifest only carries the removal made by the snapshot
            // that emptied it. The next rewrite has no reason to keep it.
            let merged = make_file_in_spec("test/merged-3.parquet", 10, 1, Literal::date(DAY));
            let tx = Transaction::new(&table);
            let action = tx
                .rewrite_files()
                .delete_file(make_file_in_spec(
                    "test/merged-1.parquet",
                    10,
                    1,
                    Literal::date(DAY),
                ))
                .add_file(merged);
            let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

            let snapshot = table.metadata().current_snapshot().unwrap();
            let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
            assert!(
                manifest_list
                    .entries()
                    .iter()
                    .all(|m| m.partition_spec_id == 1),
                "{format_version}: a spec-0 manifest outlived its last file"
            );
        }
    }

    /// The summary counts and names the partitions a rewrite changed, each
    /// under its file's own spec: one `identity(ts)` partition lost a file, one
    /// `day(ts)` partition gained one.
    #[tokio::test]
    async fn test_rewrite_files_summarizes_partitions_of_both_specs() {
        let catalog = new_memory_catalog().await;
        let (table, _, [old_1, _old_2, _new_1]) =
            spec_evolved_table_with_files(&catalog, FormatVersion::V2).await;
        // Fast append reports the partition it changed.
        let appended = &table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties;
        assert_eq!(
            appended.get("changed-partition-count").map(String::as_str),
            Some("1")
        );

        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set(
                "write.summary.partition-limit".to_string(),
                "10".to_string(),
            )
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let merged = make_file_in_spec("test/merged.parquet", 10, 1, Literal::date(DAY));
        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_file(old_1).add_file(merged);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let summary = &table
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties;
        assert_eq!(
            summary.get("changed-partition-count").map(String::as_str),
            Some("2"),
            "{summary:?}"
        );
        // The removed file is named under `identity(ts)`, the added one under
        // `day(ts)`.
        let mut partitions: Vec<&str> = summary
            .keys()
            .filter_map(|k| k.strip_prefix("partitions."))
            .collect();
        partitions.sort();
        assert_eq!(
            partitions,
            vec!["ts=2026-08-24+09%3A30%3A00+UTC", "ts_day=2026-08-24"],
            "{summary:?}"
        );
    }

    // Removing delete files: the writer side is staged with `SnapshotProducer`,
    // which also records the delete totals the removal lowers.

    /// Adds delete files and keeps every live manifest, as a merge-on-read
    /// writer does.
    struct AddDeletesOperation;

    impl SnapshotProduceOperation for AddDeletesOperation {
        fn operation(&self) -> Operation {
            Operation::Overwrite
        }

        fn truncate_full_table(&self) -> bool {
            false
        }

        async fn delete_entries(&self, _: &SnapshotProducer<'_>) -> Result<Vec<ManifestEntry>> {
            Ok(vec![])
        }

        async fn existing_manifest(
            &self,
            producer: &SnapshotProducer<'_>,
        ) -> Result<Vec<ManifestFile>> {
            let snapshot = producer.table.metadata().current_snapshot().unwrap();
            let manifest_list = producer.table.manifest_list_reader(snapshot).load().await?;
            Ok(manifest_list
                .consume_entries()
                .into_iter()
                .filter(|m| m.has_added_files() || m.has_existing_files())
                .collect())
        }
    }

    /// Commit `delete_files` in one new delete manifest.
    async fn stage_delete_files(
        catalog: &impl Catalog,
        table: &Table,
        delete_files: Vec<DataFile>,
    ) -> Table {
        let mut commit = SnapshotProducer::builder()
            .with_table(table)
            .with_commit_uuid(Uuid::now_v7())
            .with_added_delete_files(delete_files)
            .build()
            .commit(AddDeletesOperation, DefaultManifestProcess)
            .await
            .unwrap();
        let table_commit = TableCommit::builder()
            .ident(table.identifier().clone())
            .updates(commit.take_updates())
            .requirements(commit.take_requirements())
            .build();
        catalog.update_table(table_commit).await.unwrap()
    }

    fn make_delete_file(
        table: &Table,
        path: &str,
        content: DataContentType,
        records: u64,
    ) -> DataFile {
        let mut file = make_data_file(table, path, records, 100);
        file.content = content;
        if content == DataContentType::EqualityDeletes {
            file.equality_ids = Some(vec![1]);
        }
        file
    }

    /// A v2 table with one data file and one staged position delete file.
    async fn table_with_position_delete(catalog: &impl Catalog) -> (Table, DataFile, DataFile) {
        let table = make_v2_minimal_table_in_catalog(catalog).await;
        let data = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(catalog, &table, vec![data.clone()]).await;
        let deletes = make_delete_file(
            &table,
            "test/1-deletes.parquet",
            DataContentType::PositionDeletes,
            2,
        );
        let table = stage_delete_files(catalog, &table, vec![deletes.clone()]).await;
        (table, data, deletes)
    }

    async fn manifests_of(table: &Table, content: ManifestContentType) -> Vec<ManifestFile> {
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        manifest_list
            .consume_entries()
            .into_iter()
            .filter(|m| m.content == content)
            .collect()
    }

    fn properties(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    /// Path, status, snapshot id, data and file sequence number of an entry.
    type EntryIds = (
        String,
        ManifestStatus,
        Option<i64>,
        Option<i64>,
        Option<i64>,
    );

    /// The entries of `manifest_file`, sorted by path.
    async fn entries_of(table: &Table, manifest_file: &ManifestFile) -> Vec<EntryIds> {
        let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
        let mut entries: Vec<_> = manifest
            .entries()
            .iter()
            .map(|e| {
                (
                    e.file_path().to_string(),
                    e.status(),
                    e.snapshot_id(),
                    e.sequence_number(),
                    e.file_sequence_number,
                )
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    /// Removing one of two position delete files rewrites their delete
    /// manifest as a delete manifest: the survivor keeps its ids, the removed
    /// file is recorded with its own sequence numbers, and data manifests stay.
    #[tokio::test]
    async fn test_rewrite_files_removes_a_position_delete_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let data = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![data]).await;
        let removed = make_delete_file(
            &table,
            "test/1-deletes-a.parquet",
            DataContentType::PositionDeletes,
            2,
        );
        let kept = make_delete_file(
            &table,
            "test/1-deletes-b.parquet",
            DataContentType::PositionDeletes,
            3,
        );
        let table = stage_delete_files(&catalog, &table, vec![removed.clone(), kept]).await;
        let staged = table.metadata().current_snapshot().unwrap().clone();
        let data_manifests = manifests_of(&table, ManifestContentType::Data).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_delete_file(removed);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Replace);
        assert_eq!(
            snapshot.summary().additional_properties,
            properties(&[
                ("changed-partition-count", "1"),
                ("removed-delete-files", "1"),
                ("removed-files-size", "100"),
                ("removed-position-delete-files", "1"),
                ("removed-position-deletes", "2"),
                ("total-data-files", "1"),
                ("total-delete-files", "1"),
                ("total-equality-deletes", "0"),
                ("total-files-size", "200"),
                ("total-position-deletes", "3"),
                ("total-records", "10"),
            ])
        );

        assert_eq!(
            manifests_of(&table, ManifestContentType::Data).await,
            data_manifests
        );
        let delete_manifests = manifests_of(&table, ManifestContentType::Deletes).await;
        assert_eq!(delete_manifests.len(), 1);
        let staged_seq = Some(staged.sequence_number());
        assert_eq!(entries_of(&table, &delete_manifests[0]).await, vec![
            (
                "test/1-deletes-a.parquet".to_string(),
                ManifestStatus::Deleted,
                Some(snapshot.snapshot_id()),
                staged_seq,
                staged_seq,
            ),
            (
                "test/1-deletes-b.parquet".to_string(),
                ManifestStatus::Existing,
                Some(staged.snapshot_id()),
                staged_seq,
                staged_seq,
            ),
        ]);
    }

    /// Removing the only entry of an equality delete manifest keeps that
    /// manifest with just the deleted entry, as the data path does, and passes
    /// the other delete manifest through.
    #[tokio::test]
    async fn test_rewrite_files_removes_an_equality_delete_file() {
        let catalog = new_memory_catalog().await;
        let (table, _, _) = table_with_position_delete(&catalog).await;
        let position_manifests = manifests_of(&table, ManifestContentType::Deletes).await;
        let equality = make_delete_file(
            &table,
            "test/eq-deletes.parquet",
            DataContentType::EqualityDeletes,
            4,
        );
        let table = stage_delete_files(&catalog, &table, vec![equality.clone()]).await;
        let staged = table.metadata().current_snapshot().unwrap().clone();

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_delete_file(equality);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(
            snapshot.summary().additional_properties,
            properties(&[
                ("changed-partition-count", "1"),
                ("removed-delete-files", "1"),
                ("removed-equality-delete-files", "1"),
                ("removed-equality-deletes", "4"),
                ("removed-files-size", "100"),
                ("total-data-files", "1"),
                ("total-delete-files", "1"),
                ("total-equality-deletes", "0"),
                ("total-files-size", "200"),
                ("total-position-deletes", "2"),
                ("total-records", "10"),
            ])
        );

        let delete_manifests = manifests_of(&table, ManifestContentType::Deletes).await;
        assert_eq!(delete_manifests.len(), 2);
        assert_eq!(delete_manifests[0], position_manifests[0]);
        let emptied = &delete_manifests[1];
        assert!(!emptied.has_added_files() && !emptied.has_existing_files());
        let staged_seq = Some(staged.sequence_number());
        assert_eq!(entries_of(&table, emptied).await, vec![(
            "test/eq-deletes.parquet".to_string(),
            ManifestStatus::Deleted,
            Some(snapshot.snapshot_id()),
            staged_seq,
            staged_seq,
        )]);
    }

    #[tokio::test]
    async fn test_rewrite_files_missing_delete_file() {
        let catalog = new_memory_catalog().await;
        let (table, _, _) = table_with_position_delete(&catalog).await;
        let ghost = make_delete_file(
            &table,
            "test/ghost-deletes.parquet",
            DataContentType::PositionDeletes,
            1,
        );

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_delete_file(ghost);
        let err = action
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.message(),
            "Failed to find the following files to delete in the current snapshot: test/ghost-deletes.parquet"
        );
    }

    /// A delete-only rewrite refuses what Java refuses, and a file given to
    /// the method of the other content is refused instead of removed.
    #[tokio::test]
    async fn test_rewrite_files_refuses_invalid_delete_file_removals() {
        let catalog = new_memory_catalog().await;
        let (table, data, deletes) = table_with_position_delete(&catalog).await;
        let added = make_data_file(&table, "test/added.parquet", 10, 100);

        let cases: [(&str, RewriteFilesAction, &str); 3] = [
            (
                "data file given to delete_delete_file",
                Transaction::new(&table)
                    .rewrite_files()
                    .delete_delete_file(data.clone()),
                "Cannot rewrite files: test/1.parquet is a data file, remove it with delete_file",
            ),
            (
                "delete file given to delete_file",
                Transaction::new(&table)
                    .rewrite_files()
                    .delete_file(deletes.clone()),
                "Cannot rewrite files: test/1-deletes.parquet is a PositionDeletes file, remove it with delete_delete_file",
            ),
            (
                "data file added without a data file removed",
                Transaction::new(&table)
                    .rewrite_files()
                    .delete_delete_file(deletes)
                    .add_file(added),
                "Data files to add must be empty because there's no data file to be rewritten",
            ),
        ];
        for (case, action, message) in cases {
            let Err(err) = Arc::new(action).commit(&table).await else {
                panic!("{case}: the rewrite must be refused");
            };
            assert_eq!(err.kind(), ErrorKind::DataInvalid, "{case}");
            assert_eq!(err.message(), message, "{case}");
        }
    }

    /// Removing one of two deletion vectors in the same Puffin file leaves the
    /// other one live.
    #[tokio::test]
    async fn test_rewrite_files_removes_one_deletion_vector_of_a_puffin_file() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let data_1 = make_data_file(&table, "test/1.parquet", 10, 100);
        let data_2 = make_data_file(&table, "test/2.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![data_1, data_2]).await;
        let dv = |referenced: &str, offset: i64| {
            let mut file = make_delete_file(
                &table,
                "test/dvs.puffin",
                DataContentType::PositionDeletes,
                1,
            );
            file.file_format = DataFileFormat::Puffin;
            file.referenced_data_file = Some(referenced.to_string());
            file.content_offset = Some(offset);
            file.content_size_in_bytes = Some(40);
            file
        };
        let removed = dv("test/1.parquet", 4);
        let kept = dv("test/2.parquet", 44);
        let table = stage_delete_files(&catalog, &table, vec![removed.clone(), kept]).await;

        let tx = Transaction::new(&table);
        let action = tx.rewrite_files().delete_delete_file(removed);
        let table = action.apply(tx).unwrap().commit(&catalog).await.unwrap();

        let delete_manifests = manifests_of(&table, ManifestContentType::Deletes).await;
        assert_eq!(delete_manifests.len(), 1);
        let manifest = table
            .manifest_reader()
            .read(&delete_manifests[0])
            .await
            .unwrap();
        let mut entries: Vec<_> = manifest
            .entries()
            .iter()
            .map(|e| {
                (
                    e.data_file().content_offset(),
                    e.data_file().content_size_in_bytes(),
                    e.status(),
                )
            })
            .collect();
        entries.sort_by_key(|(offset, _, _)| *offset);
        assert_eq!(entries, vec![
            (Some(4), Some(40), ManifestStatus::Deleted),
            (Some(44), Some(40), ManifestStatus::Existing),
        ]);
    }

    // Concurrent deletes: a rewrite is planned, then delete files are committed
    // before it.

    /// The partition `x=value` of spec 0 of the minimal tables.
    fn x(value: i64) -> Struct {
        Struct::from_iter([Some(Literal::long(value))])
    }

    /// A file named `path`, of spec `spec_id`, scoped to the data file
    /// `referenced` if given.
    fn file_in_spec(
        path: &str,
        content: DataContentType,
        spec_id: i32,
        partition: Struct,
        referenced: Option<&str>,
    ) -> DataFile {
        let mut file = DataFileBuilder::default()
            .content(content)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition(partition)
            .partition_spec_id(spec_id)
            .build()
            .unwrap();
        file.referenced_data_file = referenced.map(str::to_string);
        if content == DataContentType::EqualityDeletes {
            file.equality_ids = Some(vec![1]);
        }
        file
    }

    /// Make `spec` the default and check that it got `spec_id`.
    async fn set_default_spec(
        catalog: &impl Catalog,
        table: &Table,
        spec: UnboundPartitionSpec,
        spec_id: i32,
    ) -> Table {
        let commit = TableCommit::builder()
            .ident(table.identifier().clone())
            .requirements(vec![])
            .updates(vec![
                TableUpdate::AddSpec { spec },
                TableUpdate::SetDefaultSpec { spec_id: -1 },
            ])
            .build();
        let table = catalog.update_table(commit).await.unwrap();
        assert_eq!(table.metadata().default_partition_spec_id(), spec_id);
        table
    }

    /// When an unpartitioned spec (spec id 1) becomes the default, relative to
    /// the concurrent delete.
    enum SpecChange {
        None,
        BeforeDelete,
        AfterDelete,
        /// Before the delete, and spec 0 is the default again after it.
        AroundDelete,
    }

    /// The data sequence number the rewrite gives its added files.
    enum SequenceNumber {
        New,
        Start,
        AboveStart,
    }

    struct Case {
        name: &'static str,
        spec_change: SpecChange,
        sequence_number: SequenceNumber,
        output: Option<DataFile>,
        delete: DataFile,
        /// The content type named by the refusal, if refused.
        refused: Option<&'static str>,
    }

    /// A rewrite of `test/1.parquet` (spec 0, partition `x=1`) is refused only
    /// for a new delete that can apply to it. `test/2.parquet` shares the
    /// partition and stays.
    #[tokio::test]
    async fn test_rewrite_files_rejects_only_new_deletes_for_replaced_files() {
        use DataContentType::{EqualityDeletes, PositionDeletes};
        let deletes = "test/new-deletes.parquet";
        let output = || {
            Some(file_in_spec(
                "test/out.parquet",
                DataContentType::Data,
                0,
                x(1),
                None,
            ))
        };
        let unpartitioned_output = || {
            Some(file_in_spec(
                "test/out.parquet",
                DataContentType::Data,
                1,
                Struct::empty(),
                None,
            ))
        };
        let cases = [
            Case {
                name: "equality delete in the partition, start sequence number",
                spec_change: SpecChange::None,
                sequence_number: SequenceNumber::Start,
                output: output(),
                delete: file_in_spec(deletes, EqualityDeletes, 0, x(1), None),
                refused: None,
            },
            Case {
                name: "equality delete in the partition, new sequence number",
                spec_change: SpecChange::None,
                sequence_number: SequenceNumber::New,
                output: output(),
                delete: file_in_spec(deletes, EqualityDeletes, 0, x(1), None),
                refused: Some("EqualityDeletes"),
            },
            Case {
                name: "equality delete in the partition, sequence number above the start",
                spec_change: SpecChange::None,
                sequence_number: SequenceNumber::AboveStart,
                output: output(),
                delete: file_in_spec(deletes, EqualityDeletes, 0, x(1), None),
                refused: Some("EqualityDeletes"),
            },
            Case {
                name: "equality delete in the partition, output in an unpartitioned spec, start sequence number",
                spec_change: SpecChange::AfterDelete,
                sequence_number: SequenceNumber::Start,
                output: unpartitioned_output(),
                delete: file_in_spec(deletes, EqualityDeletes, 0, x(1), None),
                refused: Some("EqualityDeletes"),
            },
            Case {
                name: "equality delete in another partition, new sequence number",
                spec_change: SpecChange::None,
                sequence_number: SequenceNumber::New,
                output: output(),
                delete: file_in_spec(deletes, EqualityDeletes, 0, x(2), None),
                refused: None,
            },
            Case {
                name: "equality delete in an unpartitioned spec, output in another spec, start sequence number",
                spec_change: SpecChange::AroundDelete,
                sequence_number: SequenceNumber::Start,
                output: output(),
                delete: file_in_spec(deletes, EqualityDeletes, 1, Struct::empty(), None),
                refused: None,
            },
            Case {
                name: "equality delete in an unpartitioned spec, output in another spec, new sequence number",
                spec_change: SpecChange::AroundDelete,
                sequence_number: SequenceNumber::New,
                output: output(),
                delete: file_in_spec(deletes, EqualityDeletes, 1, Struct::empty(), None),
                refused: Some("EqualityDeletes"),
            },
            Case {
                name: "equality delete in the partition, no output, start sequence number",
                spec_change: SpecChange::None,
                sequence_number: SequenceNumber::Start,
                output: None,
                delete: file_in_spec(deletes, EqualityDeletes, 0, x(1), None),
                refused: None,
            },
            Case {
                name: "equality delete in the partition, no output, new sequence number",
                spec_change: SpecChange::None,
                sequence_number: SequenceNumber::New,
                output: None,
                delete: file_in_spec(deletes, EqualityDeletes, 0, x(1), None),
                refused: Some("EqualityDeletes"),
            },
            Case {
                name: "position delete for the replaced file",
                spec_change: SpecChange::None,
                sequence_number: SequenceNumber::Start,
                output: output(),
                delete: file_in_spec(deletes, PositionDeletes, 0, x(1), Some("test/1.parquet")),
                refused: Some("PositionDeletes"),
            },
            Case {
                name: "position delete for another file of the partition",
                spec_change: SpecChange::None,
                sequence_number: SequenceNumber::Start,
                output: output(),
                delete: file_in_spec(deletes, PositionDeletes, 0, x(1), Some("test/2.parquet")),
                refused: None,
            },
            Case {
                name: "partition-scoped position delete in the partition",
                spec_change: SpecChange::None,
                sequence_number: SequenceNumber::Start,
                output: output(),
                delete: file_in_spec(deletes, PositionDeletes, 0, x(1), None),
                refused: Some("PositionDeletes"),
            },
            Case {
                name: "partition-scoped position delete in another partition",
                spec_change: SpecChange::None,
                sequence_number: SequenceNumber::Start,
                output: output(),
                delete: file_in_spec(deletes, PositionDeletes, 0, x(2), None),
                refused: None,
            },
            Case {
                name: "partition-scoped position delete of another spec",
                spec_change: SpecChange::BeforeDelete,
                sequence_number: SequenceNumber::Start,
                output: unpartitioned_output(),
                delete: file_in_spec(deletes, PositionDeletes, 1, Struct::empty(), None),
                refused: None,
            },
        ];
        for case in cases {
            let catalog = new_memory_catalog().await;
            let table = make_v2_minimal_table_in_catalog(&catalog).await;
            let replaced = make_data_file(&table, "test/1.parquet", 10, 100);
            let other = make_data_file(&table, "test/2.parquet", 10, 100);
            let mut table = append_files(&catalog, &table, vec![replaced.clone(), other]).await;
            let starting_snapshot = table.metadata().current_snapshot().unwrap().clone();

            let mut action = Transaction::new(&table)
                .rewrite_files()
                .delete_file(replaced);
            if let Some(output) = case.output {
                action = action.add_file(output);
            }
            let action = match case.sequence_number {
                SequenceNumber::New => action,
                SequenceNumber::Start => {
                    action.data_sequence_number(starting_snapshot.sequence_number())
                }
                SequenceNumber::AboveStart => {
                    action.data_sequence_number(starting_snapshot.sequence_number() + 1)
                }
            };
            let action = Arc::new(action);
            let partitioned = table.metadata().default_partition_spec().as_ref().clone();
            let unpartitioned = UnboundPartitionSpec::builder().build();
            if let SpecChange::BeforeDelete | SpecChange::AroundDelete = case.spec_change {
                table = set_default_spec(&catalog, &table, unpartitioned.clone(), 1).await;
            }
            table = stage_delete_files(&catalog, &table, vec![case.delete]).await;
            match case.spec_change {
                SpecChange::AfterDelete => {
                    table = set_default_spec(&catalog, &table, unpartitioned, 1).await;
                }
                SpecChange::AroundDelete => {
                    table = set_default_spec(&catalog, &table, partitioned.into(), 0).await;
                }
                SpecChange::None | SpecChange::BeforeDelete => {}
            }

            let outcome = action.commit(&table).await.map(|_| ()).map_err(|err| {
                assert_eq!(err.kind(), ErrorKind::DataInvalid, "{}", case.name);
                err.message().to_string()
            });
            let expected = case.refused.map_or(Ok(()), |content| {
                Err(format!(
                    "Cannot rewrite files: the {content} file {deletes} was added after the starting snapshot {} and can apply to the replaced data file test/1.parquet.",
                    starting_snapshot.snapshot_id()
                ))
            });
            assert_eq!(outcome, expected, "{}", case.name);
        }
    }

    #[test]
    fn test_may_be_same_partition() {
        let tuple = |value: Option<Literal>| Struct::from_iter([value]);
        let cases = [
            (Some(Literal::int(1)), Some(Literal::long(1)), true),
            (Some(Literal::long(1)), Some(Literal::long(2)), false),
            (None, Some(Literal::long(1)), false),
            (None, None, true),
            (
                Some(Literal::float(1.5_f32)),
                Some(Literal::double(1.5_f64)),
                true,
            ),
        ];
        for (a, b, expected) in cases {
            let case = format!("{a:?} / {b:?}");
            assert_eq!(
                may_be_same_partition(&tuple(a), &tuple(b)),
                expected,
                "{case}"
            );
        }
    }

    /// Removing a delete file after the start rewrites its delete manifest
    /// under a new sequence number; the deletes it keeps are as old as before
    /// and do not stop the rewrite.
    #[tokio::test]
    async fn test_rewrite_files_ignores_delete_manifests_rewritten_since_the_start() {
        use DataContentType::PositionDeletes;
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let replaced = make_data_file(&table, "test/1.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![replaced.clone()]).await;
        let removed = file_in_spec(
            "test/1-deletes-a.parquet",
            PositionDeletes,
            0,
            x(1),
            Some("test/1.parquet"),
        );
        let kept = file_in_spec(
            "test/1-deletes-b.parquet",
            PositionDeletes,
            0,
            x(1),
            Some("test/1.parquet"),
        );
        let table = stage_delete_files(&catalog, &table, vec![removed.clone(), kept]).await;
        let starting_sequence_number = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        let action = Transaction::new(&table)
            .rewrite_files()
            .delete_file(replaced)
            .data_sequence_number(starting_sequence_number);
        let action = Arc::new(action);

        let tx = Transaction::new(&table);
        let removal = tx.rewrite_files().delete_delete_file(removed);
        let table = removal.apply(tx).unwrap().commit(&catalog).await.unwrap();
        let delete_manifests = manifests_of(&table, ManifestContentType::Deletes).await;
        assert_eq!(delete_manifests.len(), 1);
        assert_eq!(
            delete_manifests[0].sequence_number,
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .sequence_number()
        );

        action.commit(&table).await.unwrap();
    }

    /// A delete added after the start is still new once a later snapshot has
    /// rewritten its manifest and lists it as `Existing`, as a Java writer's
    /// manifest merge does.
    #[tokio::test]
    async fn test_rewrite_files_rejects_new_deletes_in_rewritten_delete_manifests() {
        use DataContentType::PositionDeletes;
        let catalog = new_memory_catalog().await;
        let table = make_v2_minimal_table_in_catalog(&catalog).await;
        let replaced = make_data_file(&table, "test/1.parquet", 10, 100);
        let other = make_data_file(&table, "test/2.parquet", 10, 100);
        let table = append_files(&catalog, &table, vec![replaced.clone(), other]).await;
        let starting_snapshot = table.metadata().current_snapshot().unwrap().clone();

        let action = Transaction::new(&table)
            .rewrite_files()
            .delete_file(replaced)
            .data_sequence_number(starting_snapshot.sequence_number());
        let action = Arc::new(action);

        let new = file_in_spec(
            "test/1-deletes.parquet",
            PositionDeletes,
            0,
            x(1),
            Some("test/1.parquet"),
        );
        let removed = file_in_spec(
            "test/2-deletes.parquet",
            PositionDeletes,
            0,
            x(1),
            Some("test/2.parquet"),
        );
        let table = stage_delete_files(&catalog, &table, vec![new, removed.clone()]).await;
        let staged = table.metadata().current_snapshot().unwrap().clone();
        let tx = Transaction::new(&table);
        let removal = tx.rewrite_files().delete_delete_file(removed);
        let table = removal.apply(tx).unwrap().commit(&catalog).await.unwrap();
        let removal_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();
        let delete_manifests = manifests_of(&table, ManifestContentType::Deletes).await;
        assert_eq!(delete_manifests.len(), 1);
        let staged_seq = Some(staged.sequence_number());
        assert_eq!(entries_of(&table, &delete_manifests[0]).await, vec![
            (
                "test/1-deletes.parquet".to_string(),
                ManifestStatus::Existing,
                Some(staged.snapshot_id()),
                staged_seq,
                staged_seq,
            ),
            (
                "test/2-deletes.parquet".to_string(),
                ManifestStatus::Deleted,
                Some(removal_snapshot_id),
                staged_seq,
                staged_seq,
            ),
        ]);

        let Err(err) = action.commit(&table).await else {
            panic!("a new delete for the replaced file must be rejected");
        };
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.message(),
            format!(
                "Cannot rewrite files: the PositionDeletes file test/1-deletes.parquet was added after the starting snapshot {} and can apply to the replaced data file test/1.parquet.",
                starting_snapshot.snapshot_id()
            )
        );
    }
}
