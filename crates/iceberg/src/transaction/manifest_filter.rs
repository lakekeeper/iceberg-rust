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

use std::collections::HashSet;

use crate::error::Result;
use crate::spec::{DataFile, ManifestFile, ManifestStatus};
use crate::table::Table;
use crate::transaction::snapshot::SnapshotProducer;
use crate::{Error, ErrorKind};

/// Accumulates the set of files an operation removes from a table and rewrites the
/// affected manifests during manifest production.
///
/// A delete-class operation owns one `ManifestFilterManager` for data manifests and one for
/// delete manifests. Removed files are recorded via [`ManifestFilterManager::delete_file`]
/// keyed by their file path; the later filtering pass drops the matching manifest entries
/// and re-emits the survivors.
///
/// **In v1 this always rewrites**: there is no cross-retry rewrite cache (that is deferred
/// to future work). The rewrite body is a placeholder / pass-through for now — the filter
/// seam is the normative part. The single `Mutex<MergingCache>` on `MergingSnapshotProducer`
/// is retained precisely so the deferred cache has a home.
///
/// NOTE: the placeholder rewrite body has been replaced with a working (non-cached)
/// implementation. This is intentionally not upstreamed — the final shape of the cached
/// solution is still open, so this fork carries a pragmatic "make it work" version.
#[derive(Default)]
pub(crate) struct ManifestFilterManager {
    deleted_files: HashSet<String>,
    fail_missing_delete_paths: bool,
}

impl ManifestFilterManager {
    pub(crate) fn new(fail_missing_delete_paths: bool) -> Self {
        Self {
            deleted_files: HashSet::new(),
            fail_missing_delete_paths,
        }
    }

    /// Record a file for removal.
    ///
    /// `DataFile` covers both data and delete files, so the same entry point serves
    /// data-manifest and delete-manifest filtering. Removals are keyed by file path;
    /// recording the same path more than once keeps a single removal entry.
    pub(crate) fn delete_file(&mut self, file: DataFile) {
        self.deleted_files.insert(file.file_path().to_string());
    }

    fn is_removed(&self, path: &str) -> bool {
        self.deleted_files.contains(path)
    }

    /// Rewrite the given `manifests`, dropping any entries recorded for removal and
    /// re-emitting the survivors.
    ///
    /// **v1 always rewrites**: every input manifest goes through the rewrite path
    /// unconditionally on every attempt — there is no cache lookup/store. The rewrite body
    /// is a placeholder (pass-through) for now; the filter seam is the normative part.
    ///
    /// NOTE: the placeholder has been replaced with a working (non-cached) rewrite that
    /// drops the recorded files and, when `fail_missing_delete_paths` is set, fails with
    /// [`ErrorKind::PreconditionFailed`] if a recorded removal is not found. Not upstreamed —
    /// the final cached design is undecided.
    pub(crate) async fn filter_manifests(
        &self,
        sp: &mut SnapshotProducer<'_>,
        base: &Table,
        manifests: Vec<ManifestFile>,
    ) -> Result<Vec<ManifestFile>> {
        // TODO(future): cache rewritten manifests per input manifest path to avoid
        // re-writing (and orphaning) on retry. Deferred from v1; would read/write
        // MergingCache.filter_cache under a brief lock (never held across IO).
        if self.deleted_files.is_empty() {
            return Ok(manifests);
        }

        let mut pending_deletes: HashSet<String> = self.deleted_files.iter().cloned().collect();
        let file_io = base.file_io().clone();
        let mut filtered = Vec::with_capacity(manifests.len());

        for manifest_file in manifests {
            let manifest = manifest_file.load_manifest(&file_io).await?;
            let entries = manifest.entries();

            let has_removed_entry = entries.iter().any(|entry| {
                entry.status() != ManifestStatus::Deleted && self.is_removed(entry.file_path())
            });
            if !has_removed_entry {
                filtered.push(manifest_file);
                continue;
            }

            let mut writer = sp.new_manifest_writer(manifest_file.content)?;
            let mut survivors = 0usize;
            for entry in entries {
                if entry.status() == ManifestStatus::Deleted {
                    continue;
                }
                if self.is_removed(entry.file_path()) {
                    pending_deletes.remove(entry.file_path());
                    continue;
                }
                writer.add_existing_file(
                    entry.data_file().clone(),
                    entry.snapshot_id().unwrap_or_default(),
                    entry.sequence_number().unwrap_or_default(),
                    entry.file_sequence_number,
                )?;
                survivors += 1;
            }

            if survivors == 0 {
                continue;
            }

            filtered.push(writer.write_manifest_file().await?);
        }

        if self.fail_missing_delete_paths && !pending_deletes.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Missing required files to delete: {pending_deletes:?}"),
            ));
        }

        Ok(filtered)
    }
}
