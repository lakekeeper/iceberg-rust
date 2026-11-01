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

//! The equality-delete keys a reader has loaded, shared by every data file it reads.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard};

use crate::arrow::caching_delete_file_loader::EqDeleteKey;
use crate::spec::Type;

/// Marks a value of [`KeyFiles::keys`] as an index into [`KeyFiles::overflow`].
const OVERFLOW: u64 = 1 << 63;

/// The keys of every equality delete file with one column layout that a reader has loaded,
/// each with the ids of the files that hold it.
///
/// A data file is probed against the keys of its own delete files only, so the index holds
/// one copy of the keys however many data files are read at once.
#[derive(Debug)]
pub(crate) struct EqDeleteIndex {
    /// Name, field id and type of each key column, in key order.
    fields: Vec<(String, i32, Type)>,
    keys: RwLock<KeyFiles>,
}

#[derive(Debug, Default)]
struct KeyFiles {
    /// The one file of a key, or `OVERFLOW | i` for a key held by the files in `overflow[i]`.
    keys: HashMap<EqDeleteKey, u64>,
    overflow: Vec<Vec<u64>>,
}

impl KeyFiles {
    fn insert(&mut self, key: EqDeleteKey, file: u64) {
        match self.keys.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(file);
            }
            Entry::Occupied(mut entry) => {
                let value = *entry.get();
                if value & OVERFLOW == 0 {
                    // Push first: a panic in between must not leave a marker without its list.
                    self.overflow.push(vec![value, file]);
                    *entry.get_mut() = OVERFLOW | (self.overflow.len() - 1) as u64;
                } else {
                    self.overflow[(value & !OVERFLOW) as usize].push(file);
                }
            }
        }
    }

    fn contains(&self, key: &EqDeleteKey, files: &FileSet) -> bool {
        match self.keys.get(key) {
            None => false,
            Some(&value) if value & OVERFLOW == 0 => files.contains(value),
            Some(&value) => self.overflow[(value & !OVERFLOW) as usize]
                .iter()
                .any(|&file| files.contains(file)),
        }
    }
}

impl EqDeleteIndex {
    pub(crate) fn new(fields: Vec<(String, i32, Type)>) -> Self {
        Self {
            fields,
            keys: RwLock::new(KeyFiles::default()),
        }
    }

    /// Name, field id and type of each key column, in key order.
    pub(crate) fn fields(&self) -> &[(String, i32, Type)] {
        &self.fields
    }

    /// Whether `fields` has this index's field ids and types, in the same order.
    pub(crate) fn layout_matches(&self, fields: &[(String, i32, Type)]) -> bool {
        self.fields.len() == fields.len()
            && self
                .fields
                .iter()
                .zip(fields)
                .all(|((_, id, ty), (_, other_id, other_ty))| id == other_id && ty == other_ty)
    }

    /// Adds the keys of the delete file `file`. Each file id must be added once.
    pub(crate) fn insert_file(&self, keys: HashSet<EqDeleteKey>, file: u64) {
        // A panic while inserting leaves keys under an id that no loaded file has, which no
        // probe matches.
        let mut index = self.keys.write().unwrap_or_else(PoisonError::into_inner);
        for key in keys {
            index.insert(key, file);
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.keys.read().unwrap().keys.len()
    }
}

/// A set of delete file ids, dense and small: one bit per file the reader has loaded.
#[derive(Debug, Clone, Default)]
struct FileSet(Vec<u64>);

impl FileSet {
    fn insert(&mut self, file: u64) {
        let word = (file / 64) as usize;
        if word >= self.0.len() {
            self.0.resize(word + 1, 0);
        }
        self.0[word] |= 1 << (file % 64);
    }

    fn contains(&self, file: u64) -> bool {
        self.0
            .get((file / 64) as usize)
            .is_some_and(|word| word & (1 << (file % 64)) != 0)
    }
}

/// The equality deletes of one data file with one column layout: the reader's shared index,
/// restricted to the data file's own delete files.
#[derive(Debug, Clone)]
pub(crate) struct EqDeleteProbe {
    index: Arc<EqDeleteIndex>,
    files: FileSet,
    key_count: usize,
}

impl EqDeleteProbe {
    pub(crate) fn new(index: Arc<EqDeleteIndex>) -> Self {
        Self {
            index,
            files: FileSet::default(),
            key_count: 0,
        }
    }

    /// Adds the delete file `file`, which holds `key_count` keys of the index.
    pub(crate) fn add_file(&mut self, file: u64, key_count: usize) {
        self.files.insert(file);
        self.key_count += key_count;
    }

    pub(crate) fn index(&self) -> &Arc<EqDeleteIndex> {
        &self.index
    }

    /// Name, field id and type of each key column, in key order.
    pub(crate) fn fields(&self) -> &[(String, i32, Type)] {
        self.index.fields()
    }

    /// Whether the delete files hold no key, so no row can match.
    pub(crate) fn is_empty(&self) -> bool {
        self.key_count == 0
    }

    /// Locks the index for reading. Hold the matcher for a batch, not for a whole file: other
    /// delete files wait for it to be added.
    pub(crate) fn matcher(&self) -> EqDeleteMatcher<'_> {
        EqDeleteMatcher {
            keys: self
                .index
                .keys
                .read()
                .unwrap_or_else(PoisonError::into_inner),
            files: &self.files,
        }
    }

    /// The keys of this probe's delete files.
    #[cfg(test)]
    pub(crate) fn keys(&self) -> HashSet<EqDeleteKey> {
        let matcher = self.matcher();
        matcher
            .keys
            .keys
            .keys()
            .filter(|key| matcher.contains(key))
            .cloned()
            .collect()
    }
}

pub(crate) struct EqDeleteMatcher<'a> {
    keys: RwLockReadGuard<'a, KeyFiles>,
    files: &'a FileSet,
}

impl EqDeleteMatcher<'_> {
    /// Whether one of the probe's delete files holds `key`.
    pub(crate) fn contains(&self, key: &EqDeleteKey) -> bool {
        self.keys.contains(key, self.files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{Datum, PrimitiveType};

    fn key(v: i64) -> EqDeleteKey {
        EqDeleteKey(vec![Some(Datum::long(v))])
    }

    fn index() -> Arc<EqDeleteIndex> {
        Arc::new(EqDeleteIndex::new(vec![(
            "id".to_string(),
            1,
            Type::Primitive(PrimitiveType::Long),
        )]))
    }

    // A key held by several files matches for a probe with any one of them, and for no
    // other probe.
    #[test]
    fn test_probe_matches_only_its_own_files() {
        let index = index();
        index.insert_file(HashSet::from([key(1), key(2)]), 0);
        index.insert_file(HashSet::from([key(2), key(3)]), 1);
        index.insert_file(HashSet::from([key(2)]), 130);
        assert_eq!(index.len(), 3);

        let probe = |files: &[u64]| {
            let mut probe = EqDeleteProbe::new(index.clone());
            for &file in files {
                probe.add_file(file, 1);
            }
            let mut keys: Vec<i64> = probe
                .keys()
                .into_iter()
                .map(|k| match k.0[0].as_ref().unwrap().literal() {
                    crate::spec::PrimitiveLiteral::Long(v) => *v,
                    other => panic!("{other:?}"),
                })
                .collect();
            keys.sort();
            keys
        };
        assert_eq!(probe(&[]), Vec::<i64>::new());
        assert_eq!(probe(&[0]), vec![1, 2]);
        assert_eq!(probe(&[1]), vec![2, 3]);
        assert_eq!(probe(&[130]), vec![2]);
        assert_eq!(probe(&[2, 64, 129]), Vec::<i64>::new());
        assert_eq!(probe(&[0, 1, 130]), vec![1, 2, 3]);
    }

    #[test]
    fn test_layout_matches_ignores_names() {
        let index = index();
        assert!(index.layout_matches(&[(
            "other".to_string(),
            1,
            Type::Primitive(PrimitiveType::Long)
        )]));
        assert!(!index.layout_matches(&[(
            "id".to_string(),
            1,
            Type::Primitive(PrimitiveType::Int)
        )]));
        assert!(!index.layout_matches(&[(
            "id".to_string(),
            2,
            Type::Primitive(PrimitiveType::Long)
        )]));
        assert!(!index.layout_matches(&[]));
    }
}

/// Random tables through the planner's delete index and the reader, with real Parquet files:
/// a row must be deleted iff one of its task's equality delete files holds its key, in that
/// file's layout. Files may store a key column as `int` (promoted to `long`), a data file may
/// lack `b`, and the projection may leave out key columns.
#[cfg(test)]
mod differential_tests {
    use std::collections::{HashMap, HashSet};
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use futures::{SinkExt, StreamExt, TryStreamExt};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use tempfile::TempDir;

    use crate::arrow::ArrowReaderBuilder;
    use crate::delete_file_index::DeleteFileIndex;
    use crate::io::FileIO;
    use crate::runtime::Runtime;
    use crate::scan::{DeleteFileContext, FileScanTask, FileScanTaskStream};
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestEntry,
        ManifestStatus, NestedField, PartitionSpec, PrimitiveType, Schema, Struct, Transform, Type,
    };

    /// Values of the columns `a` (field 1) and `b` (field 2).
    type Row = (Option<i64>, Option<i64>);

    struct DeleteFile {
        layout: Vec<i32>,
        keys: Vec<Vec<Option<i64>>>,
    }

    struct DataFileCase {
        file: DataFile,
        seq: i64,
        /// Row id (field 4) and values.
        rows: Vec<(i64, Row)>,
    }

    fn project(layout: &[i32], row: &Row) -> Vec<Option<i64>> {
        layout
            .iter()
            .map(|id| match id {
                1 => row.0,
                2 => row.1,
                _ => unreachable!(),
            })
            .collect()
    }

    fn rand_val(rng: &mut StdRng) -> Option<i64> {
        if rng.random_bool(0.15) {
            None
        } else {
            Some(rng.random_range(0..6))
        }
    }

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "a", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(3, "p", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(4, "r", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap()
    }

    /// Identity, unpartitioned, all-void, and identity again under another spec id.
    fn specs(schema: &Schema) -> Vec<PartitionSpec> {
        let identity = |id| {
            PartitionSpec::builder(schema.clone())
                .with_spec_id(id)
                .add_partition_field("p", "p", Transform::Identity)
                .unwrap()
                .build()
                .unwrap()
        };
        vec![
            identity(0),
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .build()
                .unwrap(),
            PartitionSpec::builder(schema.clone())
                .with_spec_id(2)
                .add_partition_field("p", "p_void", Transform::Void)
                .unwrap()
                .build()
                .unwrap(),
            identity(3),
        ]
    }

    fn rand_partition(rng: &mut StdRng, spec_id: i32) -> Struct {
        match spec_id {
            1 => Struct::empty(),
            2 => Struct::from_iter([None::<Literal>]),
            _ if rng.random_bool(0.1) => Struct::from_iter([None::<Literal>]),
            _ => Struct::from_iter([Some(Literal::long(rng.random_range(0..3)))]),
        }
    }

    /// A column of field `id`, stored as `int` if `int32`.
    fn column(name: &str, id: i32, int32: bool, values: Vec<Option<i64>>) -> (Field, ArrayRef) {
        let (data_type, array) = if int32 {
            let values = values.into_iter().map(|v| v.map(|v| v as i32));
            (
                DataType::Int32,
                Arc::new(values.collect::<Int32Array>()) as ArrayRef,
            )
        } else {
            (
                DataType::Int64,
                Arc::new(Int64Array::from(values)) as ArrayRef,
            )
        };
        let field = Field::new(name, data_type, id != 4).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]));
        (field, array)
    }

    fn write_parquet(path: &str, columns: Vec<(Field, ArrayRef)>) -> u64 {
        let (fields, columns): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        std::fs::metadata(path).unwrap().len()
    }

    /// Returns (mismatching rows, rows, deleted rows).
    async fn run_case(seed: u64, dir: &str) -> (usize, usize, usize) {
        let mut rng = StdRng::seed_from_u64(seed);
        let schema = Arc::new(schema());
        let specs = specs(&schema);
        // Some cases load more than 64 delete files into one reader.
        let n_del = if rng.random_bool(0.1) {
            rng.random_range(60..150)
        } else {
            rng.random_range(0..25)
        };
        let n_data = rng.random_range(1..25);

        let (index, mut tx) = DeleteFileIndex::new(Runtime::current());
        let mut deletes: HashMap<String, DeleteFile> = HashMap::new();
        for d in 0..n_del {
            let spec_id = rng.random_range(0..4);
            let partition = rand_partition(&mut rng, spec_id);
            let layout = if rng.random_bool(0.5) {
                vec![1]
            } else {
                vec![1, 2]
            };
            let n_keys = rng.random_range(1..6);
            let keys: Vec<Vec<Option<i64>>> = (0..n_keys)
                .map(|_| project(&layout, &(rand_val(&mut rng), rand_val(&mut rng))))
                .collect();
            let path = format!("{dir}/eq-{d}.parquet");
            let columns = layout
                .iter()
                .enumerate()
                .map(|(c, id)| {
                    let name = if *id == 1 { "a" } else { "b" };
                    let values = keys.iter().map(|k| k[c]).collect();
                    column(name, *id, rng.random_bool(0.3), values)
                })
                .collect();
            let size = write_parquet(&path, columns);
            let file = DataFileBuilder::default()
                .file_path(path.clone())
                .file_format(DataFileFormat::Parquet)
                .content(DataContentType::EqualityDeletes)
                .equality_ids(Some(layout.clone()))
                .record_count(n_keys as u64)
                .partition(partition)
                .partition_spec_id(spec_id)
                .file_size_in_bytes(size)
                .build()
                .unwrap();
            let seq = rng.random_range(1..12);
            // The planner sometimes lacks the delete's spec.
            let partition_spec = rng
                .random_bool(0.8)
                .then(|| Arc::new(specs[spec_id as usize].clone()));
            tx.send(DeleteFileContext {
                manifest_entry: Arc::new(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .sequence_number(seq)
                        .data_file(file)
                        .build(),
                ),
                partition_spec_id: spec_id,
                partition_spec,
            })
            .await
            .unwrap();
            deletes.insert(path, DeleteFile { layout, keys });
        }
        drop(tx);

        let mut row_id = 0;
        let mut data_files = Vec::new();
        for d in 0..n_data {
            let spec_id = rng.random_range(0..4);
            let partition = rand_partition(&mut rng, spec_id);
            let seq = rng.random_range(1..12);
            // A file written before `b` was added reads `b` as null.
            let has_b = rng.random_bool(0.85);
            let rows: Vec<(i64, Row)> = (0..rng.random_range(1..15))
                .map(|_| {
                    row_id += 1;
                    let (a, b) = (rand_val(&mut rng), rand_val(&mut rng));
                    (row_id, (a, b.filter(|_| has_b)))
                })
                .collect();
            let path = format!("{dir}/data-{d}.parquet");
            let values = |f: fn(&(i64, Row)) -> Option<i64>| rows.iter().map(f).collect();
            let mut columns = vec![column("a", 1, rng.random_bool(0.3), values(|r| r.1.0))];
            if has_b {
                columns.push(column("b", 2, rng.random_bool(0.3), values(|r| r.1.1)));
            }
            columns.push(column("p", 3, false, values(|_| None)));
            columns.push(column("r", 4, false, values(|r| Some(r.0))));
            let size = write_parquet(&path, columns);
            let file = DataFileBuilder::default()
                .file_path(path)
                .file_format(DataFileFormat::Parquet)
                .content(DataContentType::Data)
                .record_count(rows.len() as u64)
                .partition(partition)
                .partition_spec_id(spec_id)
                .file_size_in_bytes(size)
                .build()
                .unwrap();
            data_files.push(DataFileCase { file, seq, rows });
        }

        let mut expected_kept = HashSet::new();
        let mut tasks = Vec::new();
        for data in &data_files {
            let task_deletes = index
                .get_deletes_for_data_file(&data.file, Some(data.seq))
                .await
                .unwrap();
            for (id, row) in &data.rows {
                let deleted = task_deletes.iter().any(|delete| {
                    let delete = &deletes[delete.file_path()];
                    delete.keys.contains(&project(&delete.layout, row))
                });
                if !deleted {
                    expected_kept.insert(*id);
                }
            }
            tasks.push(
                FileScanTask::builder()
                    .with_file_size_in_bytes(data.file.file_size_in_bytes())
                    .with_start(0)
                    .with_length(0)
                    .with_data_file_path(data.file.file_path().to_string())
                    .with_data_file_format(DataFileFormat::Parquet)
                    .with_schema(schema.clone())
                    .with_project_field_ids(match rng.random_range(0..4) {
                        0 => vec![4, 1, 2],
                        1 => vec![4, 1],
                        2 => vec![4, 2],
                        _ => vec![4],
                    })
                    .with_deletes(task_deletes)
                    .with_data_sequence_number(Some(data.seq))
                    .with_case_sensitive(false)
                    .build()
                    .unwrap(),
            );
        }

        // One reader for the whole scan, data files in random order, read concurrently.
        for i in (1..tasks.len()).rev() {
            let j = rng.random_range(0..=i);
            tasks.swap(i, j);
        }
        let concurrency = rng.random_range(1..=8);
        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
            .with_data_file_concurrency_limit(concurrency)
            .build();
        let read_one = |task: FileScanTask| {
            let stream = Box::pin(futures::stream::iter([Ok(task)])) as FileScanTaskStream;
            reader.clone().read(stream).unwrap().stream()
        };
        let batches: Vec<RecordBatch> = match seed % 3 {
            0 => {
                let stream = Box::pin(futures::stream::iter(tasks.into_iter().map(Ok)))
                    as FileScanTaskStream;
                reader
                    .read(stream)
                    .unwrap()
                    .stream()
                    .try_collect()
                    .await
                    .unwrap()
            }
            1 => futures::stream::iter(tasks)
                .map(read_one)
                .flatten_unordered(concurrency)
                .try_collect()
                .await
                .unwrap(),
            _ => {
                // As Plus reads a group: P buckets at once, each one `read` per data file in turn.
                let buckets = rng.random_range(1..=4);
                let mut streams: Vec<Vec<FileScanTask>> = vec![Vec::new(); buckets];
                for (i, task) in tasks.into_iter().enumerate() {
                    streams[i % buckets].push(task);
                }
                let streams = streams
                    .into_iter()
                    .map(|bucket| futures::stream::iter(bucket).map(read_one).flatten());
                futures::stream::select_all(streams)
                    .try_collect()
                    .await
                    .unwrap()
            }
        };
        let mut kept = HashSet::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(ids.null_count(), 0);
            kept.extend(ids.values().iter().copied());
        }

        let rows = row_id as usize;
        let mismatches = expected_kept.symmetric_difference(&kept).count();
        (mismatches, rows, rows - expected_kept.len())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_equality_deletes_match_planner_lists_on_random_tables() {
        let cases: u64 = std::env::var("ICEBERG_EQ_DELETE_DIFF_CASES")
            .ok()
            .map_or(300, |cases| cases.parse().unwrap());
        let (mut mismatches, mut rows, mut deleted, mut failing) = (0, 0, 0, Vec::new());
        for seed in 0..cases {
            let dir = TempDir::new().unwrap();
            let (m, r, d) = run_case(seed, dir.path().to_str().unwrap()).await;
            if m > 0 {
                failing.push(seed);
            }
            mismatches += m;
            rows += r;
            deleted += d;
        }
        println!("cases={cases} rows={rows} deleted={deleted} mismatching_rows={mismatches}");
        assert!(
            deleted > rows / 10,
            "too few deletes to test: {deleted} of {rows}"
        );
        assert_eq!(failing, Vec::<u64>::new());
    }
}
