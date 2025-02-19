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

//! Defines the [view metadata](https://iceberg.apache.org/view-spec/#view-metadata).
//! The main struct here is [ViewMetadata] which defines the data for a view.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use _serde::ViewMetadataEnum;
use chrono::{DateTime, Utc};
use itertools::{FoldWhile, Itertools};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

use super::view_version::{ViewVersion, ViewVersionId, ViewVersionRef};
use super::{Schema, SchemaId, SchemaRef, ViewRepresentation};
use crate::catalog::ViewCreation;
use crate::error::{timestamp_ms_to_utc, Result};
use crate::spec::view_properties::{
    REPLACE_DROP_DIALECT_ALLOWED, REPLACE_DROP_DIALECT_ALLOWED_DEFAULT, VERSION_HISTORY_SIZE,
    VERSION_HISTORY_SIZE_DEFAULT,
};
use crate::Error;

/// Reference to [`ViewMetadata`].
pub type ViewMetadataRef = Arc<ViewMetadata>;

pub(crate) static INITIAL_VIEW_VERSION_ID: i32 = 1;

#[derive(Debug, PartialEq, Deserialize, Eq, Clone)]
#[serde(try_from = "ViewMetadataEnum", into = "ViewMetadataEnum")]
/// Fields for the version 1 of the view metadata.
///
/// We assume that this data structure is always valid, so we will panic when invalid error happens.
/// We check the validity of this data structure when constructing.
pub struct ViewMetadata {
    /// Integer Version for the format.
    pub format_version: ViewFormatVersion,
    /// A UUID that identifies the view, generated when the view is created.
    pub view_uuid: Uuid,
    /// The view's base location; used to create metadata file locations
    pub location: String,
    /// ID of the current version of the view (version-id)
    pub current_version_id: ViewVersionId,
    /// A list of known versions of the view
    pub versions: HashMap<ViewVersionId, ViewVersionRef>,
    /// A list of version log entries with the timestamp and version-id for every
    /// change to current-version-id
    pub version_log: Vec<ViewVersionLog>,
    /// A list of schemas, stored as objects with schema-id.
    pub schemas: HashMap<i32, SchemaRef>,
    /// A string to string map of view properties.
    /// Properties are used for metadata such as comment and for settings that
    /// affect view maintenance. This is not intended to be used for arbitrary metadata.
    pub properties: HashMap<String, String>,
}

impl ViewMetadata {
    /// Returns format version of this metadata.
    #[inline]
    pub fn format_version(&self) -> ViewFormatVersion {
        self.format_version
    }

    /// Returns uuid of current view.
    #[inline]
    pub fn uuid(&self) -> Uuid {
        self.view_uuid
    }

    /// Returns view location.
    #[inline]
    pub fn location(&self) -> &str {
        self.location.as_str()
    }

    /// Returns the current version id.
    #[inline]
    pub fn current_version_id(&self) -> ViewVersionId {
        self.current_version_id
    }

    /// Returns all view versions.
    #[inline]
    pub fn versions(&self) -> impl ExactSizeIterator<Item = &ViewVersionRef> {
        self.versions.values()
    }

    /// Lookup a view version by id.
    #[inline]
    pub fn version_by_id(&self, version_id: ViewVersionId) -> Option<&ViewVersionRef> {
        self.versions.get(&version_id)
    }

    /// Returns the current view version.
    #[inline]
    pub fn current_version(&self) -> &ViewVersionRef {
        self.versions
            .get(&self.current_version_id)
            .expect("Current version id set, but not found in view versions")
    }

    /// Returns schemas
    #[inline]
    pub fn schemas_iter(&self) -> impl ExactSizeIterator<Item = &SchemaRef> {
        self.schemas.values()
    }

    /// Lookup schema by id.
    #[inline]
    pub fn schema_by_id(&self, schema_id: SchemaId) -> Option<&SchemaRef> {
        self.schemas.get(&schema_id)
    }

    /// Get current schema
    #[inline]
    pub fn current_schema(&self) -> &SchemaRef {
        let schema_id = self.current_version().schema_id();
        self.schema_by_id(schema_id)
            .expect("Current schema id set, but not found in view metadata")
    }

    /// Returns properties of the view.
    #[inline]
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    /// Append view version to view
    fn add_version(&mut self, view_version: ViewVersion) -> Result<AddedOrPresent<ViewVersionId>> {
        if self.versions.contains_key(&view_version.version_id()) {
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                format!(
                    "A version with the version id {} already exists.",
                    view_version.version_id()
                ),
            ));
        }

        let (exist, max_version) = self
            .versions
            .values()
            .fold_while((None, 0), |(_, max), v| {
                if is_same_version(v, &view_version) {
                    FoldWhile::Done((Some(v.version_id()), max))
                } else {
                    FoldWhile::Continue((None, max.max(v.version_id())))
                }
            })
            .into_inner();
        if let Some(v) = exist {
            return Ok(AddedOrPresent::AlreadyPresent(v));
        }

        let new_version_id = max_version + 1;

        let mut view_version = view_version;
        view_version.version_id = new_version_id;
        self.versions.insert(new_version_id, Arc::new(view_version));
        Ok(AddedOrPresent::Added(new_version_id))
    }

    fn set_current_version_id(&mut self, current_version_id: ViewVersionId) -> Result<()> {
        if !self.versions.contains_key(&current_version_id) {
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                format!(
                    "No version exists with the version id {}.",
                    current_version_id
                ),
            ));
        }
        self.current_version_id = current_version_id;
        self.version_log
            .push(ViewVersionLog::now(current_version_id));
        Ok(())
    }

    fn add_schema(&mut self, schema: Schema) -> Result<AddedOrPresent<i32>> {
        if self.schemas.contains_key(&schema.schema_id()) {
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                format!(
                    "A schema with the schema id {} already exists.",
                    schema.schema_id()
                ),
            ));
        }
        // Not sure why, but the java implementation replaces schema_ids by internally computed ids
        let (maybe_existing, max_schema_id) = self
            .schemas
            .values()
            .fold_while((None, 0), |(_, max), s| {
                if is_same_schema(s, &schema) {
                    FoldWhile::Done((Some(s.schema_id()), max))
                } else {
                    FoldWhile::Continue((None, max.max(s.schema_id())))
                }
            })
            .into_inner();

        if let Some(existing) = maybe_existing {
            return Ok(AddedOrPresent::AlreadyPresent(existing));
        }

        let schema_id = max_schema_id + 1;

        // TODO: use for updates
        let _highest_field_id = self.highest_field_id().max(schema.highest_field_id());

        let schema = Arc::new(schema.into_builder().with_schema_id(schema_id).build()?);
        let schema_id = schema.schema_id();
        self.schemas.insert(schema_id, schema);
        Ok(AddedOrPresent::Added(schema_id))
    }

    fn set_properties(&mut self, properties: HashMap<String, String>) {
        self.properties.extend(properties);
    }

    fn remove_properties(&mut self, keys: HashSet<&String>) {
        self.properties.retain(|k, _| !keys.contains(k));
    }

    fn assign_uuid(&mut self, uuid: Uuid) {
        self.view_uuid = uuid;
    }

    fn highest_field_id(&self) -> i32 {
        self.schemas
            .values()
            .map(|s| s.highest_field_id())
            .max()
            .unwrap_or(0)
    }

    /// Returns view history.
    #[inline]
    pub fn history(&self) -> &[ViewVersionLog] {
        &self.version_log
    }
}

/// `ViewVersion` wrapper to allow appending a new version or replacing the current version.
///
/// `Append` will add a new version without setting it as the current version.
/// `AsCurrent` will add a new version and set it as the current version.
#[derive(Debug, Clone)]
pub enum AppendViewVersion {
    /// Append a new version to the view.
    Append(ViewVersion),
    /// Replace the current version with a new version.
    AsCurrent(ViewVersion),
}

enum AddedOrPresent<T> {
    Added(T),
    AlreadyPresent(T),
}

impl AsRef<ViewVersion> for AppendViewVersion {
    fn as_ref(&self) -> &ViewVersion {
        match self {
            AppendViewVersion::Append(v) => v,
            AppendViewVersion::AsCurrent(v) => v,
        }
    }
}

impl AsMut<ViewVersion> for AppendViewVersion {
    fn as_mut(&mut self) -> &mut ViewVersion {
        match self {
            AppendViewVersion::Append(v) => v,
            AppendViewVersion::AsCurrent(v) => v,
        }
    }
}

// Checks whether the given view versions would behave the same while ignoring the view version
// id, the creation timestamp, and the operation.
fn is_same_version(a: &ViewVersion, b: &ViewVersion) -> bool {
    a.summary == b.summary
        && a.representations == b.representations
        && a.default_catalog == b.default_catalog
        && a.default_namespace == b.default_namespace
        && a.schema_id == b.schema_id
}

fn is_same_schema(a: &Schema, b: &Schema) -> bool {
    a.as_struct() == b.as_struct()
        && a.identifier_field_ids().collect::<HashSet<_>>()
            == b.identifier_field_ids().collect::<HashSet<_>>()
}

/// Manipulating view metadata.
pub struct ViewMetadataBuilder {
    previous: ViewVersionRef,
    metadata: ViewMetadata,
    last_added_version: Option<ViewVersionId>,
    last_added_schema: Option<i32>,
    added_versions: usize, // TODO: Update tracking needed?
}

// TODO: these errors don't match at all
impl ViewMetadataBuilder {
    /// Creates a new view metadata builder from the given view metadata.
    pub fn new(origin: ViewMetadata) -> Self {
        Self {
            previous: origin.current_version().clone(),
            metadata: origin,
            last_added_version: None,
            last_added_schema: None,
            added_versions: 0,
        }
    }

    /// Adds a new version to the view metadata.
    ///
    /// If the schema id is -1, the schema id of the last added schema will be used.
    /// If the view version matches an existing version (ignoring the version id, creation timestamp, and operation),
    /// a new version will not be added.
    ///
    /// # Errors
    /// - If the schema id is -1 and no schema was added before, an error will be returned.
    /// - If the view version contains multiple queries for the same dialect, an error will be returned.
    /// - If the view version is already present, an error will be returned.
    pub fn add_version(mut self, mut version: AppendViewVersion) -> Result<Self> {
        let schema_id = version.as_ref().schema_id();
        if schema_id == -1 {
            version.as_mut().schema_id = self.last_added_schema.ok_or(Error::new(
                crate::ErrorKind::DataInvalid,
                "Cannot set schema to last added without adding schema before.",
            ))?;
        }

        let (_, maybe_err) = version.as_ref().representations().iter().fold_while((HashSet::new(), None), |(mut dialects, _), r| match r {
            ViewRepresentation::Sql(sql) => {
                if dialects.insert(sql.dialect.as_str()) {
                    FoldWhile::Continue((dialects, None))
                } else {
                    FoldWhile::Done((dialects, Some(Error::new(
                        crate::ErrorKind::DataInvalid,
                        format!("Invalid view version: Cannot add multiple queries for dialect {}", sql.dialect),
                    ))))
                }
            }
        }).into_inner();
        if let Some(err) = maybe_err {
            return Err(err);
        }

        match version {
            AppendViewVersion::Append(version) => {
                self.add_and_maybe_set_last_version(version)?;
                Ok(self)
            }
            AppendViewVersion::AsCurrent(version) => {
                let version_id = version.version_id();
                self.add_and_maybe_set_last_version(version)?;
                Ok(self.set_current_version_id(version_id)?)
            }
        }
    }

    /// Adds a new schema to the view metadata.
    ///
    /// If the schema matches an existing schema, a new schema will not be added.
    /// Any set schema_id will not be respected, it will be set to the next available id.
    pub fn add_schema(mut self, schema: Schema) -> Result<Self> {
        match self.metadata.add_schema(schema)? {
            AddedOrPresent::Added(added) => {
                self.last_added_schema = Some(added);
            }
            AddedOrPresent::AlreadyPresent(_) => {}
        };
        Ok(self)
    }

    fn add_and_maybe_set_last_version(&mut self, version: ViewVersion) -> Result<()> {
        match self.metadata.add_version(version)? {
            AddedOrPresent::Added(added) => {
                self.added_versions += 1;
                self.last_added_version = Some(added);
            }
            AddedOrPresent::AlreadyPresent(_) => {}
        };
        Ok(())
    }

    /// Sets the current version id.
    pub fn set_current_version_id(mut self, current_version_id: ViewVersionId) -> Result<Self> {
        if current_version_id == -1 {
            if let Some(last_added_version) = self.last_added_version {
                return self.set_current_version_id(last_added_version);
            }
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                "Cannot set current version to last added without adding version before.",
            ));
        }
        self.metadata.set_current_version_id(current_version_id)?;
        Ok(self)
    }

    /// Creates a new view metadata builder from the given view creation.
    pub fn from_view_creation(view_creation: ViewCreation) -> Result<Self> {
        let ViewCreation {
            location,
            schema,
            properties,
            name: _,
            representations,
            default_catalog,
            default_namespace,
            summary,
        } = view_creation;
        let initial_version_id = super::INITIAL_VIEW_VERSION_ID;
        let version = ViewVersion::builder()
            .with_default_catalog(default_catalog)
            .with_default_namespace(default_namespace)
            .with_representations(representations)
            .with_schema_id(schema.schema_id())
            .with_summary(summary)
            .with_timestamp_ms(Utc::now().timestamp_millis())
            .with_version_id(initial_version_id)
            .build();

        let versions = HashMap::from_iter(vec![(initial_version_id, version.into())]);

        let view_metadata = ViewMetadata {
            format_version: ViewFormatVersion::V1,
            view_uuid: Uuid::now_v7(),
            location,
            current_version_id: initial_version_id,
            versions,
            version_log: vec![ViewVersionLog::now(initial_version_id)],
            schemas: HashMap::from_iter(vec![(schema.schema_id(), Arc::new(schema))]),
            properties,
        };

        Ok(Self {
            previous: view_metadata.current_version().clone(),
            metadata: view_metadata,
            last_added_version: None,
            last_added_schema: None,
            added_versions: 1,
        })
    }

    /// Changes uuid of view metadata.
    pub fn assign_uuid(mut self, uuid: Uuid) -> Self {
        self.metadata.assign_uuid(uuid);
        self
    }

    /// Updates view properties, replacing existing keys, leaving old entries.
    pub fn set_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.metadata.set_properties(properties);
        self
    }

    /// Removes view properties by keys.
    pub fn remove_properties(mut self, keys: HashSet<&String>) -> Self {
        self.metadata.remove_properties(keys);
        self
    }

    /// Returns the new view metadata after changes.
    pub fn build(mut self) -> Result<ViewMetadata> {
        if self.metadata.versions.is_empty() {
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                "Cannot create view metadata without versions.",
            ));
        }

        if self
            .metadata
            .properties()
            .get(REPLACE_DROP_DIALECT_ALLOWED)
            .map(|s| s == "true")
            .unwrap_or(REPLACE_DROP_DIALECT_ALLOWED_DEFAULT)
        {
            check_if_dialect_is_dropped(
                self.previous.as_ref(),
                self.metadata.current_version().as_ref(),
            )?;
        }

        let history_size = self
            .metadata
            .properties()
            .get(VERSION_HISTORY_SIZE)
            .map(|s| {
                s.parse().map_err(|_| {
                    crate::Error::new(
                        crate::ErrorKind::DataInvalid,
                        format!(
                            "{} must be positive int but was: {}",
                            VERSION_HISTORY_SIZE, s
                        ),
                    )
                })
            })
            .transpose()?
            .unwrap_or(VERSION_HISTORY_SIZE_DEFAULT);

        if history_size < 1 {
            return Err(crate::Error::new(
                crate::ErrorKind::DataInvalid,
                format!(
                    "{} must be positive int but was: {}",
                    VERSION_HISTORY_SIZE, history_size
                ),
            ));
        }

        if self.metadata.versions.len() > history_size.max(self.added_versions) {
            let mut versions = self.metadata.versions.keys().copied().collect::<Vec<_>>();
            versions.sort();
            let to_remove = versions
                .iter()
                .take(versions.len() - history_size)
                .copied()
                .collect::<HashSet<_>>();
            self.metadata
                .version_log
                .retain(|log| !to_remove.contains(&log.version_id));
            self.metadata
                .versions
                .retain(|version_id, _| !to_remove.contains(version_id));
        }
        Ok(self.metadata)
    }
}

fn check_if_dialect_is_dropped(previous: &ViewVersion, current: &ViewVersion) -> Result<()> {
    let base_dialects = sql_dialects_for(previous);
    let updated_dialects = sql_dialects_for(current);

    if !updated_dialects.is_superset(&base_dialects) {
        return Err(crate::Error::new(
            crate::ErrorKind::DataInvalid,
            format!(
                "Cannot replace view due to loss of view dialects ({REPLACE_DROP_DIALECT_ALLOWED}=false):\nPrevious dialects: {:?}\nNew dialects: {:?}",
                base_dialects,
                updated_dialects
            ),
        ));
    }
    Ok(())
}

fn sql_dialects_for(view_version: &ViewVersion) -> HashSet<String> {
    view_version
        .representations()
        .iter()
        .map(|repr| match repr {
            ViewRepresentation::Sql(sql) => sql.dialect.to_lowercase(),
        })
        .collect()
}

/// View metadata properties.
pub mod view_properties {
    /// View metadata version history size
    pub const VERSION_HISTORY_SIZE: &str = "version.history.num-entries";
    /// Default view metadata version history size
    pub const VERSION_HISTORY_SIZE_DEFAULT: usize = 10;
    /// View metadata compression codec
    pub const METADATA_COMPRESSION: &str = "write.metadata.compression-codec";
    /// Default view metadata compression codec
    pub const METADATA_COMPRESSION_DEFAULT: &str = "gzip";
    /// View metadata comment
    pub const COMMENT: &str = "comment";
    /// View metadata replace drop dialect allowed
    pub const REPLACE_DROP_DIALECT_ALLOWED: &str = "replace.drop-dialect.allowed";
    /// Default view metadata replace drop dialect allowed
    pub const REPLACE_DROP_DIALECT_ALLOWED_DEFAULT: bool = false;
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
/// A log of when each snapshot was made.
pub struct ViewVersionLog {
    /// ID that current-version-id was set to
    version_id: ViewVersionId,
    /// Timestamp when the view's current-version-id was updated (ms from epoch)
    timestamp_ms: i64,
}

impl ViewVersionLog {
    #[inline]
    /// Creates a new view version log.
    pub fn new(version_id: ViewVersionId, timestamp: i64) -> Self {
        Self {
            version_id,
            timestamp_ms: timestamp,
        }
    }

    /// Returns a new ViewVersionLog with the current timestamp
    pub fn now(version_id: ViewVersionId) -> Self {
        Self {
            version_id,
            timestamp_ms: Utc::now().timestamp_millis(),
        }
    }

    /// Returns the version id.
    #[inline]
    pub fn version_id(&self) -> ViewVersionId {
        self.version_id
    }

    /// Returns the timestamp in milliseconds from epoch.
    #[inline]
    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    /// Returns the last updated timestamp as a DateTime<Utc> with millisecond precision.
    pub fn timestamp(self) -> Result<DateTime<Utc>> {
        timestamp_ms_to_utc(self.timestamp_ms)
    }
}

pub(super) mod _serde {
    /// This is a helper module that defines types to help with serialization/deserialization.
    /// For deserialization the input first gets read into either the [ViewMetadataV1] struct
    /// and then converted into the [ViewMetadata] struct. Serialization works the other way around.
    /// [ViewMetadataV1] is an internal struct that are only used for serialization and deserialization.
    use std::{collections::HashMap, sync::Arc};

    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use super::{ViewFormatVersion, ViewVersionId, ViewVersionLog};
    use crate::spec::schema::_serde::SchemaV2;
    use crate::spec::table_metadata::_serde::VersionNumber;
    use crate::spec::view_version::_serde::ViewVersionV1;
    use crate::spec::{ViewMetadata, ViewVersion};
    use crate::{Error, ErrorKind};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(untagged)]
    pub(super) enum ViewMetadataEnum {
        V1(ViewMetadataV1),
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "kebab-case")]
    /// Defines the structure of a v1 view metadata for serialization/deserialization
    pub(super) struct ViewMetadataV1 {
        pub format_version: VersionNumber<1>,
        pub(super) view_uuid: Uuid,
        pub(super) location: String,
        pub(super) current_version_id: ViewVersionId,
        pub(super) versions: Vec<ViewVersionV1>,
        pub(super) version_log: Vec<ViewVersionLog>,
        pub(super) schemas: Vec<SchemaV2>,
        pub(super) properties: Option<std::collections::HashMap<String, String>>,
    }

    impl Serialize for ViewMetadata {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer {
            // we must do a clone here
            let metadata_enum: ViewMetadataEnum =
                self.clone().try_into().map_err(serde::ser::Error::custom)?;

            metadata_enum.serialize(serializer)
        }
    }

    impl TryFrom<ViewMetadataEnum> for ViewMetadata {
        type Error = Error;
        fn try_from(value: ViewMetadataEnum) -> Result<Self, Error> {
            match value {
                ViewMetadataEnum::V1(value) => value.try_into(),
            }
        }
    }

    impl TryFrom<ViewMetadata> for ViewMetadataEnum {
        type Error = Error;
        fn try_from(value: ViewMetadata) -> Result<Self, Error> {
            Ok(match value.format_version {
                ViewFormatVersion::V1 => ViewMetadataEnum::V1(value.into()),
            })
        }
    }

    impl TryFrom<ViewMetadataV1> for ViewMetadata {
        type Error = Error;
        fn try_from(value: ViewMetadataV1) -> Result<Self, self::Error> {
            let schemas = HashMap::from_iter(
                value
                    .schemas
                    .into_iter()
                    .map(|schema| Ok((schema.schema_id, Arc::new(schema.try_into()?))))
                    .collect::<Result<Vec<_>, Error>>()?,
            );
            let versions = HashMap::from_iter(
                value
                    .versions
                    .into_iter()
                    .map(|x| Ok((x.version_id, Arc::new(ViewVersion::from(x)))))
                    .collect::<Result<Vec<_>, Error>>()?,
            );
            // Make sure at least the current schema exists
            let current_version =
                versions
                    .get(&value.current_version_id)
                    .ok_or(self::Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "No version exists with the current version id {}.",
                            value.current_version_id
                        ),
                    ))?;
            if !schemas.contains_key(&current_version.schema_id()) {
                return Err(self::Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "No schema exists with the schema id {}.",
                        current_version.schema_id()
                    ),
                ));
            }

            Ok(ViewMetadata {
                format_version: ViewFormatVersion::V1,
                view_uuid: value.view_uuid,
                location: value.location,
                schemas,
                properties: value.properties.unwrap_or_default(),
                current_version_id: value.current_version_id,
                versions,
                version_log: value.version_log,
            })
        }
    }

    impl From<ViewMetadata> for ViewMetadataV1 {
        fn from(v: ViewMetadata) -> Self {
            let schemas = v
                .schemas
                .into_values()
                .map(|x| {
                    Arc::try_unwrap(x)
                        .unwrap_or_else(|schema| schema.as_ref().clone())
                        .into()
                })
                .collect();
            let versions = v
                .versions
                .into_values()
                .map(|x| {
                    Arc::try_unwrap(x)
                        .unwrap_or_else(|version| version.as_ref().clone())
                        .into()
                })
                .collect();
            ViewMetadataV1 {
                format_version: VersionNumber::<1>,
                view_uuid: v.view_uuid,
                location: v.location,
                schemas,
                properties: Some(v.properties),
                current_version_id: v.current_version_id,
                versions,
                version_log: v.version_log,
            }
        }
    }
}

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
/// Iceberg format version
pub enum ViewFormatVersion {
    /// Iceberg view spec version 1
    V1 = 1u8,
}

impl PartialOrd for ViewFormatVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ViewFormatVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        (*self as u8).cmp(&(*other as u8))
    }
}

impl Display for ViewFormatVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ViewFormatVersion::V1 => write!(f, "v1"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;

    use anyhow::Result;
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

    use super::{ViewFormatVersion, ViewMetadataBuilder, ViewVersionLog};
    use crate::spec::{
        NestedField, PrimitiveType, Schema, SqlViewRepresentation, Type, ViewMetadata,
        ViewRepresentations, ViewVersion,
    };
    use crate::{NamespaceIdent, ViewCreation};

    fn check_view_metadata_serde(json: &str, expected_type: ViewMetadata) {
        let desered_type: ViewMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(desered_type, expected_type);

        let sered_json = serde_json::to_string(&expected_type).unwrap();
        let parsed_json_value = serde_json::from_str::<ViewMetadata>(&sered_json).unwrap();

        assert_eq!(parsed_json_value, desered_type);
    }

    fn get_test_view_metadata(file_name: &str) -> ViewMetadata {
        let path = format!("testdata/view_metadata/{}", file_name);
        let metadata: String = fs::read_to_string(path).unwrap();

        serde_json::from_str(&metadata).unwrap()
    }

    #[test]
    fn test_view_data_v1() {
        let data = r#"
        {
            "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
            "format-version" : 1,
            "location" : "s3://bucket/warehouse/default.db/event_agg",
            "current-version-id" : 1,
            "properties" : {
              "comment" : "Daily event counts"
            },
            "versions" : [ {
              "version-id" : 1,
              "timestamp-ms" : 1573518431292,
              "schema-id" : 1,
              "default-catalog" : "prod",
              "default-namespace" : [ "default" ],
              "summary" : {
                "engine-name" : "Spark",
                "engineVersion" : "3.3.2"
              },
              "representations" : [ {
                "type" : "sql",
                "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
                "dialect" : "spark"
              } ]
            } ],
            "schemas": [ {
              "schema-id": 1,
              "type" : "struct",
              "fields" : [ {
                "id" : 1,
                "name" : "event_count",
                "required" : false,
                "type" : "int",
                "doc" : "Count of events"
              } ]
            } ],
            "version-log" : [ {
              "timestamp-ms" : 1573518431292,
              "version-id" : 1
            } ]
          }
        "#;

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(
                NestedField::optional(1, "event_count", Type::Primitive(PrimitiveType::Int))
                    .with_doc("Count of events"),
            )])
            .build()
            .unwrap();
        let version = ViewVersion::builder()
            .with_version_id(1)
            .with_timestamp_ms(1573518431292)
            .with_schema_id(1)
            .with_default_catalog("prod".to_string().into())
            .with_default_namespace(NamespaceIdent::from_vec(vec!["default".to_string()]).unwrap())
            .with_summary(HashMap::from_iter(vec![
                ("engineVersion".to_string(), "3.3.2".to_string()),
                ("engine-name".to_string(), "Spark".to_string()),
            ]))
            .with_representations(ViewRepresentations(vec![SqlViewRepresentation {
                sql: "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2"
                    .to_string(),
                dialect: "spark".to_string(),
            }
            .into()]))
            .build();

        let expected = ViewMetadata {
            format_version: ViewFormatVersion::V1,
            view_uuid: Uuid::parse_str("fa6506c3-7681-40c8-86dc-e36561f83385").unwrap(),
            location: "s3://bucket/warehouse/default.db/event_agg".to_string(),
            current_version_id: 1,
            versions: HashMap::from_iter(vec![(1, Arc::new(version))]),
            version_log: vec![ViewVersionLog {
                timestamp_ms: 1573518431292,
                version_id: 1,
            }],
            schemas: HashMap::from_iter(vec![(1, Arc::new(schema))]),
            properties: HashMap::from_iter(vec![(
                "comment".to_string(),
                "Daily event counts".to_string(),
            )]),
        };

        check_view_metadata_serde(data, expected);
    }

    #[test]
    fn test_invalid_view_uuid() -> Result<()> {
        let data = r#"
            {
                "format-version" : 1,
                "view-uuid": "xxxx"
            }
        "#;
        assert!(serde_json::from_str::<ViewMetadata>(data).is_err());
        Ok(())
    }

    #[test]
    fn test_view_builder_from_view_creation() {
        let representations = ViewRepresentations(vec![SqlViewRepresentation {
            sql: "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2"
                .to_string(),
            dialect: "spark".to_string(),
        }
        .into()]);
        let creation = ViewCreation::builder()
            .location("s3://bucket/warehouse/default.db/event_agg".to_string())
            .name("view".to_string())
            .schema(Schema::builder().build().unwrap())
            .default_namespace(NamespaceIdent::from_vec(vec!["default".to_string()]).unwrap())
            .representations(representations)
            .build();

        let metadata = ViewMetadataBuilder::from_view_creation(creation)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(
            metadata.location(),
            "s3://bucket/warehouse/default.db/event_agg"
        );
        assert_eq!(metadata.current_version_id(), 1);
        assert_eq!(metadata.versions().count(), 1);
        assert_eq!(metadata.schemas_iter().count(), 1);
        assert_eq!(metadata.properties().len(), 0);
    }

    #[test]
    fn test_view_metadata_v1_file_valid() {
        let metadata =
            fs::read_to_string("testdata/view_metadata/ViewMetadataV1Valid.json").unwrap();

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(
                    NestedField::optional(1, "event_count", Type::Primitive(PrimitiveType::Int))
                        .with_doc("Count of events"),
                ),
                Arc::new(NestedField::optional(
                    2,
                    "event_date",
                    Type::Primitive(PrimitiveType::Date),
                )),
            ])
            .build()
            .unwrap();

        let version = ViewVersion::builder()
            .with_version_id(1)
            .with_timestamp_ms(1573518431292)
            .with_schema_id(1)
            .with_default_catalog("prod".to_string().into())
            .with_default_namespace(NamespaceIdent::from_vec(vec!["default".to_string()]).unwrap())
            .with_summary(HashMap::from_iter(vec![
                ("engineVersion".to_string(), "3.3.2".to_string()),
                ("engine-name".to_string(), "Spark".to_string()),
            ]))
            .with_representations(ViewRepresentations(vec![SqlViewRepresentation {
                sql: "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2"
                    .to_string(),
                dialect: "spark".to_string(),
            }
            .into()]))
            .build();

        let expected = ViewMetadata {
            format_version: ViewFormatVersion::V1,
            view_uuid: Uuid::parse_str("fa6506c3-7681-40c8-86dc-e36561f83385").unwrap(),
            location: "s3://bucket/warehouse/default.db/event_agg".to_string(),
            current_version_id: 1,
            versions: HashMap::from_iter(vec![(1, Arc::new(version))]),
            version_log: vec![ViewVersionLog {
                timestamp_ms: 1573518431292,
                version_id: 1,
            }],
            schemas: HashMap::from_iter(vec![(1, Arc::new(schema))]),
            properties: HashMap::from_iter(vec![(
                "comment".to_string(),
                "Daily event counts".to_string(),
            )]),
        };

        check_view_metadata_serde(&metadata, expected);
    }

    #[test]
    fn test_view_builder_assign_uuid() {
        let metadata = get_test_view_metadata("ViewMetadataV1Valid.json");
        let metadata_builder = ViewMetadataBuilder::new(metadata);
        let uuid = Uuid::new_v4();
        let metadata = metadata_builder.assign_uuid(uuid).build().unwrap();
        assert_eq!(metadata.uuid(), uuid);
    }

    #[test]
    fn test_view_metadata_v1_unsupported_version() {
        let metadata =
            fs::read_to_string("testdata/view_metadata/ViewMetadataUnsupportedVersion.json")
                .unwrap();

        let desered: Result<ViewMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "data did not match any variant of untagged enum ViewMetadataEnum"
        )
    }

    #[test]
    fn test_view_metadata_v1_version_not_found() {
        let metadata =
            fs::read_to_string("testdata/view_metadata/ViewMetadataV1CurrentVersionNotFound.json")
                .unwrap();

        let desered: Result<ViewMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "DataInvalid => No version exists with the current version id 2."
        )
    }

    #[test]
    fn test_view_metadata_v1_schema_not_found() {
        let metadata =
            fs::read_to_string("testdata/view_metadata/ViewMetadataV1SchemaNotFound.json").unwrap();

        let desered: Result<ViewMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "DataInvalid => No schema exists with the schema id 2."
        )
    }

    #[test]
    fn test_view_metadata_v1_missing_schema_for_version() {
        let metadata =
            fs::read_to_string("testdata/view_metadata/ViewMetadataV1MissingSchema.json").unwrap();

        let desered: Result<ViewMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "data did not match any variant of untagged enum ViewMetadataEnum"
        )
    }

    #[test]
    fn test_view_metadata_v1_missing_current_version() {
        let metadata =
            fs::read_to_string("testdata/view_metadata/ViewMetadataV1MissingCurrentVersion.json")
                .unwrap();

        let desered: Result<ViewMetadata, serde_json::Error> = serde_json::from_str(&metadata);

        assert_eq!(
            desered.unwrap_err().to_string(),
            "data did not match any variant of untagged enum ViewMetadataEnum"
        )
    }
}
