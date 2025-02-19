use std::collections::HashMap;
use std::str::FromStr;

use opendal::services::AzdlsConfig;

use crate::{Error, ErrorKind, Result};

/// Azdls configuration keys with conversions to [`opendal::Operator`] configuration keys.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, strum::EnumString, strum::Display,
)]
#[strum(serialize_all = "snake_case")]
pub enum ConfigKeys {
    /// Az endpoint to use
    Endpoint,
    /// Az client id, used for client credential flow, created in microsoft app registration
    ClientId,
    /// Az client secret, used for client credential flow, created in microsoft app registration
    ClientSecret,
    /// Az tenant id, required for client credential flow
    TenantId,
    /// Az account key, used for shared key authentication
    AccountKey,
    /// Az storage account name
    AccountName,
    /// Az filesystem to use, also known as container
    Filesystem,
    /// Az authority host, used for client credential flow
    AuthorityHost,
}

pub(crate) fn azdls_config_parse(m: HashMap<String, String>) -> Result<AzdlsConfig> {
    let mut cfg = AzdlsConfig::default();
    for (k, v) in m.into_iter() {
        let config_key = ConfigKeys::from_str(k.as_str()).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid azdls config key: {}", k),
            )
        })?;
        match config_key {
            ConfigKeys::Endpoint => cfg.endpoint = Some(v),
            ConfigKeys::ClientId => cfg.client_id = Some(v),
            ConfigKeys::ClientSecret => cfg.client_secret = Some(v),
            ConfigKeys::TenantId => cfg.tenant_id = Some(v),
            ConfigKeys::AccountKey => cfg.account_key = Some(v),
            ConfigKeys::AccountName => cfg.account_name = Some(v),
            ConfigKeys::Filesystem => cfg.filesystem = v,
            ConfigKeys::AuthorityHost => cfg.authority_host = Some(v),
        }
    }

    Ok(cfg)
}
