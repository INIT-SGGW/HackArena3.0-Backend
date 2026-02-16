//! Trusted auth-claims parsing helpers.
//!
//! Envoy/edge auth is responsible for JWT verification and injects verified
//! claims in `x-ha3-auth-claims`. Backend only parses and authorizes.
//!
//! Security contract:
//! - the backend must be reachable only via trusted ingress/gateway,
//! - ingress must strip any inbound `x-ha3-auth-claims` from clients,
//! - ingress must inject its own verified claims value.

use std::collections::HashSet;

use base64::Engine as _;
use serde::Deserialize;
use tonic::Status;
use tonic::metadata::MetadataMap;

/// Trusted metadata header injected by ingress after JWT verification.
const AUTH_CLAIMS_HEADER: &str = "x-ha3-auth-claims";

#[derive(Clone, Deserialize)]
struct TokenClaims {
    roles: Option<Vec<String>>,
    realm_access: Option<RealmAccess>,
}

#[derive(Clone, Deserialize)]
struct RealmAccess {
    roles: Option<Vec<String>>,
}

/// Claims-aware authorizer for trusted edge-injected auth metadata.
pub struct TokenValidator;

impl TokenValidator {
    /// Creates a validator instance.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Parse trusted claims from request metadata and check admin role.
    pub async fn is_admin(&self, metadata: &MetadataMap) -> Result<bool, Status> {
        let claims = claims_from_metadata(metadata)?
            .ok_or_else(|| Status::unauthenticated("missing x-ha3-auth-claims"))?;
        let roles = extract_roles(&claims);
        Ok(roles.iter().any(|role| is_admin_role(role)))
    }
}

/// Parse `x-ha3-auth-claims` from gRPC metadata.
///
/// Supported formats:
/// - raw JSON object string,
/// - base64url-encoded JSON object string.
fn claims_from_metadata(metadata: &MetadataMap) -> Result<Option<TokenClaims>, Status> {
    let Some(value) = metadata.get(AUTH_CLAIMS_HEADER) else {
        return Ok(None);
    };
    let raw = value
        .to_str()
        .map_err(|_| Status::unauthenticated("invalid x-ha3-auth-claims header"))?;
    if raw.trim().is_empty() {
        return Err(Status::unauthenticated("empty x-ha3-auth-claims"));
    }

    if let Ok(claims) = serde_json::from_str::<TokenClaims>(raw) {
        return Ok(Some(claims));
    }

    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| Status::unauthenticated("invalid x-ha3-auth-claims encoding"))?;
    let claims = serde_json::from_slice::<TokenClaims>(&decoded)
        .map_err(|_| Status::unauthenticated("invalid x-ha3-auth-claims payload"))?;
    Ok(Some(claims))
}

fn extract_roles(claims: &TokenClaims) -> Vec<String> {
    let mut roles = HashSet::new();
    if let Some(list) = &claims.roles {
        for entry in list {
            if !entry.is_empty() {
                roles.insert(entry.clone());
            }
        }
    }
    if let Some(realm_access) = &claims.realm_access {
        if let Some(list) = &realm_access.roles {
            for entry in list {
                if !entry.is_empty() {
                    roles.insert(entry.clone());
                }
            }
        }
    }
    roles.into_iter().collect()
}

fn is_admin_role(role: &str) -> bool {
    matches!(role, "admin" | "hackarena-admin" | "hackarena3-admin")
}
