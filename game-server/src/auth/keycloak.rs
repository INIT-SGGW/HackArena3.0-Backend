//! Keycloak JWT validation helpers backed by a JWKS endpoint.
//!
//! The validator fetches and caches the JSON Web Key Set (JWKS), then uses it
//! to verify ES256 tokens and extract authorization claims.

use std::collections::HashSet;

use serde::Deserialize;
use tonic::Status;
use tonic::metadata::MetadataMap;

use super::jwks::JwksValidator;

#[derive(Clone, Deserialize)]
struct TokenClaims {
    scope: Option<String>,
    scp: Option<Vec<String>>,
    instance_uuid: Option<String>,
    roles: Option<Vec<String>>,
    realm_access: Option<RealmAccess>,
}

#[derive(Clone, Deserialize)]
struct RealmAccess {
    roles: Option<Vec<String>>,
}

/// Validates JWTs against a JWKS URL and extracts authorization claims.
pub struct TokenValidator {
    validator: JwksValidator,
}

impl TokenValidator {
    /// Create a new validator with explicit audience and issuer configuration.
    pub fn new_with_config(jwks_url: &str, audience: Vec<String>, issuers: Vec<String>) -> Self {
        Self {
            validator: JwksValidator::new(jwks_url, audience, issuers),
        }
    }

    /// Validate the token and return a de-duplicated list of scopes.
    pub async fn scopes_from_token(&self, token: &str) -> Result<Vec<String>, Status> {
        let claims = self.claims_from_token(token).await?;
        Ok(extract_scopes(&claims))
    }

    /// Validate the token and return the instance UUID claim, if present.
    pub async fn instance_uuid_from_token(&self, token: &str) -> Result<Option<String>, Status> {
        let claims = self.claims_from_token(token).await?;
        Ok(claims.instance_uuid)
    }

    /// Validate the token and return a de-duplicated list of roles.
    pub async fn roles_from_token(&self, token: &str) -> Result<Vec<String>, Status> {
        let claims = self.claims_from_token(token).await?;
        Ok(extract_roles(&claims))
    }

    /// Validate the token and return whether it contains any admin role.
    pub async fn is_admin(&self, token: &str) -> Result<bool, Status> {
        let roles = self.roles_from_token(token).await?;
        Ok(roles.iter().any(|role| is_admin_role(role)))
    }

    async fn claims_from_token(&self, token: &str) -> Result<TokenClaims, Status> {
        self.validator.decode_claims::<TokenClaims>(token).await
    }
}

fn extract_scopes(claims: &TokenClaims) -> Vec<String> {
    let mut scopes = HashSet::new();
    if let Some(scope) = &claims.scope {
        for entry in scope.split_whitespace() {
            if !entry.is_empty() {
                scopes.insert(entry.to_string());
            }
        }
    }
    if let Some(list) = &claims.scp {
        for entry in list {
            if !entry.is_empty() {
                scopes.insert(entry.clone());
            }
        }
    }
    scopes.into_iter().collect()
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

/// Parse the `authorization` metadata entry and return a bearer token if present.
pub fn parse_bearer_token(metadata: &MetadataMap) -> Result<Option<String>, Status> {
    let Some(value) = metadata.get("authorization") else {
        return Ok(None);
    };
    let raw = value
        .to_str()
        .map_err(|_| Status::unauthenticated("invalid authorization header"))?;
    let token = raw
        .strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))
        .ok_or_else(|| Status::unauthenticated("authorization must be bearer"))?;
    if token.is_empty() {
        return Err(Status::unauthenticated("empty bearer token"));
    }
    Ok(Some(token.to_string()))
}
