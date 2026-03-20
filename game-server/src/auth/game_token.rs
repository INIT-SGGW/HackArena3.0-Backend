//! Game-token parsing and validation helpers.

use std::collections::HashSet;

use serde::Deserialize;
use tonic::Status;
use tonic::metadata::MetadataMap;

use super::jwks::JwksValidator;

#[derive(Clone, Deserialize)]
#[serde(untagged)]
enum ScopeClaim {
    Single(String),
    Many(Vec<String>),
}

#[derive(Clone, Deserialize)]
struct GameTokenClaims {
    sub: String,
    scope: Option<ScopeClaim>,
    scp: Option<ScopeClaim>,
    instance_uuid: Option<String>,
    team_id: Option<String>,
}

/// Validates game-tokens against a JWKS URL and extracts authorization claims.
pub struct GameTokenValidator {
    validator: JwksValidator,
}

impl GameTokenValidator {
    /// Create a new game-token validator with explicit endpoint, audience and issuer configuration.
    pub fn new_with_config(
        game_token_jwks_endpoint: &str,
        audience: Vec<String>,
        issuers: Vec<String>,
    ) -> Self {
        Self {
            validator: JwksValidator::new(game_token_jwks_endpoint, audience, issuers),
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

    /// Validate the token and return the team ID claim, if present.
    pub async fn team_id_from_token(&self, token: &str) -> Result<Option<String>, Status> {
        let claims = self.claims_from_token(token).await?;
        Ok(claims.team_id)
    }

    /// Validate the token and return the subject claim.
    pub async fn subject_from_token(&self, token: &str) -> Result<String, Status> {
        let claims = self.claims_from_token(token).await?;
        Ok(claims.sub)
    }

    async fn claims_from_token(&self, token: &str) -> Result<GameTokenClaims, Status> {
        self.validator.decode_claims::<GameTokenClaims>(token).await
    }
}

fn extract_scopes(claims: &GameTokenClaims) -> Vec<String> {
    let mut scopes = HashSet::new();
    collect_scope_claim(&mut scopes, claims.scope.as_ref());
    collect_scope_claim(&mut scopes, claims.scp.as_ref());
    scopes.into_iter().collect()
}

fn collect_scope_claim(scopes: &mut HashSet<String>, claim: Option<&ScopeClaim>) {
    let Some(claim) = claim else {
        return;
    };

    match claim {
        ScopeClaim::Single(value) => {
            for entry in value.split_whitespace() {
                if !entry.is_empty() {
                    scopes.insert(entry.to_string());
                }
            }
        }
        ScopeClaim::Many(values) => {
            for entry in values {
                if !entry.is_empty() {
                    scopes.insert(entry.clone());
                }
            }
        }
    }
}

/// Parse the `x-ha3-game-token` metadata entry and return a token if present.
pub fn parse_game_token(metadata: &MetadataMap) -> Result<Option<String>, Status> {
    let Some(value) = metadata.get("x-ha3-game-token") else {
        return Ok(None);
    };
    let raw = value
        .to_str()
        .map_err(|_| Status::unauthenticated("invalid x-ha3-game-token header"))?;
    if raw.trim().is_empty() {
        return Err(Status::unauthenticated("empty x-ha3-game-token"));
    }
    Ok(Some(raw.to_string()))
}
