//! Submission repository for build queue state and team slots.

use sqlx::PgPool;

/// Persisted submission lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmissionStatusRecord {
    Queued,
    Building,
    Succeeded,
    Failed,
}

impl SubmissionStatusRecord {
    fn as_db(self) -> &'static str {
        match self {
            SubmissionStatusRecord::Queued => "queued",
            SubmissionStatusRecord::Building => "building",
            SubmissionStatusRecord::Succeeded => "succeeded",
            SubmissionStatusRecord::Failed => "failed",
        }
    }
}

/// Input for creating a new submission row.
#[derive(Debug, Clone)]
pub struct NewSubmissionRecord {
    pub submission_id: String,
    pub team_id: String,
    pub user_id: String,
    pub description: String,
    pub wrapper_kind: String,
    pub wrapper_version: String,
    pub archive_path: String,
}

/// Filled slot view for frontend streaming.
#[derive(Debug, Clone)]
pub struct FilledTeamSlotRecord {
    pub slot_index: i16,
    pub submission_id: String,
    pub description: Option<String>,
}

/// Succeeded submission assigned to requested team slot.
#[derive(Debug, Clone)]
pub struct SucceededTeamSlotSubmissionRecord {
    pub submission_id: String,
    pub image_ref: Option<String>,
}

/// Succeeded submission not referenced by any team slot.
#[derive(Debug, Clone)]
pub struct OrphanedSubmissionRecord {
    pub submission_id: String,
    pub image_ref: Option<String>,
    pub archive_path: String,
}

/// Repository for submission state persistence.
#[derive(Clone)]
pub struct SubmissionRepo {
    pool: PgPool,
}

impl SubmissionRepo {
    /// Creates a repository backed by the provided Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a queued submission row.
    pub async fn create_submission(&self, submission: &NewSubmissionRecord) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO submissions (
                submission_id,
                team_id,
                user_id,
                description,
                wrapper_kind,
                wrapper_version,
                status,
                archive_path
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7::submission_status, $8
            )
            "#,
        )
        .bind(&submission.submission_id)
        .bind(&submission.team_id)
        .bind(&submission.user_id)
        .bind(&submission.description)
        .bind(&submission.wrapper_kind)
        .bind(&submission.wrapper_version)
        .bind(SubmissionStatusRecord::Queued.as_db())
        .bind(&submission.archive_path)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Sets submission status to building.
    pub async fn mark_building(&self, submission_id: &str) -> anyhow::Result<()> {
        self.update_status(submission_id, SubmissionStatusRecord::Building, None, None)
            .await
    }

    /// Sets submission status to succeeded and stores image reference.
    pub async fn mark_succeeded(&self, submission_id: &str, image_ref: &str) -> anyhow::Result<()> {
        self.update_status(
            submission_id,
            SubmissionStatusRecord::Succeeded,
            Some(image_ref),
            None,
        )
        .await
    }

    /// Marks submission as succeeded and assigns it to slot in one transaction.
    pub async fn mark_succeeded_and_assign_slot(
        &self,
        submission_id: &str,
        team_id: &str,
        slot_index: i16,
        image_ref: &str,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;

        let status_updated = sqlx::query(
            r#"
            UPDATE submissions
            SET status = $3::submission_status,
                image_ref = $4,
                error_message = NULL,
                updated_at = NOW()
            WHERE submission_id = $1
              AND team_id = $2
            "#,
        )
        .bind(submission_id)
        .bind(team_id)
        .bind(SubmissionStatusRecord::Succeeded.as_db())
        .bind(image_ref)
        .execute(&mut *tx)
        .await?;
        if status_updated.rows_affected() != 1 {
            anyhow::bail!("submission not found for success update");
        }

        let slot_updated = sqlx::query(
            r#"
            UPDATE team_submission_slots
            SET submission_id = $3,
                updated_at = NOW()
            WHERE team_id = $1
              AND slot_index = $2
            "#,
        )
        .bind(team_id)
        .bind(slot_index)
        .bind(submission_id)
        .execute(&mut *tx)
        .await?;
        if slot_updated.rows_affected() != 1 {
            anyhow::bail!("team slot not found for assignment");
        }

        tx.commit().await?;
        Ok(())
    }

    /// Sets submission status to failed and stores error details.
    pub async fn mark_failed(
        &self,
        submission_id: &str,
        error_message: &str,
    ) -> anyhow::Result<()> {
        self.update_status(
            submission_id,
            SubmissionStatusRecord::Failed,
            None,
            Some(error_message),
        )
        .await
    }

    /// Ensures exactly three slots exist for the given team.
    pub async fn ensure_team_slots(&self, team_id: &str) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;

        for slot_index in 1_i16..=3 {
            sqlx::query(
                r#"
                INSERT INTO team_submission_slots (team_id, slot_index)
                VALUES ($1, $2)
                ON CONFLICT (team_id, slot_index) DO NOTHING
                "#,
            )
            .bind(team_id)
            .bind(slot_index)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Assigns submission to team slot.
    pub async fn assign_submission_to_slot(
        &self,
        team_id: &str,
        slot_index: i16,
        submission_id: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE team_submission_slots
            SET submission_id = $3,
                updated_at = NOW()
            WHERE team_id = $1
              AND slot_index = $2
            "#,
        )
        .bind(team_id)
        .bind(slot_index)
        .bind(submission_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Returns only filled slots pointing to succeeded submissions.
    pub async fn list_filled_succeeded_slots(
        &self,
        team_id: &str,
    ) -> anyhow::Result<Vec<FilledTeamSlotRecord>> {
        let rows = sqlx::query_as::<_, (i16, String, Option<String>)>(
            r#"
            SELECT slots.slot_index, submissions.submission_id, submissions.description
            FROM team_submission_slots AS slots
            JOIN submissions ON submissions.submission_id = slots.submission_id
            WHERE slots.team_id = $1
              AND submissions.status = 'succeeded'::submission_status
            ORDER BY slots.slot_index ASC
            "#,
        )
        .bind(team_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(slot_index, submission_id, description)| FilledTeamSlotRecord {
                    slot_index,
                    submission_id,
                    description,
                },
            )
            .collect())
    }

    /// Returns succeeded submission currently assigned to team slot.
    pub async fn get_succeeded_submission_for_slot(
        &self,
        team_id: &str,
        slot_index: i16,
    ) -> anyhow::Result<Option<SucceededTeamSlotSubmissionRecord>> {
        let row = sqlx::query_as::<_, (String, Option<String>)>(
            r#"
            SELECT submissions.submission_id, submissions.image_ref
            FROM team_submission_slots AS slots
            JOIN submissions ON submissions.submission_id = slots.submission_id
            WHERE slots.team_id = $1
              AND slots.slot_index = $2
              AND submissions.status = 'succeeded'::submission_status
            LIMIT 1
            "#,
        )
        .bind(team_id)
        .bind(slot_index)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(
            |(submission_id, image_ref)| SucceededTeamSlotSubmissionRecord {
                submission_id,
                image_ref,
            },
        ))
    }

    /// Returns succeeded submissions for team that are not used in any slot.
    pub async fn list_orphaned_succeeded_submissions(
        &self,
        team_id: &str,
        keep_submission_id: &str,
    ) -> anyhow::Result<Vec<OrphanedSubmissionRecord>> {
        let rows = sqlx::query_as::<_, (String, Option<String>, String)>(
            r#"
            SELECT s.submission_id, s.image_ref, s.archive_path
            FROM submissions AS s
            WHERE s.team_id = $1
              AND s.status = 'succeeded'::submission_status
              AND s.submission_id <> $2
              AND NOT EXISTS (
                  SELECT 1
                  FROM team_submission_slots AS slots
                  WHERE slots.submission_id = s.submission_id
              )
            ORDER BY s.created_at ASC
            "#,
        )
        .bind(team_id)
        .bind(keep_submission_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(submission_id, image_ref, archive_path)| OrphanedSubmissionRecord {
                    submission_id,
                    image_ref,
                    archive_path,
                },
            )
            .collect())
    }

    /// Clears image reference after successful image cleanup.
    pub async fn clear_image_ref(&self, submission_id: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE submissions
            SET image_ref = NULL,
                updated_at = NOW()
            WHERE submission_id = $1
            "#,
        )
        .bind(submission_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn update_status(
        &self,
        submission_id: &str,
        status: SubmissionStatusRecord,
        image_ref: Option<&str>,
        error_message: Option<&str>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE submissions
            SET status = $2::submission_status,
                image_ref = COALESCE($3, image_ref),
                error_message = $4,
                updated_at = NOW()
            WHERE submission_id = $1
            "#,
        )
        .bind(submission_id)
        .bind(status.as_db())
        .bind(image_ref)
        .bind(error_message)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
