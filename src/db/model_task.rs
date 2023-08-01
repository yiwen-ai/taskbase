use axum_web::{context::unix_ms, erring::HTTPError};
use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;
use std::collections::HashSet;

use crate::db::scylladb::{self, extract_applied};

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct Task {
    pub uid: xid::Id,
    pub id: xid::Id,
    pub gid: xid::Id,
    pub status: i8,
    pub kind: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub duedate: i64,
    pub threshold: i16,
    pub approvers: HashSet<xid::Id>,
    pub assignees: HashSet<xid::Id>,
    pub resolved: HashSet<xid::Id>,
    pub rejected: HashSet<xid::Id>,
    pub message: String,
    pub payload: Vec<u8>,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

impl Task {
    pub fn with_pk(uid: xid::Id, id: xid::Id) -> Self {
        Self {
            uid,
            id,
            ..Default::default()
        }
    }

    pub fn select_fields(select_fields: Vec<String>, with_pk: bool) -> anyhow::Result<Vec<String>> {
        if select_fields.is_empty() {
            return Ok(Self::fields());
        }

        let fields = Self::fields();
        for field in &select_fields {
            if !fields.contains(field) {
                return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into());
            }
        }

        let mut select_fields = select_fields;
        let field = "gid".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        let field = "status".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        let field = "kind".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        if with_pk {
            let field = "uid".to_string();
            if !select_fields.contains(&field) {
                select_fields.push(field);
            }
            let field = "id".to_string();
            if !select_fields.contains(&field) {
                select_fields.push(field);
            }
        }

        Ok(select_fields)
    }

    pub async fn get_one(
        &mut self,
        db: &scylladb::ScyllaDB,
        select_fields: Vec<String>,
    ) -> anyhow::Result<()> {
        let fields = Self::select_fields(select_fields, false)?;
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM task WHERE uid=? AND id=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.uid.to_cql(), self.id.to_cql());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        self.updated_at = unix_ms() as i64;

        let fields = Self::fields();
        self._fields = fields.clone();

        let mut cols_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut vals_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut params: Vec<&CqlValue> = Vec::with_capacity(fields.len());
        let cols = self.to();

        for field in &fields {
            cols_name.push(field);
            vals_name.push("?");
            params.push(cols.get(field).unwrap());
        }

        let query = format!(
            "INSERT INTO task ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(
                HTTPError::new(409, "Task save failed, please try again".to_string()).into(),
            );
        }

        Ok(true)
    }

    pub async fn update(
        &mut self,
        db: &scylladb::ScyllaDB,
        cols: ColumnsMap,
        updated_at: i64,
    ) -> anyhow::Result<bool> {
        let valid_fields = vec!["duedate", "message"];
        let update_fields = cols.keys();
        for field in &update_fields {
            if !valid_fields.contains(&field.as_str()) {
                return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into());
            }
        }

        self.get_one(db, vec!["status".to_string(), "updated_at".to_string()])
            .await?;
        if self.updated_at != updated_at {
            return Err(HTTPError::new(
                409,
                format!(
                    "Task updated_at conflict, expected updated_at {}, got {}",
                    self.updated_at, updated_at
                ),
            )
            .into());
        }

        let mut set_fields: Vec<String> = Vec::with_capacity(update_fields.len() + 1);
        let mut params: Vec<CqlValue> = Vec::with_capacity(update_fields.len() + 1 + 3);

        let new_updated_at = unix_ms() as i64;
        set_fields.push("updated_at=?".to_string());
        params.push(new_updated_at.to_cql());
        for field in &update_fields {
            set_fields.push(format!("{}=?", field));
            params.push(cols.get(field).unwrap().to_owned());
        }

        let query = format!(
            "UPDATE task SET {} WHERE uid=? AND id=? IF updated_at=?",
            set_fields.join(",")
        );
        params.push(self.uid.to_cql());
        params.push(self.id.to_cql());
        params.push(updated_at.to_cql());

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(
                HTTPError::new(409, "Task update failed, please try again".to_string()).into(),
            );
        }

        self.updated_at = new_updated_at;
        Ok(true)
    }

    pub async fn update_assignees(
        &mut self,
        db: &scylladb::ScyllaDB,
        remove: Vec<xid::Id>,
        add: Vec<xid::Id>,
        updated_at: i64,
    ) -> anyhow::Result<bool> {
        self.get_one(db, vec!["updated_at".to_string()]).await?;
        if self.updated_at != updated_at {
            return Err(HTTPError::new(
                409,
                format!(
                    "Task updated_at conflict, expected updated_at {}, got {}",
                    self.updated_at, updated_at
                ),
            )
            .into());
        }

        let mut updated_at = updated_at;
        let new_updated_at = unix_ms() as i64;
        if !remove.is_empty() {
            let mut params: Vec<CqlValue> = Vec::with_capacity(remove.len() + 4);
            let query = format!(
                "UPDATE task SET assignees=assignees-{{ {} }}, updated_at=? WHERE uid=? AND id=? IF updated_at=?",
                remove.iter().map(|_| "?").collect::<Vec<&str>>().join(",")
            );

            for id in &remove {
                params.push(id.to_cql());
            }
            params.push(new_updated_at.to_cql());
            params.push(self.uid.to_cql());
            params.push(self.id.to_cql());
            params.push(updated_at.to_cql());

            let res = db.execute(query, params).await?;
            if !extract_applied(res) {
                return Err(HTTPError::new(
                    409,
                    "Task update failed, please try again".to_string(),
                )
                .into());
            }
            updated_at = new_updated_at;
        }

        if !add.is_empty() {
            let mut params: Vec<CqlValue> = Vec::with_capacity(add.len() + 4);
            let query = format!(
                "UPDATE task SET assignees=assignees+{{ {} }}, updated_at=? WHERE uid=? AND id=? IF updated_at=?",
                add.iter().map(|_| "?").collect::<Vec<&str>>().join(",")
            );

            for id in &add {
                params.push(id.to_cql());
            }
            params.push(new_updated_at.to_cql());
            params.push(self.uid.to_cql());
            params.push(self.id.to_cql());
            params.push(updated_at.to_cql());

            let res = db.execute(query, params).await?;
            if !extract_applied(res) {
                return Err(HTTPError::new(
                    409,
                    "Task update failed, please try again".to_string(),
                )
                .into());
            }
        }

        Ok(true)
    }

    pub async fn update_resolved(
        &mut self,
        db: &scylladb::ScyllaDB,
        assignee: xid::Id,
    ) -> anyhow::Result<bool> {
        self.get_one(db, vec!["approvers".to_string(), "assignees".to_string()])
            .await?;

        if (!self.approvers.is_empty() || !self.assignees.is_empty()) && !self.approvers.contains(&assignee) && !self.assignees.contains(&assignee) {
            return Err(HTTPError::new(403, "can not resolve task".to_string()).into());
        }

        let query = "UPDATE task SET rejected=rejected-{?}, resolved=resolved+{?} WHERE uid=? AND id=? IF EXISTS";
        let params = (
            assignee.to_cql(),
            assignee.to_cql(),
            self.uid.to_cql(),
            self.id.to_cql(),
        );
        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Task update_resolved failed, please try again".to_string(),
            )
            .into());
        }

        self.get_one(
            db,
            vec![
                "threshold".to_string(),
                "status".to_string(),
                "resolved".to_string(),
                "rejected".to_string(),
            ],
        )
        .await?;

        let can_approve = self.approvers.is_empty() || self.approvers.contains(&assignee);
        if self.status != 1
            && can_approve
            && self.resolved.len() >= self.threshold as usize
            && self.resolved.len() > self.rejected.len()
        {
            let query = "UPDATE task SET status=? WHERE uid=? AND id=? IF EXISTS";
            let params = (1i8, self.uid.to_cql(), self.id.to_cql());
            let res = db.execute(query, params).await?;
            if !extract_applied(res) {
                return Err(HTTPError::new(
                    409,
                    "Task update_resolved failed, please try again".to_string(),
                )
                .into());
            }
        }
        Ok(true)
    }

    pub async fn update_rejected(
        &mut self,
        db: &scylladb::ScyllaDB,
        assignee: xid::Id,
    ) -> anyhow::Result<bool> {
        if (!self.approvers.is_empty() || !self.assignees.is_empty()) && !self.approvers.contains(&assignee) && !self.assignees.contains(&assignee) {
            return Err(HTTPError::new(403, "can not reject task".to_string()).into());
        }

        let query = "UPDATE task SET resolved=resolved-{?}, rejected=rejected+{?} WHERE uid=? AND id=? IF EXISTS";
        let params = (
            assignee.to_cql(),
            assignee.to_cql(),
            self.uid.to_cql(),
            self.id.to_cql(),
        );
        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Task update_rejected failed, please try again".to_string(),
            )
            .into());
        }

        self.get_one(
            db,
            vec![
                "threshold".to_string(),
                "status".to_string(),
                "resolved".to_string(),
                "rejected".to_string(),
            ],
        )
        .await?;

        let can_approve = self.approvers.is_empty() || self.approvers.contains(&assignee);
        if self.status != -1
            && can_approve
            && self.rejected.len() >= self.threshold as usize
            && self.rejected.len() > self.resolved.len()
        {
            let query = "UPDATE task SET status=? WHERE uid=? AND id=? IF EXISTS";
            let params = (-1i8, self.uid.to_cql(), self.id.to_cql());
            let res = db.execute(query, params).await?;
            if !extract_applied(res) {
                return Err(HTTPError::new(
                    409,
                    "Task update_rejected failed, please try again".to_string(),
                )
                .into());
            }
        }
        Ok(true)
    }

    pub async fn delete(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        let res = self.get_one(db, Vec::new()).await;
        if res.is_err() {
            return Ok(false); // already deleted
        }

        let query = "DELETE FROM task WHERE uid=? AND id=?";
        let params = (self.uid.to_cql(), self.id.to_cql());
        let _ = db.execute(query, params).await?;
        Ok(true)
    }

    pub async fn batch_delete_by_uid(
        db: &scylladb::ScyllaDB,
        uid: xid::Id,
        status: Option<i8>,
    ) -> anyhow::Result<()> {
        match status {
            Some(status) => {
                let query = "DELETE FROM task WHERE uid=? AND status=?";
                let params = (uid.to_cql(), status);
                let _ = db.execute(query, params).await?;
            }
            None => {
                let query = "DELETE FROM task WHERE uid=?";
                let params = (uid.to_cql(),);
                let _ = db.execute(query, params).await?;
            }
        }

        Ok(())
    }

    pub async fn list(
        db: &scylladb::ScyllaDB,
        uid: xid::Id,
        select_fields: Vec<String>,
        page_size: u16,
        page_token: Option<xid::Id>,
        status: Option<i8>,
    ) -> anyhow::Result<Vec<Task>> {
        let fields = Self::select_fields(select_fields, true)?;

        let rows = if let Some(id) = page_token {
            if status.is_none() {
                let query = scylladb::Query::new(format!(
                    "SELECT {} FROM task WHERE uid=? AND id<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(",")
                ))
                .with_page_size(page_size as i32);
                let params = (uid.to_cql(), id.to_cql(), page_size as i32);
                db.execute_paged(query, params, None).await?
            } else {
                let query = scylladb::Query::new(format!(
                    "SELECT {} FROM task WHERE uid=? AND status=? AND id<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(","))).with_page_size(page_size as i32);
                let params = (uid.to_cql(), id.to_cql(), status.unwrap(), page_size as i32);
                db.execute_paged(query, params, None).await?
            }
        } else if status.is_none() {
            let query = scylladb::Query::new(format!(
                "SELECT {} FROM task WHERE uid=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (uid.to_cql(), page_size as i32);
            db.execute_iter(query, params).await?
        } else {
            let query = scylladb::Query::new(format!(
                "SELECT {} FROM task WHERE uid=? AND status=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (uid.as_bytes(), status.unwrap(), page_size as i32);
            db.execute_iter(query, params).await?
        };

        let mut res: Vec<Task> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut doc = Task::default();
            let mut cols = ColumnsMap::with_capacity(fields.len());
            cols.fill(row, &fields)?;
            doc.fill(&cols);
            doc._fields = fields.clone();
            res.push(doc);
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::OnceCell;

    use crate::conf;

    use super::*;

    static DB: OnceCell<scylladb::ScyllaDB> = OnceCell::const_new();

    async fn get_db() -> scylladb::ScyllaDB {
        let cfg = conf::Conf::new().unwrap_or_else(|err| panic!("config error: {}", err));
        let res = scylladb::ScyllaDB::new(cfg.scylla, "logbase_test").await;
        res.unwrap()
    }

    // #[tokio::test(flavor = "current_thread")]
    // #[ignore]
    // async fn task_model_works() {
    // }
}
