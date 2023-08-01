use axum_web::erring::HTTPError;

use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;

use crate::db::scylladb::{self, extract_applied};

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct GroupNotification {
    pub gid: xid::Id,
    pub tid: xid::Id,
    pub sender: xid::Id,
    pub role: i8,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct Notification {
    pub uid: xid::Id,
    pub tid: xid::Id,
    pub sender: xid::Id,
    pub status: i8,
    pub message: String,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

impl GroupNotification {
    pub fn with_pk(gid: xid::Id, tid: xid::Id, sender: xid::Id) -> Self {
        Self {
            gid,
            tid,
            sender,
            ..Default::default()
        }
    }

    pub async fn get_one(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<()> {
        let fields = Self::fields();
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM group_notification WHERE gid=? AND tid=? AND sender=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.gid.to_cql(), self.tid.to_cql(), self.sender.to_cql());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
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
            "INSERT INTO group_notification ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "GroupNotification save failed, please try again".to_string(),
            )
            .into());
        }

        Ok(true)
    }

    pub async fn delete(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<()> {
        let query = "DELETE FROM group_notification WHERE gid=? AND tid=? AND sender=?";
        let params = (self.gid.to_cql(), self.tid.to_cql(), self.sender.to_cql());
        let _ = db.execute(query, params).await?;
        Ok(())
    }

    pub async fn list(
        db: &scylladb::ScyllaDB,
        gid: xid::Id,
        page_size: u16,
        page_token: Option<xid::Id>,
        role: Option<i8>,
    ) -> anyhow::Result<Vec<GroupNotification>> {
        let fields = Self::fields();

        let rows = if let Some(tid) = page_token {
            if role.is_none() {
                let query = scylladb::Query::new(format!(
                    "SELECT {} FROM group_notification WHERE gid=? AND tid<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(",")
                ))
                .with_page_size(page_size as i32);
                let params = (gid.to_cql(), tid.to_cql(), page_size as i32);
                db.execute_paged(query, params, None).await?
            } else {
                let query = scylladb::Query::new(format!(
                    "SELECT {} FROM group_notification WHERE gid=? AND role=? AND tid<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(","))).with_page_size(page_size as i32);
                let params = (gid.to_cql(), tid.to_cql(), role.unwrap(), page_size as i32);
                db.execute_paged(query, params, None).await?
            }
        } else if role.is_none() {
            let query = scylladb::Query::new(format!(
                "SELECT {} FROM group_notification WHERE gid=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (gid.to_cql(), page_size as i32);
            db.execute_iter(query, params).await?
        } else {
            let query = scylladb::Query::new(format!(
                "SELECT {} FROM group_notification WHERE gid=? AND role=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (gid.as_bytes(), role.unwrap(), page_size as i32);
            db.execute_iter(query, params).await?
        };

        let mut res: Vec<GroupNotification> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut doc = GroupNotification::default();
            let mut cols = ColumnsMap::with_capacity(fields.len());
            cols.fill(row, &fields)?;
            doc.fill(&cols);
            doc._fields = fields.clone();
            res.push(doc);
        }

        Ok(res)
    }
}

impl Notification {
    pub fn with_pk(uid: xid::Id, tid: xid::Id, sender: xid::Id) -> Self {
        Self {
            uid,
            tid,
            sender,
            ..Default::default()
        }
    }

    pub async fn get_one(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<()> {
        let fields = Self::fields();
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM notification WHERE uid=? AND tid=? AND sender=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.uid.to_cql(), self.tid.to_cql(), self.sender.to_cql());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
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
            "INSERT INTO notification ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Notification save failed, please try again".to_string(),
            )
            .into());
        }

        Ok(true)
    }

    pub async fn update(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        let query = "UPDATE notification SET status=?,message=? WHERE uid=? AND tid=? AND sender=? IF EXISTS";
        let params = (
            self.status,
            self.message.to_cql(),
            self.uid.to_cql(),
            self.tid.to_cql(),
            self.sender.to_cql(),
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Notification update failed, please try again".to_string(),
            )
            .into());
        }
        Ok(true)
    }

    pub async fn delete(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<()> {
        let query = "DELETE FROM notification WHERE uid=? AND tid=? AND sender=?";
        let params = (self.uid.to_cql(), self.tid.to_cql(), self.sender.to_cql());
        let _ = db.execute(query, params).await?;
        Ok(())
    }

    pub async fn batch_delete_by_tid(db: &scylladb::ScyllaDB, tid: xid::Id) -> anyhow::Result<()> {
        let query = scylladb::Query::new("SELECT uid,tid,sender FROM notification WHERE tid=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s")
            .with_page_size(1000_i32);
        let params = (tid.to_cql(), 1000_i32);
        let fields = vec!["uid".to_string(), "tid".to_string(), "sender".to_string()];

        loop {
            let rows = db.execute_iter(query.clone(), params.clone()).await?;
            if rows.is_empty() {
                break;
            }

            for row in rows {
                let mut doc = Notification::default();
                let mut cols = ColumnsMap::with_capacity(3);
                cols.fill(row, &fields)?;
                doc.fill(&cols);
                let _ = doc.delete(db).await;
            }
        }

        Ok(())
    }

    pub async fn batch_delete_by_uid(
        db: &scylladb::ScyllaDB,
        uid: xid::Id,
        status: Option<i8>,
    ) -> anyhow::Result<()> {
        match status {
            Some(status) => {
                let query = "DELETE FROM notification WHERE uid=? AND status=?";
                let params = (uid.to_cql(), status);
                let _ = db.execute(query, params).await?;
            }
            None => {
                let query = "DELETE FROM notification WHERE uid=?";
                let params = (uid.to_cql(),);
                let _ = db.execute(query, params).await?;
            }
        }

        Ok(())
    }

    pub async fn list(
        db: &scylladb::ScyllaDB,
        uid: xid::Id,
        page_size: u16,
        page_token: Option<xid::Id>,
        status: Option<i8>,
    ) -> anyhow::Result<Vec<Notification>> {
        let fields = Self::fields();

        let rows = if let Some(tid) = page_token {
            if status.is_none() {
                let query = scylladb::Query::new(format!(
                    "SELECT {} FROM notification WHERE uid=? AND tid<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(",")
                ))
                .with_page_size(page_size as i32);
                let params = (uid.to_cql(), tid.to_cql(), page_size as i32);
                db.execute_paged(query, params, None).await?
            } else {
                let query = scylladb::Query::new(format!(
                    "SELECT {} FROM notification WHERE uid=? AND status=? AND tid<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(","))).with_page_size(page_size as i32);
                let params = (
                    uid.to_cql(),
                    tid.to_cql(),
                    status.unwrap(),
                    page_size as i32,
                );
                db.execute_paged(query, params, None).await?
            }
        } else if status.is_none() {
            let query = scylladb::Query::new(format!(
                "SELECT {} FROM notification WHERE uid=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (uid.to_cql(), page_size as i32);
            db.execute_iter(query, params).await?
        } else {
            let query = scylladb::Query::new(format!(
                "SELECT {} FROM notification WHERE uid=? AND status=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (uid.as_bytes(), status.unwrap(), page_size as i32);
            db.execute_iter(query, params).await?
        };

        let mut res: Vec<Notification> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut doc = Notification::default();
            let mut cols = ColumnsMap::with_capacity(fields.len());
            cols.fill(row, &fields)?;
            doc.fill(&cols);
            doc._fields = fields.clone();
            res.push(doc);
        }

        Ok(res)
    }
}
