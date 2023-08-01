use axum::{extract::State, Extension};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use validator::Validate;

use axum_web::context::ReqContext;
use axum_web::erring::{HTTPError, SuccessResponse};
use axum_web::object::PackObject;

use crate::db;

use crate::api::{token_from_xid, token_to_xid, AppState, Pagination};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct NotificationOutput {
    pub sender: PackObject<xid::Id>,
    pub tid: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    pub status: i8,
    pub ack_status: i8,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duedate: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approvers: Option<Vec<PackObject<xid::Id>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignees: Option<Vec<PackObject<xid::Id>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved: Option<Vec<PackObject<xid::Id>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejected: Option<Vec<PackObject<xid::Id>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<PackObject<Vec<u8>>>,
}

impl NotificationOutput {
    pub fn from<T>(val: db::Task, ack_status: i8, to: &PackObject<T>) -> Self {
        let mut rt = Self {
            sender: to.with(val.uid),
            tid: to.with(val.id),
            gid: to.with(val.gid),
            status: val.status,
            ack_status,
            kind: val.kind,
            ..Default::default()
        };

        for v in val._fields {
            match v.as_str() {
                "created_at" => rt.created_at = Some(val.created_at),
                "updated_at" => rt.updated_at = Some(val.updated_at),
                "duedate" => rt.duedate = Some(val.duedate),
                "threshold" => rt.threshold = Some(val.threshold),
                "approvers" => {
                    rt.approvers = Some(
                        val.approvers
                            .iter()
                            .map(|id| to.with(id.to_owned()))
                            .collect(),
                    )
                }
                "assignees" => {
                    rt.assignees = Some(
                        val.assignees
                            .iter()
                            .map(|id| to.with(id.to_owned()))
                            .collect(),
                    )
                }
                "resolved" => {
                    rt.resolved = Some(
                        val.resolved
                            .iter()
                            .map(|id| to.with(id.to_owned()))
                            .collect(),
                    )
                }
                "rejected" => {
                    rt.rejected = Some(
                        val.rejected
                            .iter()
                            .map(|id| to.with(id.to_owned()))
                            .collect(),
                    )
                }
                "message" => rt.message = Some(val.message.to_owned()),
                "payload" => rt.payload = Some(to.with(val.payload.to_owned())),
                _ => {}
            }
        }

        rt
    }
}

#[derive(Debug, Deserialize, Validate)]
pub struct DeleteNotificationInput {
    pub uid: PackObject<xid::Id>,
    pub tid: Option<PackObject<xid::Id>>,
    pub sender: Option<PackObject<xid::Id>>,
    pub status: Option<i8>,
}

pub async fn delete(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<DeleteNotificationInput>,
) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let tid = input
        .tid
        .ok_or_else(|| HTTPError::new(400, "Missing required field `tid`".to_string()))?;
    let sender = input
        .sender
        .ok_or_else(|| HTTPError::new(400, "Missing required field `sender`".to_string()))?;

    ctx.set_kvs(vec![
        ("action", "delete_notification".into()),
        ("uid", input.uid.to_string().into()),
        ("tid", tid.to_string().into()),
        ("sender", sender.to_string().into()),
    ])
    .await;

    let mut doc = db::Notification::with_pk(input.uid.unwrap(), tid.unwrap(), sender.unwrap());
    doc.delete(&app.scylla).await?;

    Ok(to.with(SuccessResponse::new(true)))
}

pub async fn batch_delete(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<DeleteNotificationInput>,
) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    ctx.set_kvs(vec![
        ("action", "batch_delete_notification".into()),
        ("uid", input.uid.to_string().into()),
    ])
    .await;

    if let Some(status) = input.status {
        ctx.set("status", status.into()).await
    }

    db::Notification::batch_delete_by_uid(&app.scylla, input.uid.unwrap(), input.status).await?;

    Ok(to.with(SuccessResponse::new(true)))
}

pub async fn list(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<Pagination>,
) -> Result<PackObject<SuccessResponse<Vec<NotificationOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let page_size = input.page_size.unwrap_or(10);
    ctx.set_kvs(vec![
        ("action", "list_notification".into()),
        ("uid", input.uid.to_string().into()),
        ("page_size", page_size.into()),
    ])
    .await;

    let fields = input.fields.unwrap_or_default();
    let res = db::Notification::list(
        &app.scylla,
        input.uid.unwrap(),
        page_size,
        token_to_xid(&input.page_token),
        input.status,
    )
    .await?;
    let next_page_token = if res.len() >= page_size as usize {
        to.with_option(token_from_xid(res.last().unwrap().tid))
    } else {
        None
    };

    let mut output: Vec<NotificationOutput> = Vec::with_capacity(res.len());
    for notiy in res {
        let mut task = db::Task::with_pk(notiy.sender, notiy.tid);
        task.get_one(&app.scylla, fields.clone()).await?;
        output.push(NotificationOutput::from(task, notiy.status, &to));
    }

    Ok(to.with(SuccessResponse {
        total_size: None,
        next_page_token,
        result: output,
    }))
}
