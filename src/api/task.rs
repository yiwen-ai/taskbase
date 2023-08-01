use axum::{
    extract::{Query, State},
    Extension,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc};
use validator::Validate;

use axum_web::context::{unix_ms, ReqContext};
use axum_web::erring::{HTTPError, SuccessResponse};
use axum_web::object::PackObject;

use crate::db;

use crate::api::{get_fields, token_from_xid, token_to_xid, AppState, Pagination};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct TaskOutput {
    pub uid: PackObject<xid::Id>,
    pub id: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    pub status: i8,
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

impl TaskOutput {
    pub fn from<T>(val: db::Task, to: &PackObject<T>) -> Self {
        let mut rt = Self {
            uid: to.with(val.uid),
            id: to.with(val.id),
            gid: to.with(val.gid),
            status: val.status,
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
pub struct QueryTask {
    pub uid: PackObject<xid::Id>,
    pub id: PackObject<xid::Id>,
    pub fields: Option<String>,
}

pub async fn get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    Query(input): Query<QueryTask>,
) -> Result<PackObject<SuccessResponse<TaskOutput>>, HTTPError> {
    input.validate()?;

    ctx.set_kvs(vec![
        ("action", "get_task".into()),
        ("uid", input.uid.to_string().into()),
        ("id", input.id.to_string().into()),
    ])
    .await;

    let mut doc = db::Task::with_pk(input.uid.unwrap(), input.id.unwrap());
    doc.get_one(&app.scylla, get_fields(input.fields)).await?;

    Ok(to.with(SuccessResponse::new(TaskOutput::from(doc, &to))))
}

#[derive(Debug, Deserialize, Validate)]
pub struct CreateTaskInput {
    pub uid: PackObject<xid::Id>,
    pub gid: PackObject<xid::Id>,
    pub kind: String,
    #[validate(range(min = 0, max = 256))]
    pub threshold: i16,
    #[validate(length(min = 0, max = 4))]
    pub approvers: Vec<PackObject<xid::Id>>,
    #[validate(length(min = 0, max = 256))]
    pub assignees: Vec<PackObject<xid::Id>>,
    pub message: String,
    pub payload: PackObject<Vec<u8>>,
    #[validate(range(min = -1, max = 2))]
    pub group_role: Option<i8>,
}

pub async fn create(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<CreateTaskInput>,
) -> Result<PackObject<SuccessResponse<TaskOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    ctx.set_kvs(vec![
        ("action", "create_task".into()),
        ("uid", input.uid.to_string().into()),
        ("gid", input.gid.to_string().into()),
        ("kind", input.kind.clone().into()),
    ])
    .await;

    let mut doc = db::Task::with_pk(input.uid.unwrap(), xid::new());
    doc.gid = input.gid.unwrap();
    doc.status = 0i8;
    doc.kind = input.kind;
    doc.created_at = unix_ms() as i64;
    doc.updated_at = doc.created_at;
    doc.threshold = input.threshold;
    doc.approvers = input.approvers.into_iter().map(|id| id.unwrap()).collect();
    doc.assignees = input.assignees.into_iter().map(|id| id.unwrap()).collect();
    doc.resolved = HashSet::new();
    doc.rejected = HashSet::new();
    doc.message = input.message;
    doc.payload = input.payload.unwrap();

    doc.save(&app.scylla).await?;

    if let Some(role) = input.group_role {
        let mut notif = db::GroupNotification::with_pk(doc.gid, doc.id, doc.uid);
        notif.role = role;
        let _ = notif.save(&app.scylla).await;
    }
    if !doc.approvers.is_empty() {
        for id in &doc.approvers {
            let mut notif = db::Notification::with_pk(*id, doc.id, doc.uid);
            let _ = notif.save(&app.scylla).await;
        }
    }
    if !doc.assignees.is_empty() {
        for id in &doc.assignees {
            let mut notif = db::Notification::with_pk(*id, doc.id, doc.uid);
            let _ = notif.save(&app.scylla).await;
        }
    }

    Ok(to.with(SuccessResponse::new(TaskOutput::from(doc, &to))))
}

#[derive(Debug, Deserialize, Validate)]
pub struct AckTaskInput {
    pub uid: PackObject<xid::Id>,
    pub tid: PackObject<xid::Id>,
    pub sender: PackObject<xid::Id>,
    #[validate(range(min = -1, max = 1))]
    pub status: i8,
    pub message: String,
}

pub async fn ack(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<AckTaskInput>,
) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    if input.status != -1 && input.status != 1 {
        return Err(HTTPError::new(
            400,
            format!("invalid status, expected -1 or 1, got {}", input.status),
        ));
    }
    ctx.set_kvs(vec![
        ("action", "ack_task".into()),
        ("uid", input.uid.to_string().into()),
        ("tid", input.tid.to_string().into()),
        ("sender", input.sender.to_string().into()),
    ])
    .await;

    let mut doc = db::Notification::with_pk(
        input.uid.unwrap(),
        input.tid.unwrap(),
        input.sender.unwrap(),
    );
    doc.get_one(&app.scylla).await?;
    if doc.status == input.status {
        return Ok(to.with(SuccessResponse::new(false)));
    }

    let mut task = db::Task::with_pk(doc.sender, doc.tid);
    if input.status == 1 {
        task.update_resolved(&app.scylla, doc.uid).await?;
    } else {
        task.update_rejected(&app.scylla, doc.uid).await?;
    }
    doc.status = input.status;
    doc.message = input.message;
    doc.update(&app.scylla).await?;

    Ok(to.with(SuccessResponse::new(true)))
}

#[derive(Debug, Deserialize, Validate)]
pub struct DeleteTaskInput {
    pub uid: PackObject<xid::Id>,
    pub id: Option<PackObject<xid::Id>>,
    pub status: Option<i8>,
}

pub async fn delete(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<DeleteTaskInput>,
) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let id = input
        .id
        .ok_or_else(|| HTTPError::new(400, "Missing required field `id`".to_string()))?;

    ctx.set_kvs(vec![
        ("action", "delete_notification".into()),
        ("uid", input.uid.to_string().into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::Task::with_pk(input.uid.unwrap(), id.unwrap());
    if doc
        .get_one(&app.scylla, vec!["gid".to_string()])
        .await
        .is_err()
    {
        return Ok(to.with(SuccessResponse::new(false)));
    }

    doc.delete(&app.scylla).await?;
    let mut notify = db::GroupNotification::with_pk(doc.gid, doc.id, doc.uid);
    let _ = notify.delete(&app.scylla).await;
    db::Notification::batch_delete_by_tid(&app.scylla, doc.id).await?;

    Ok(to.with(SuccessResponse::new(true)))
}

pub async fn list(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<Pagination>,
) -> Result<PackObject<SuccessResponse<Vec<TaskOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let page_size = input.page_size.unwrap_or(10);
    ctx.set_kvs(vec![
        ("action", "list_task".into()),
        ("uid", input.uid.to_string().into()),
        ("page_size", page_size.into()),
    ])
    .await;

    let fields = input.fields.unwrap_or_default();
    let res = db::Task::list(
        &app.scylla,
        input.uid.unwrap(),
        fields,
        page_size,
        token_to_xid(&input.page_token),
        input.status,
    )
    .await?;
    let next_page_token = if res.len() >= page_size as usize {
        to.with_option(token_from_xid(res.last().unwrap().id))
    } else {
        None
    };

    Ok(to.with(SuccessResponse {
        total_size: None,
        next_page_token,
        result: res
            .iter()
            .map(|r| TaskOutput::from(r.to_owned(), &to))
            .collect(),
    }))
}
