use axum::{middleware, routing, Router};
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::{
    catch_panic::CatchPanicLayer,
    compression::{predicate::SizeAbove, CompressionLayer},
};

use axum_web::context;
use axum_web::encoding;

use crate::api;
use crate::conf;
use crate::db;

pub async fn new(cfg: conf::Conf) -> anyhow::Result<(Arc<api::AppState>, Router)> {
    let app_state = Arc::new(new_app_state(cfg).await?);

    let mds = ServiceBuilder::new()
        .layer(CatchPanicLayer::new())
        .layer(middleware::from_fn(context::middleware))
        .layer(CompressionLayer::new().compress_when(SizeAbove::new(encoding::MIN_ENCODING_SIZE)));

    let app = Router::new()
        .route("/", routing::get(api::version))
        .route("/healthz", routing::get(api::healthz))
        .nest(
            "/v1/task",
            Router::new()
                .route("/", routing::post(api::task::create).get(api::task::get))
                .route("/ack", routing::patch(api::task::ack))
                .route("/list", routing::post(api::task::list))
                .route("/delete", routing::post(api::task::delete)),
            // .route("/batch_delete", routing::post(api::task::batch_delete)),
        )
        .nest(
            "/v1/notification",
            Router::new()
                .route("/list", routing::post(api::notification::list))
                .route("/delete", routing::post(api::notification::delete))
                .route(
                    "/batch_delete",
                    routing::post(api::notification::batch_delete),
                ),
        )
        .route_layer(mds)
        .with_state(app_state.clone());

    Ok((app_state, app))
}

async fn new_app_state(cfg: conf::Conf) -> anyhow::Result<api::AppState> {
    let keyspace = if cfg.env == "test" {
        "taskbase_test"
    } else {
        "taskbase"
    };
    let scylla = db::scylladb::ScyllaDB::new(cfg.scylla, keyspace).await?;
    Ok(api::AppState {
        scylla: Arc::new(scylla),
    })
}
