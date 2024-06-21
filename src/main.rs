pub(crate) mod api;
pub(crate) mod consume;
pub(crate) mod process;
pub(crate) mod produce;
pub(crate) mod request;

use crate::consume::consumer::Consume;

use crate::consume::Consumer;

use std::{fmt::Debug, time::Duration};

use api::cid::Cid;

use chrono::{serde::ts_milliseconds, DateTime, Utc};
use derive_getters::Getters;

use mongodb::bson::{self, doc, oid::ObjectId, Document};
use process::Process;
use produce::{Produce, RequestT};
use request::{StatusQueryT, StatusT};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tokio::time;

use tracing::{self};
use tracing_subscriber::EnvFilter;

const UPDATES_NUM: usize = 1000;
// const PRODUCE_DELAY: Option<Duration> = Some(time::Duration::from_secs(10));
const PRODUCE_DELAY: Option<Duration> = None;
// const CONSUME_DELAY: Option<Duration> = Some(time::Duration::from_secs(10));
const PRODUCE_SLEEP: Option<Duration> = Some(Duration::from_millis(50));
const PAYLOAD_SIZE_BYTES: usize = 1024;

#[derive(Debug, StructOpt)]
#[structopt(name = "mongo_tput", about = "Mongo throughput test")]
struct Opt {
    #[structopt(short = "t", long = "type", possible_values = &["producer", "consumer1", "consumer2", "consumer3"])]
    type_: String,
    #[structopt(short = "m", long = "mongo", default_value = "mongodb://mongodb:27017")]
    mongo_uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Approver(String);

impl From<String> for Approver {
    fn from(s: String) -> Self {
        Self(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Desitnation(String);

impl From<String> for Desitnation {
    fn from(s: String) -> Self {
        Self(s)
    }
}

pub(crate) struct WatchPipeline {
    pipeline: Vec<Document>,
}

impl WatchPipeline {
    pub(crate) fn with_raw_pipeline(pipeline: Vec<Document>) -> Self {
        Self { pipeline }
    }

    pub(crate) fn with_insert() -> Self {
        let pipeline = vec![doc! {
            "$match" : doc! { "operationType" : "insert" },
        }];
        Self { pipeline }
    }

    pub(crate) fn with_update(expected_status: StatusQuery) -> Self {
        let from_status = bson::ser::to_bson(&expected_status).unwrap();
        let name = from_status;
        let pipeline = vec![doc! {
            "$match": {
                "$and": [
                { "updateDescription.updatedFields.status.tag": { "$eq": name} },
                { "operationType": "update" },
                ]
        },
        }];
        Self { pipeline }
    }

    pub(crate) fn build(self) -> Vec<Document> {
        self.pipeline
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "tag", content = "content")]
enum Status {
    Pending(Desitnation),
    Approved(Approver),
    Processed,
    Finalized,
}

impl StatusT for Status {
    type Query = StatusQuery;

    fn to_query(&self) -> Self::Query {
        match self {
            Status::Pending(_) => StatusQuery::Pending,
            Status::Approved(_) => StatusQuery::Approved,
            Status::Processed => StatusQuery::Processed,
            Status::Finalized => StatusQuery::Finalized,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum StatusQuery {
    Pending,
    Approved,
    Processed,
    Finalized,
}

impl StatusQueryT for StatusQuery {}

#[derive(Debug, serde::Serialize, serde::Deserialize, Getters)]
#[serde(rename_all = "snake_case")]
// #[derive(new)]
struct Request {
    // Internal data, not exposed to the outside users.
    // It shall be used only for internal purposes.
    #[serde(rename = "_id")]
    oid: ObjectId,

    #[serde(with = "ts_milliseconds")]
    accepted_at: DateTime<Utc>,

    // DD: received from client
    cid: Cid,
    payload: String,
    status: Status,

    unique_req_data: String,
}

impl Request {
    pub(crate) fn new(payload: impl Into<String>) -> Self {
        let cid: Cid = uuid::Uuid::new_v4().into();
        let unique_req_data = format!("unique_req_data {cid:?}").to_owned();
        Request {
            oid: ObjectId::new(),
            cid,
            accepted_at: Utc::now(),
            payload: payload.into(),
            status: Status::Pending(Desitnation("sender".to_string())),
            unique_req_data,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_cid(cid: impl Into<Cid>, payload: impl Into<String>) -> Self {
        let cid = cid.into();
        let unique_req_data = format!("unique_req_data {cid:?}").to_owned();
        Request {
            oid: ObjectId::new(),
            cid,
            accepted_at: Utc::now(),
            payload: payload.into(),
            status: Status::Pending(Desitnation("sender".to_string())),
            unique_req_data,
        }
    }
}

impl RequestT for Request {
    type Status = Status;
    type Payload = String;
    fn oid(&self) -> &ObjectId {
        self.oid()
    }

    fn cid(&self) -> &Cid {
        self.cid()
    }

    fn accepted_at(&self) -> &DateTime<Utc> {
        self.accepted_at()
    }

    fn status(&self) -> &Status {
        self.status()
    }

    fn payload(&self) -> &Self::Payload {
        self.payload()
    }
}

struct DemoHandler {
    from: StatusQuery,
    to: StatusQuery,
    update: Status,
}

impl Process for DemoHandler {
    type R = Request;
    fn from(&self) -> &StatusQuery {
        &self.from
    }

    fn to(&self) -> &StatusQuery {
        &self.to
    }

    // #[must_use]
    // async fn handle_update(&self, updated: Request) -> Request {
    //     let mut updated = updated;
    //     updated.status = self.update.clone();
    //     tracing::trace!("Req unique data: {}", updated.unique_req_data());
    //     updated
    // }

    async fn process(&self, updated: Self::R) -> Self::R {
        let mut updated = updated;
        updated.status = self.update.clone();
        tracing::trace!("Req unique data: {}", updated.unique_req_data());
        updated
    }
}

fn generate_string_of_byte_length(byte_length: usize) -> String {
    // The sequence to repeat
    const SEQUENCE: &str = "DEADBEEF";
    // Repeat the sequence enough times to exceed the desired byte length
    let repeated_sequence = SEQUENCE.repeat((byte_length / SEQUENCE.len()) + 1);
    // Truncate the string to the exact byte length required
    let result = &repeated_sequence[..byte_length];

    result.to_string()
}

async fn produce(producer: impl Produce<Request = Request>, payload_size: usize) {
    if let Some(delay) = PRODUCE_DELAY {
        tracing::info!("Delaying producer for {:?}", delay);
        time::sleep(delay).await;
    }
    tracing::info!("Producing data");
    let start = time::Instant::now();
    for _i in 0..UPDATES_NUM {
        let payload = generate_string_of_byte_length(payload_size);
        let data = Request::new(payload);
        producer.produce(data).await;
    }
    let elapsed = start.elapsed();
    tracing::info!("Data produced, elapsed: {:?}", elapsed);
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let opt = Opt::from_args();
    let client = mongodb::Client::with_uri_str(opt.mongo_uri.as_str())
        .await
        .unwrap();

    let db = client.database("admin");
    while let Err(error) = db.run_command(doc! {"replSetGetStatus": 1}, None).await {
        tracing::info!("Waiting for MongoDB to be ready, error: {error:?}");
        time::sleep(time::Duration::from_secs(1)).await;
    }

    let db = client.database("db_data");
    let collection = db.collection::<Request>("data");

    tracing::info!("Connected to MongoDB");

    match opt.type_.as_str() {
        "producer" => {
            let producer = produce::Producer::new(collection);
            produce(producer, PAYLOAD_SIZE_BYTES).await;
        }
        "consumer1" => {
            let handler = DemoHandler {
                from: StatusQuery::Pending,
                to: StatusQuery::Approved,
                update: Status::Approved(Approver("approver".to_string())),
            };

            let x = Consumer::new(collection, WatchPipeline::with_insert().build(), handler);
            x.consume().await;
        }
        "consumer2" => {
            let handler = DemoHandler {
                from: StatusQuery::Approved,
                to: StatusQuery::Processed,
                update: Status::Processed,
            };
            let from = handler.from();
            let watch_pipeline = WatchPipeline::with_update(from.clone());
            let x = Consumer::new(collection, watch_pipeline.build(), handler);
            x.consume().await;
        }
        "consumer3" => {
            let handler = DemoHandler {
                from: StatusQuery::Processed,
                to: StatusQuery::Finalized,
                update: Status::Finalized,
            };
            // DD: Example of creating a raw pipeline
            let raw_pipeline = vec![doc! {
                "$match": {
                    "$and": [
                    { "updateDescription.updatedFields.status.tag": { "$eq": "processed" } },
                    { "operationType": "update" },
                    ]
            },
            }];
            let watch_pipeline = WatchPipeline::with_raw_pipeline(raw_pipeline);
            let x = Consumer::new(collection, watch_pipeline.build(), handler);
            x.consume().await;
        }
        _ => panic!("Invalid type provided."),
    }
}
