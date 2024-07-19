use mongo_pc::api;

use mongo_pc::handle;
use mongo_pc::handle::Handle;
use mongo_pc::watch::filter::Filter;
use mongo_pc::watch::{Watch, Watcher};

use std::{fmt::Debug, time::Duration};

use api::cid::Cid;

use chrono::{serde::ts_milliseconds, DateTime, Utc};
use derive_getters::Getters;

use mongo_pc::process::Process;
use mongo_pc::produce;
use mongo_pc::produce::{Produce, RequestT};
use mongo_pc::request::{StatusQueryT, StatusT};
use mongodb::bson::{self, doc, oid::ObjectId};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tokio::time;

use tracing::{self};
use tracing_subscriber::EnvFilter;

const UPDATES_NUM: usize = 1000;

const PRODUCE_DELAY: Option<Duration> = None;
// const CONSUME_DELAY: Option<Duration> = Some(time::Duration::from_secs(10));
const PRODUCE_SLEEP: Option<Duration> = Some(Duration::from_millis(100));
const PAYLOAD_SIZE_BYTES: usize = 1024;

#[derive(Debug, StructOpt)]
#[structopt(name = "mongo_pc", about = "Mongo throughput test")]
struct Opt {
    #[structopt(short = "t", long = "type", possible_values = &["producer", "consumer1", "consumer2", "consumer3", "subscriber"])]
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
struct Sender(String);

impl From<String> for Sender {
    fn from(s: String) -> Self {
        Self(s)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "tag", content = "content")]
enum Status {
    Pending(Sender),
    Approved(Approver),
    Declined(Approver),
    Processed,
    FailedToProcess(String),
    Finalized,
}

impl StatusT for Status {
    type Query = StatusQuery;

    fn to_query(&self) -> Self::Query {
        match self {
            Status::Pending(_) => StatusQuery::Pending,
            Status::Approved(_) => StatusQuery::Approved,
            Status::Declined(_) => StatusQuery::Declined,
            Status::Processed => StatusQuery::Processed,
            Status::FailedToProcess(_) => StatusQuery::FailedToProcess,
            Status::Finalized => StatusQuery::Finalized,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum StatusQuery {
    Pending,
    Approved,
    Declined,
    Processed,
    FailedToProcess,
    Finalized,
}

impl StatusQueryT for StatusQuery {
    fn query_id(&self) -> &str {
        "status.tag"
    }
}

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
            status: Status::Pending(Sender("sender".to_string())),
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
            status: Status::Pending(Sender("sender".to_string())),
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
    to: Vec<StatusQuery>,
    update: Status,
}

#[async_trait::async_trait]
impl Process for DemoHandler {
    type R = Request;

    fn from(&self) -> &StatusQuery {
        &self.from
    }

    fn to(&self) -> &[StatusQuery] {
        &self.to
    }

    async fn process(&self, updated: Self::R) -> Self::R {
        let mut updated = updated;
        updated.status = self.update.clone();
        tracing::trace!("Req unique data: {}", updated.unique_req_data());
        updated
    }
}

struct UnstableProcessor {
    from: StatusQuery,
    to: Vec<StatusQuery>,
}

#[async_trait::async_trait]
impl Process for UnstableProcessor {
    type R = Request;

    fn from(&self) -> &StatusQuery {
        &self.from
    }

    fn to(&self) -> &[StatusQuery] {
        &self.to
    }

    async fn process(&self, updated: Self::R) -> Self::R {
        let mut updated = updated;
        if updated.accepted_at().timestamp() % 2 == 0 {
            updated.status = Status::Processed;
        } else {
            updated.status = Status::FailedToProcess("failed".to_string());
        }
        tracing::trace!("Req unique data: {}", updated.unique_req_data());
        updated
    }
}

struct DemoSubscriber {}

#[async_trait::async_trait]
impl Handle<Request> for DemoSubscriber {
    async fn handle(&self, updated: Request) {
        tracing::info!("Received: status: {:?}", updated.status());
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
        if let Some(sleep) = PRODUCE_SLEEP {
            tracing::info!("Producer sleep for {:?}", sleep);
            time::sleep(sleep).await;
        }
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
            let processor = DemoHandler {
                from: StatusQuery::Pending,
                to: vec![StatusQuery::Approved],
                update: Status::Approved(Approver("approver".to_string())),
            };

            let filter = Filter::builder().with_insert(processor.from().to_owned());
            let handler = handle::handler::Handler::new(collection.clone(), processor);

            let x = Watcher::new(collection, filter.build(), handler);
            x.watch().await;
        }
        "consumer2" => {
            let processor = UnstableProcessor {
                from: StatusQuery::Approved,
                to: vec![StatusQuery::Processed, StatusQuery::FailedToProcess],
            };

            let filter = Filter::builder().with_update(processor.from().to_owned());

            let handler = handle::handler::Handler::new(collection.clone(), processor);

            let x = Watcher::new(collection, filter.build(), handler);
            x.watch().await;
        }
        "consumer3" => {
            let processor = DemoHandler {
                from: StatusQuery::Processed,
                to: vec![StatusQuery::Finalized],
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

            let from_status = bson::ser::to_bson(processor.from()).unwrap();
            let custom_prewatch_filter = doc! {"status.tag": from_status};

            let watch_pipeline = Filter::builder()
                .with_raw_watcher_pipeline(raw_pipeline)
                .with_raw_pre_watcher_filter(custom_prewatch_filter);

            let handler = handle::handler::Handler::new(collection.clone(), processor);

            let x = Watcher::new(collection, watch_pipeline.build(), handler);
            x.watch().await;
        }
        "subscriber" => {
            let subsriber = DemoSubscriber {};
            let raw_pipeline = vec![doc! {
                "$match": {
                    "$or": [
                        { "operationType": "insert" },
                        { "operationType": "update" },
                    ]
                },
            }];

            let custom_prewatch_filter = doc! {"status.tag": "exists"};

            let watch_pipeline = Filter::builder()
                .with_raw_watcher_pipeline(raw_pipeline)
                .with_raw_pre_watcher_filter(custom_prewatch_filter);

            let x = Watcher::new(collection, watch_pipeline.build(), subsriber);
            x.watch().await;
        }
        _ => panic!("Invalid type provided."),
    }
}
