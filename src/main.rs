use std::{
    collections::{BTreeSet, HashSet},
    fmt::Debug,
    marker::{Send, Sync, Unpin},
    time::Duration,
};

use futures_util::stream::StreamExt;

use chrono::{serde::ts_milliseconds, DateTime, Utc};
use derive_getters::Getters;

use mongodb::{
    bson::{self, doc, oid::ObjectId, Document},
    options::{ChangeStreamOptions, FullDocumentType},
    Collection,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use structopt::StructOpt;
use tokio::time;

use tracing::{self, span, Level};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Cid(
    // simple: 284c153cc5a642c19555ad27d1792428
    // braced: {27288856-974f-445e-9d88-ca65472a675b}
    // urn: urn:uuid:582205a5-445b-4a48-af37-0cd266a2ee3a
    // compact: bytes
    uuid::Uuid,
);

impl std::fmt::Display for Cid {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Serialize for Cid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.0.to_string().as_str())
    }
}

impl<'de> serde::Deserialize<'de> for Cid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(CidVisitor)
    }
}
struct CidVisitor;

impl<'de> serde::de::Visitor<'de> for CidVisitor {
    type Value = Cid;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a UUID the following format: 3fc178b9-be92-451c-954f-61ef0c6e9bbf")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let uuid = uuid::Uuid::parse_str(value).map_err(serde::de::Error::custom)?;
        Ok(Cid(uuid))
    }
}

impl From<uuid::Uuid> for Cid {
    fn from(uuid: uuid::Uuid) -> Self {
        Cid(uuid)
    }
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

trait StatusQueryT: Serialize + DeserializeOwned + Debug {}

trait StatusT: Serialize + DeserializeOwned + Debug {
    type Query: StatusQueryT;

    fn to_query(&self) -> Self::Query;
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

trait RequestT: DeserializeOwned + Serialize + Unpin + Send + Sync + Debug {
    type Status: StatusT;
    type Payload;

    fn oid(&self) -> &ObjectId;
    fn cid(&self) -> &Cid;
    fn accepted_at(&self) -> &DateTime<Utc>;
    fn status(&self) -> &Self::Status;
    fn payload(&self) -> &Self::Payload;
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

impl From<ApiRequest> for Request {
    fn from(api_request: ApiRequest) -> Self {
        Self::with_cid(api_request.cid, api_request.payload)
    }
}

trait ApiRequestT: Into<Self::Request> + DeserializeOwned + Debug {
    type Request: RequestT;
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Getters)]
struct ApiRequest {
    cid: Cid,
    payload: String,
    _unique_req_data: String,
}

impl ApiRequestT for ApiRequest {
    type Request = Request;
}

trait Produce {
    type ApiRequest: ApiRequestT;
    async fn produce(&self, data: Self::ApiRequest);
}

struct Producer {
    collection: Collection<Request>,
    payload_size: usize,
    sleep: Option<Duration>,
}

impl Produce for Producer {
    type ApiRequest = ApiRequest;

    async fn produce(&self, data: Self::ApiRequest) {
        let span = span!(Level::INFO, "request", cid = data.cid().to_string());
        let _enter = span.enter();

        tracing::trace!("Received  request: {:?}", data);
        let data: Request = data.into();
        self.collection.insert_one(&data, None).await.unwrap();
        tracing::trace!("Produced request: {:?}", data);
    }
}

impl Producer {
    fn new(collection: Collection<Request>, payload_size: usize, sleep: Option<Duration>) -> Self {
        Self {
            collection,
            payload_size,
            sleep,
        }
    }

    async fn produce(&self) {
        if let Some(delay) = PRODUCE_DELAY {
            tracing::info!("Delaying producer for {:?}", delay);
            time::sleep(delay).await;
        }
        tracing::info!("Producing data");
        let start = time::Instant::now();
        for i in 0..UPDATES_NUM {
            let payload = generate_string_of_byte_length(self.payload_size);
            let data = Request::new(payload);
            tracing::trace!("Producing data: {:?}", data);
            self.collection.insert_one(data, None).await.unwrap();
            if i % 100 == 0 {
                tracing::debug!("Produced {i}");
            } else {
                tracing::trace!("Produced {i}");
            }
            if let Some(sleep) = self.sleep {
                time::sleep(sleep).await;
            }
        }
        let elapsed = start.elapsed();
        tracing::info!("Data produced, elapsed: {:?}", elapsed);
    }
}

trait HandleUpdate {
    type R: RequestT;

    // DD: Is it too overengineered?
    fn from(&self) -> &<<Self::R as RequestT>::Status as StatusT>::Query;
    fn to(&self) -> &<<Self::R as RequestT>::Status as StatusT>::Query;

    #[must_use]
    async fn handle_update(&self, updated: Self::R) -> Self::R;
}

struct DemoHandler {
    from: StatusQuery,
    to: StatusQuery,
    update: Status,
}

impl HandleUpdate for DemoHandler {
    type R = Request;
    fn from(&self) -> &StatusQuery {
        &self.from
    }

    fn to(&self) -> &StatusQuery {
        &self.to
    }

    #[must_use]
    async fn handle_update(&self, updated: Request) -> Request {
        let mut updated = updated;
        updated.status = self.update.clone();
        tracing::trace!("Req unique data: {}", updated.unique_req_data());
        updated
    }
}

struct Consumer<H: HandleUpdate, D: RequestT> {
    collection: Collection<D>,
    watch_pipeline: Vec<Document>,
    handler: H,
}

pub(crate) trait Consume {
    async fn consume(&self);
}

impl<H, R> Consume for Consumer<H, R>
where
    H: HandleUpdate<R = R>,
    R: RequestT,
{
    async fn consume(&self) {
        self.consume().await;
    }
}

impl<H, R> Consumer<H, R>
where
    H: HandleUpdate<R = R>,
    R: RequestT,
{
    pub(crate) fn new(
        collection: Collection<R>,
        watch_pipeline: Vec<Document>,
        handler: H,
    ) -> Self {
        Self {
            collection,
            watch_pipeline,
            handler,
        }
    }

    async fn handle_update(
        &self,
        updated: R,
        is_prewatched: bool,
        hash_set: &mut HashSet<Cid>,
        ord_time: &mut BTreeSet<DateTime<Utc>>,
    ) {
        if let Some(accepted_at) = ord_time.last() {
            if updated.accepted_at() < accepted_at {
                tracing::warn!("Out of order data: {:?}", updated)
            }
        }

        ord_time.insert(updated.accepted_at().to_owned());

        if is_prewatched {
            hash_set.insert(updated.cid().to_owned());
        } else if hash_set.contains(updated.cid()) {
            tracing::info!("Ignored duplicate data, cid: {:?}", updated.cid());
            return;
        } else if !hash_set.is_empty() {
            tracing::info!("Clear hash cache, no duplicates");
            hash_set.clear();
        }

        assert_eq!(
            std::mem::discriminant(&updated.status().to_query()),
            std::mem::discriminant(self.handler.from()),
            "Received status: {:?}, expected: {:?}",
            updated.status().to_query(),
            self.handler.from(),
        );

        let updated = self.handler.handle_update(updated).await;

        assert_eq!(
            std::mem::discriminant(&updated.status().to_query()),
            std::mem::discriminant(self.handler.to()),
            "Updated to status: {:?}, expected: {:?}",
            updated.status().to_query(),
            self.handler.to(),
        );

        let updated_document = bson::to_document(&updated).unwrap();
        let query = doc! { "_id" : updated.oid() };
        let updated_doc = doc! {
            "$set": updated_document
        };
        tracing::debug!("Document updated: {:?}", updated_doc);
        self.collection
            .update_one(query, updated_doc, None)
            .await
            .unwrap();
    }

    async fn consume(&self) {
        tracing::debug!("Initiating change stream");
        let full_doc = Some(FullDocumentType::UpdateLookup);
        let opts = ChangeStreamOptions::builder()
            .full_document(full_doc)
            .build();

        let mut update_change_stream = self
            .collection
            .watch(self.watch_pipeline.clone(), opts)
            .await
            .unwrap();

        let from_status = bson::ser::to_bson(self.handler.from()).unwrap();
        let name = from_status;
        let filter = doc! {"status.tag": name};

        tracing::debug!("Initiating pre-watched data, filter: {filter:?}");
        let mut pre_watched_data = self.collection.find(filter, None).await.unwrap();

        let mut ord_time = BTreeSet::new();

        let mut hash_set = HashSet::new();
        let mut i = 0;
        while let Some(doc) = pre_watched_data.next().await.transpose().unwrap() {
            let span = span!(Level::INFO, "request", cid = doc.cid().to_string());
            let _enter = span.enter();

            tracing::trace!("Pre-watched data: {:?}", doc);
            self.handle_update(doc, true, &mut hash_set, &mut ord_time)
                .await;
            i += 1;
        }

        tracing::info!("Pre-watched data updated: {:?}", i);

        tracing::info!("Watching for updates");

        let mut i = 0;
        while let Some(event) = update_change_stream.next().await.transpose().unwrap() {
            tracing::debug!(
                "Update performed: {:?}, full document: {:?}",
                event.update_description,
                event.full_document
            );

            let updated = event.full_document.unwrap();

            let span = span!(Level::INFO, "request", cid = updated.cid().to_string());
            let _enter = span.enter();

            tracing::trace!("Update performed: {:?}", event.update_description);
            self.handle_update(updated, false, &mut hash_set, &mut ord_time)
                .await;

            i += 1;
            if i >= UPDATES_NUM {
                tracing::info!("Processed all updates");
                break;
            }
        }
        tracing::info!("Consumed {i}, no more updates");
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
            Producer::new(collection, PAYLOAD_SIZE_BYTES, PRODUCE_SLEEP)
                .produce()
                .await
        }
        "consumer1" => {
            let handler = DemoHandler {
                from: StatusQuery::Pending,
                to: StatusQuery::Approved,
                update: Status::Approved(Approver("approver".to_string())),
            };
            Consumer::new(collection, WatchPipeline::with_insert().build(), handler)
                .consume()
                .await
        }
        "consumer2" => {
            let handler = DemoHandler {
                from: StatusQuery::Approved,
                to: StatusQuery::Processed,
                update: Status::Processed,
            };
            let from = handler.from();
            let watch_pipeline = WatchPipeline::with_update(from.clone());
            Consumer::new(collection, watch_pipeline.build(), handler)
                .consume()
                .await
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
            Consumer::new(collection, watch_pipeline.build(), handler)
                .consume()
                .await
        }
        _ => panic!("Invalid type provided."),
    }
}
