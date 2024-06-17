use std::{
    collections::HashSet,
    fmt::Debug,
    marker::{Send, Sync, Unpin},
    time::Duration,
};

use futures_util::stream::StreamExt;

use chrono::serde::ts_milliseconds;
use derive_getters::Getters;

use mongodb::{
    bson::{self, doc, oid::ObjectId, Document},
    options::{ChangeStreamOptions, FullDocumentType},
    Collection,
};
use serde::{de::DeserializeOwned, Serialize};
use structopt::StructOpt;
use tokio::time;

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

impl serde::Serialize for Cid {
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Approver(String);

impl From<String> for Approver {
    fn from(s: String) -> Self {
        Self(s)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Desitnation(String);

impl From<String> for Desitnation {
    fn from(s: String) -> Self {
        Self(s)
    }
}

trait StatusQueryT: serde::Serialize + DeserializeOwned + Debug {}

trait StatusT: serde::Serialize + DeserializeOwned + Debug {
    type Query: StatusQueryT;

    fn to_query(&self) -> Self::Query;
    // fn tag(&self) -> &str;
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
    fn accepted_at(&self) -> &chrono::DateTime<chrono::Utc>;
    fn status(&self) -> &Self::Status;
    fn payload(&self) -> &Self::Payload;
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Getters)]
#[serde(rename_all = "snake_case")]
// #[derive(new)]
struct Request {
    // Internal data, not exposed to the outside users.
    // It shall be used only for internal purposes.
    // #[getter(skip)]
    #[serde(rename = "_id")]
    oid: ObjectId,

    #[serde(with = "ts_milliseconds")]
    accepted_at: chrono::DateTime<chrono::Utc>,

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
            accepted_at: chrono::Utc::now(),
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
            accepted_at: chrono::Utc::now(),
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

    fn accepted_at(&self) -> &chrono::DateTime<chrono::Utc> {
        self.accepted_at()
    }

    fn status(&self) -> &Status {
        self.status()
    }

    fn payload(&self) -> &Self::Payload {
        self.payload()
    }
}

struct Producer {
    collection: Collection<Request>,
    payload_size: usize,
    sleep: Option<Duration>,
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
            log::info!("Delaying producer for {:?}", delay);
            time::sleep(delay).await;
        }
        log::info!("Producing data");
        let start = time::Instant::now();
        for i in 0..UPDATES_NUM {
            let payload = generate_string_of_byte_length(self.payload_size);
            let data = Request::new(payload);
            let updated_document = bson::to_document(&data).unwrap();
            log::info!("Producing data: {:?}", updated_document);
            self.collection.insert_one(data, None).await.unwrap();
            log::info!("Produced {i}");
            if let Some(sleep) = self.sleep {
                time::sleep(sleep).await;
            }
        }
        let elapsed = start.elapsed();
        log::info!("Data produced, elapsed: {:?}", elapsed);
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
    async fn handle_update(&self, mut updated: Request) -> Request {
        updated.status = self.update.clone();
        updated
    }
}

struct Consumer<H: HandleUpdate, D: RequestT> {
    collection: Collection<D>,
    pipeline: Vec<Document>,
    handler: H,
}

impl<H, R> Consumer<H, R>
where
    H: HandleUpdate<R = R>,
    R: RequestT,
{
    fn new(collection: Collection<R>, pipeline: Vec<Document>, handler: H) -> Self {
        Self {
            collection,
            pipeline,
            handler,
        }
    }

    async fn handle_update(
        &self,
        updated: R,
        is_prewatched: bool,
        hash_set: &mut HashSet<Cid>,
        ord_time: &mut std::collections::BTreeSet<chrono::DateTime<chrono::Utc>>,
    ) {
        if let Some(accepted_at) = ord_time.last() {
            if updated.accepted_at() < accepted_at {
                log::warn!("Out of order data: {:?}", updated)
            }
        }

        ord_time.insert(updated.accepted_at().to_owned());

        if is_prewatched {
            hash_set.insert(updated.cid().to_owned());
        } else if hash_set.contains(updated.cid()) {
            log::warn!("Duplicate data: {:?}", updated);
            return;
        } else {
            log::info!("Clear hash cache, no duplicates");
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
        log::debug!("Document updated: {:?}", updated_doc);
        self.collection
            .update_one(query, updated_doc, None)
            .await
            .unwrap();
    }

    async fn consume(&self) {
        let full_doc = Some(FullDocumentType::UpdateLookup);
        let opts = ChangeStreamOptions::builder()
            .full_document(full_doc)
            .build();

        let mut update_change_stream = self
            .collection
            .watch(self.pipeline.clone(), opts)
            .await
            .unwrap();

        let from_status = bson::ser::to_bson(self.handler.from()).unwrap();
        let name = from_status;
        let filter = doc! {"status.tag": name};
        let mut pre_watched_data = self.collection.find(filter, None).await.unwrap();

        let mut ord_time = std::collections::BTreeSet::new();

        let mut hash_set = HashSet::new();
        let mut i = 0;
        while let Some(doc) = pre_watched_data.next().await.transpose().unwrap() {
            self.handle_update(doc, true, &mut hash_set, &mut ord_time)
                .await;
            i += 1;
        }

        log::info!("Pre-watched data updated: {:?}", i);

        log::info!("Watching for updates");

        let mut i = 0;
        while let Some(event) = update_change_stream.next().await.transpose().unwrap() {
            log::debug!(
                "Update performed: {:?}, full document: {:?}",
                event.update_description,
                event.full_document
            );
            let updated = event.full_document.unwrap();
            self.handle_update(updated, false, &mut hash_set, &mut ord_time)
                .await;

            i += 1;
            if i >= UPDATES_NUM {
                log::info!("Processed all updates");
                break;
            }
        }
        log::info!("Consumed {i}, no more updates");
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
    env_logger::init();

    let opt = Opt::from_args();
    let client = mongodb::Client::with_uri_str(opt.mongo_uri.as_str())
        .await
        .unwrap();

    let db = client.database("admin");
    while let Err(error) = db.run_command(doc! {"replSetGetStatus": 1}, None).await {
        log::info!("Waiting for MongoDB to be ready, error: {error:?}");
        time::sleep(time::Duration::from_secs(1)).await;
    }

    let db = client.database("db_data");
    let collection = db.collection::<Request>("data");

    log::info!("Connected to MongoDB");

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
            Consumer::new(
                collection,
                vec![doc! {
                    "$match" : doc! { "operationType" : "insert" },
                }],
                handler,
            )
            .consume()
            .await
        }
        "consumer2" => {
            let handler = DemoHandler {
                from: StatusQuery::Approved,
                to: StatusQuery::Processed,
                update: Status::Processed,
            };
            Consumer::new(
                collection,
                vec![doc! {
                    "$match": {
                        "$and": [
                        // DD: for simple cases, when status is a value
                        { "updateDescription.updatedFields.status.tag": { "$eq": "approved" } },
                        { "operationType": "update" },
                        ]
                },
                }],
                handler,
            )
            .consume()
            .await
        }
        "consumer3" => {
            let handler = DemoHandler {
                from: StatusQuery::Processed,
                to: StatusQuery::Finalized,
                update: Status::Finalized,
            };
            Consumer::new(
                collection,
                vec![doc! {
                    "$match": {
                        "$and": [
                        // DD: for simple cases, when status is a value
                        { "updateDescription.updatedFields.status.tag": { "$eq": "processed" } },
                        { "operationType": "update" },
                        ]
                },
                }],
                handler,
            )
            .consume()
            .await
        }
        _ => panic!("Invalid type provided."),
    }
}
