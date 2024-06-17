use std::{collections::HashSet, time::Duration};

use futures_util::stream::StreamExt;

use chrono::serde::ts_milliseconds;
use derive_getters::Getters;

use mongodb::{
    bson::{self, doc, oid::ObjectId, Document},
    options::{ChangeStreamOptions, FullDocumentType},
    Collection,
};
use structopt::StructOpt;
use tokio::time;

const UPDATES_NUM: usize = 1000;
// const PRODUCE_DELAY: Option<Duration> = Some(time::Duration::from_secs(10));
const PRODUCE_DELAY: Option<Duration> = None;
const CONSUME_DELAY: Option<Duration> = Some(time::Duration::from_secs(10));
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
struct Cid(String);

impl From<uuid::Uuid> for Cid {
    fn from(uuid: uuid::Uuid) -> Self {
        Cid(uuid.to_string())
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "tag", content = "content")]
enum Status {
    Pending(Desitnation),
    Approved(Approver),
    Processed,
    Finalized,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum StatusQuery {
    Pending,
    Approved,
    Processed,
    Finalized,
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
}

impl Request {
    pub(crate) fn new(payload: impl Into<String>) -> Self {
        Request {
            oid: ObjectId::new(),
            cid: uuid::Uuid::new_v4().into(),
            accepted_at: chrono::Utc::now(),
            payload: payload.into(),
            status: Status::Pending(Desitnation("sender".to_string())),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_cid(cid: impl Into<Cid>, payload: impl Into<String>) -> Self {
        Request {
            oid: ObjectId::new(),
            cid: cid.into(),
            accepted_at: chrono::Utc::now(),
            payload: payload.into(),
            status: Status::Pending(Desitnation("sender".to_string())),
        }
    }
}

async fn produce(collection: Collection<Request>) {
    if let Some(delay) = PRODUCE_DELAY {
        log::info!("Delaying producer for {:?}", delay);
        time::sleep(delay).await;
    }
    log::info!("Producing data");
    let start = time::Instant::now();
    for i in 0..UPDATES_NUM {
        let payload = generate_string_of_byte_length(PAYLOAD_SIZE_BYTES);
        let data = Request::new(payload);
        let updated_document = bson::to_document(&data).unwrap();
        log::info!("Producing data: {:?}", updated_document);
        collection.insert_one(data, None).await.unwrap();
        log::info!("Produced {i}");
        if let Some(sleep) = PRODUCE_SLEEP {
            time::sleep(sleep).await;
        }
    }
    let elapsed = start.elapsed();
    log::info!("Data produced, elapsed: {:?}", elapsed);
}

async fn consume_created(
    collection: Collection<Request>,
    from: StatusQuery,
    to: StatusQuery,
    update: Status,
) {
    let pipeline = vec![doc! { "$match" : doc! { "operationType" : "insert" } }];
    watch_and_update(collection, pipeline, from, to, update).await;
}

async fn consume_updated(
    collection: Collection<Request>,
    from: StatusQuery,
    to: StatusQuery,
    update: Status,
) {
    let from_status = bson::ser::to_bson(&from).unwrap();
    let name = from_status;
    let pipeline = vec![doc! {
    "$match": {
                 "$and": [
                 // DD: for simple cases, when status is a value
                 { "updateDescription.updatedFields.status.tag": { "$eq":  name } },
                 { "operationType": "update" },
                 ]
         }
      }];
    watch_and_update(collection, pipeline, from, to, update).await;
}

async fn watch_and_update(
    collection: Collection<Request>,
    pipeline: Vec<Document>,
    _from: StatusQuery,
    _to: StatusQuery,
    update: Status,
) {
    if let Some(delay) = CONSUME_DELAY {
        log::info!("Delaying consumer for {:?}", delay);
        time::sleep(delay).await;
    }

    let full_doc = Some(FullDocumentType::UpdateLookup);
    let opts = ChangeStreamOptions::builder()
        .full_document(full_doc)
        .build();

    let mut hash_set = HashSet::new();

    let mut update_change_stream = collection.watch(pipeline, opts).await.unwrap();
    log::info!("Watching for updates");

    let from_status = bson::ser::to_bson(&_from).unwrap();
    let name = from_status;
    let filter = doc! {"status.tag": name};
    let mut pre_watched_data = collection.find(filter, None).await.unwrap();

    let mut i = 0;
    while let Some(doc) = pre_watched_data.next().await.transpose().unwrap() {
        hash_set.insert(doc.cid().to_owned());
        update_status(doc, update.clone(), collection.clone()).await;
        i += 1;
    }

    log::info!("Pre-watched data updated: {:?}", i);

    let mut i = 0;
    while let Some(event) = update_change_stream.next().await.transpose().unwrap() {
        log::debug!(
            "Update performed: {:?}, full document: {:?}",
            event.update_description,
            event.full_document
        );

        // DD: Place holder for the actual business logic - START

        let updated = event.full_document.unwrap();
        if !hash_set.is_empty() {
            if let Some(id) = hash_set.get(&updated.cid()) {
                log::info!(
                    "Already processed: {:?}, hash_set size: {}",
                    id,
                    hash_set.len()
                );
                continue;
            } else {
                log::info!("No hash_set entry for: {:?}, clear cache", updated.cid());
                hash_set.clear();
            }
        }
        update_status(updated, update.clone(), collection.clone()).await;

        i += 1;
        if i >= UPDATES_NUM {
            log::info!("Processed all updates");
            break;
        }
    }
    log::info!("Consumed {i}, no more updates");
}

async fn update_status(
    mut to_update: Request,
    update: Status,
    collection: Collection<Request>,
) -> Request {
    to_update.status = update;

    let updated_document = bson::to_document(&to_update).unwrap();
    let query = doc! { "_id" : to_update.oid() };
    let updated = doc! {
        "$set": updated_document
    };
    log::debug!("Document updated: {:?}", updated);
    collection.update_one(query, updated, None).await.unwrap();

    to_update
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
        "producer" => produce(collection).await,
        "consumer1" => {
            consume_created(
                collection,
                StatusQuery::Pending,
                StatusQuery::Approved,
                // Status::Pending(Desitnation("".to_string())),
                Status::Approved(Approver("approver".to_string())),
            )
            .await
        }
        "consumer2" => {
            consume_updated(
                collection,
                StatusQuery::Approved,
                StatusQuery::Processed,
                // Status::Approved(Approver("approver".to_string())),
                Status::Processed,
            )
            .await
        }
        "consumer3" => {
            consume_updated(
                collection,
                StatusQuery::Processed,
                StatusQuery::Finalized,
                Status::Finalized,
            )
            .await
        }
        _ => panic!("Invalid type provided."),
    }
}
