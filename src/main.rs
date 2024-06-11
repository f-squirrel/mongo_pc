use std::time::Duration;

use futures_util::stream::StreamExt;

use derive_getters::Getters;
use mongodb::{
    bson::{self, doc, oid::ObjectId, Document},
    options::{ChangeStreamOptions, FullDocumentType},
    Collection,
};
use structopt::StructOpt;
use tokio::time;

const UPDATES_NUM: usize = 100000;
const PRODUCE_SLEEP: Option<Duration> = Some(Duration::from_millis(50));

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Opt {
    #[structopt(short = "t", long = "type", possible_values = &["producer", "consumer1", "consumer2", "consumer3"])]
    type_: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
struct Receiver(String);

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
struct Data {
    // DD: Do not change it anywhere after creation.
    // We need it to be able to safely update objects
    #[serde(rename = "_id")]
    id: ObjectId,

    cid: Cid,
    payload: String,
    status: Status,
}

impl Data {
    pub(crate) fn new(payload: String) -> Self {
        Data {
            id: ObjectId::new(),
            cid: uuid::Uuid::new_v4().into(),
            payload,
            status: Status::Pending(Desitnation("sender".to_string())),
        }
    }
}

async fn produce(collection: Collection<Data>) {
    time::sleep(time::Duration::from_secs(10)).await;
    log::info!("Producing data");
    // produce
    let start = time::Instant::now();
    for _i in 0..UPDATES_NUM {
        let data = Data::new("data".to_string());
        let updated_document = bson::to_document(&data).unwrap();
        log::info!("Producing data: {:?}", updated_document);
        collection.insert_one(data, None).await.unwrap();
        log::info!("Produced {_i}");
        if let Some(sleep) = PRODUCE_SLEEP {
            time::sleep(sleep).await;
        }
    }
    let elapsed = start.elapsed();
    log::info!("Data produced, elapsed: {:?}", elapsed);
}

async fn consume_created(
    collection: Collection<Data>,
    from: StatusQuery,
    to: StatusQuery,
    update: Status,
) {
    let pipeline = vec![doc! { "$match" : doc! { "operationType" : "insert" } }];
    watch_and_update(collection, pipeline, from, to, update).await;
}

async fn consume_updated(
    collection: Collection<Data>,
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
    collection: Collection<Data>,
    pipeline: Vec<Document>,
    _from: StatusQuery,
    _to: StatusQuery,
    update: Status,
) {
    let full_doc = Some(FullDocumentType::UpdateLookup);
    let opts = ChangeStreamOptions::builder()
        .full_document(full_doc)
        .build();
    let mut update_change_stream = collection.watch(pipeline, opts).await.unwrap();
    log::info!("Watching for updates");
    let mut i = 0;
    while let Some(event) = update_change_stream.next().await.transpose().unwrap() {
        log::debug!(
            "Update performed: {:?}, full document: {:?}",
            event.update_description,
            event.full_document
        );
        let mut updated = event.full_document.unwrap();
        updated.status = update.clone();
        let updated_document = bson::to_document(&updated).unwrap();
        let query = doc! { "_id" : updated.id.clone() };
        let updated = doc! {
            "$set": updated_document
        };
        log::debug!("Document updated: {:?}", updated);
        collection.update_one(query, updated, None).await.unwrap();
        i += 1;
        if i >= UPDATES_NUM {
            log::debug!("Processed all updates");
            break;
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let client = mongodb::Client::with_uri_str("mongodb://mongodb:27017")
        .await
        .unwrap();

    let db = client.database("admin");
    while let Err(error) = db.run_command(doc! {"replSetGetStatus": 1}, None).await {
        log::info!("Waiting for MongoDB to be ready, error: {error:?}");
        time::sleep(time::Duration::from_secs(1)).await;
    }

    let db = client.database("db_data");
    let collection = db.collection::<Data>("data");

    log::info!("Connected to MongoDB");

    let opt = Opt::from_args();
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
