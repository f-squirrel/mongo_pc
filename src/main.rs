use futures_util::stream::StreamExt;

use mongodb::{
    bson::{self, doc, oid::ObjectId, Document},
    options::{ChangeStreamOptions, FullDocumentType},
    Collection,
};
use structopt::StructOpt;
use tokio::time;

const UPDATES_NUM: usize = 100000;

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
enum Status {
    Pending,
    Approved,
    Sent,
    Finalized,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Data {
    // DD: Do not change it anywhere after creation
    #[serde(rename = "_id")]
    id: ObjectId,

    cid: Cid,
    payload: String,
    status: Status,
}

async fn produce(collection: Collection<Data>) {
    time::sleep(time::Duration::from_secs(10)).await;
    log::info!("Producing data");
    // produce
    for _i in 0..UPDATES_NUM {
        let data = Data {
            id: ObjectId::new(),
            payload: "data".to_string(),
            cid: uuid::Uuid::new_v4().into(),
            status: Status::Pending,
        };
        collection.insert_one(data, None).await.unwrap();
        log::info!("Produced {_i}");
    }
    log::info!("Data produced");
}

async fn consume_created(collection: Collection<Data>, from: Status, to: Status) {
    let pipeline = vec![doc! { "$match" : doc! { "operationType" : "insert" } }];
    watch_and_update(collection, pipeline, from, to).await;
}

async fn consume_updated(collection: Collection<Data>, from: Status, to: Status) {
    let from_status = bson::ser::to_bson(&from).unwrap();
    let pipeline = vec![doc! {
    "$match": {
                 "$and": [
                 { "updateDescription.updatedFields.status": { "$eq":  from_status } },
                 { "operationType": "update" },
                 ]
         }
      }];
    watch_and_update(collection, pipeline, from, to).await;
}

async fn watch_and_update(
    collection: Collection<Data>,
    pipeline: Vec<Document>,
    _from: Status,
    to: Status,
) {
    let full_doc = Some(FullDocumentType::UpdateLookup);
    let opts = ChangeStreamOptions::builder()
        .full_document(full_doc)
        .build();
    let mut update_change_stream = collection.watch(pipeline, opts).await.unwrap();
    log::info!("Watching for updates");
    let mut i = 0;
    while let Some(event) = update_change_stream.next().await.transpose().unwrap() {
        log::info!(
            "Update performed: {:?}, full document: {:?}",
            event.update_description,
            event.full_document
        );
        let mut updated = event.full_document.unwrap();
        updated.status = to.clone();
        let updated_document = bson::to_document(&updated).unwrap();
        let query = doc! { "_id" : updated.id.clone() };
        let updated = doc! {
            "$set": updated_document
        };
        log::info!("Document updated: {:?}", updated);
        collection.update_one(query, updated, None).await.unwrap();
        i += 1;
        if i >= UPDATES_NUM {
            log::info!("Processed all updates");
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
        "consumer1" => consume_created(collection, Status::Pending, Status::Approved).await,
        "consumer2" => consume_updated(collection, Status::Approved, Status::Sent).await,
        "consumer3" => consume_updated(collection, Status::Sent, Status::Finalized).await,
        _ => panic!("Invalid type provided."),
    }
}