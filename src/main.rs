use std::os::unix::raw::mode_t;

use futures_util::stream::StreamExt;

use mongodb::{
    bson::{self, bson, doc, oid::ObjectId, Document},
    Collection,
};
// use mongodb::change_stream::

use structopt::StructOpt;
use tokio::time;

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Opt {
    /// Set type
    #[structopt(short = "t", long = "type", possible_values = &["producer", "consumer1", "consumer2"])]
    type_: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum Status {
    Pending,
    Approved,
    Finalized,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Data {
    _id: ObjectId,
    status: Status,
}

async fn produce(collection: Collection<Data>) {
    log::info!("Producing data");
    // produce
    for _i in 0..100000 {
        let data = Data {
            _id: ObjectId::new(),
            status: Status::Pending,
        };
        collection.insert_one(data, None).await.unwrap();
    }
    log::info!("Data produced");
}

async fn consume_created(collection: Collection<Data>, from: Status, to: Status) {
    let pipeline = vec![doc! { "$match" : doc! { "operationType" : "update" } }];
    watch_and_update(collection, pipeline, from, to).await;
}

async fn consume_updated(collection: Collection<Data>, from: Status, to: Status) {
    let pipeline = vec![doc! {
    "$match": {
                // doc!{ "operationType": "update" }
                 "$and": [
                //  { "updateDescription.updatedFields.status": { "$eq": bson::to_document(&from).unwrap() } },
                 { "updateDescription.updatedFields.status": { "$exists": true } },
                 { "operationType": "update" }]
         }
      }];
    watch_and_update(collection, pipeline, from, to).await;
}

async fn watch_and_update(
    collection: Collection<Data>,
    pipeline: Vec<Document>,
    from: Status,
    to: Status,
) {
    let mut update_change_stream = collection.watch(pipeline, None).await.unwrap();
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
        let query = doc! { "_id" : updated_document.get("_id").unwrap() };
        let updated = doc! {
            "$set": updated_document
        };
        log::info!("Document updated: {:?}", updated);
        collection.update_one(query, updated, None).await.unwrap();
        i += 1;
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    time::sleep(time::Duration::from_secs(10)).await;

    let client = mongodb::Client::with_uri_str("mongodb://mongodb:27017")
        .await
        .unwrap();
    let db = client.database("db_data");
    let collection = db.collection::<Data>("data");

    log::info!("Connected to MongoDB");

    let opt = Opt::from_args();
    match opt.type_.as_str() {
        "producer" => produce(collection).await,
        "consumer1" => consume_created(collection, Status::Pending, Status::Approved).await,
        "consumer2" => consume_updated(collection, Status::Approved, Status::Finalized).await,
        _ => panic!("Invalid type provided."),
    }
}
