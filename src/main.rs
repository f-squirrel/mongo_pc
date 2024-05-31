use std::os::unix::raw::mode_t;

use futures_util::stream::StreamExt;

use mongodb::{
    bson,
    bson::{bson, doc, Document},
    Collection,
};
// use mongodb::change_stream::

use structopt::StructOpt;

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
    _id: String,
    status: Status,
}

async fn produce(collection: Collection<Data>) {
    // produce
    for i in 0..10000 {
        let data = Data {
            _id: i.to_string(),
            status: Status::Pending,
        };
        collection.insert_one(data, None).await.unwrap();
    }
}

async fn watch_and_update(collection: Collection<Data>, from: Status, to: Status) {
    // consume
    let pipeline = vec![doc! { "$match" : doc! { "operationType" : "update" } }];
    // let pipeline = vec![doc! {
    // "$match": {
    //             doc!{ "operationType": "update" }
    //             //  "$and": [
    //             //  { "updateDescription.updatedFields.status": { "$eq": bson::to_document(&from).unwrap() } },
    //             //  { "updateDescription.updatedFields.status": { "$eq": bson::to_document(&from).unwrap() } },
    //             //  { "operationType": "update" }]
    //      }
    //   }];

    let mut update_change_stream = collection.watch(pipeline, None).await.unwrap();
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
        collection
            .update_one(query, updated_document, None)
            .await
            .unwrap();

        log::info!("Document updated: {:?}", updated);
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    // create a connection to mongodb
    //mongodb://mongodb:27017
    let client = mongodb::Client::with_uri_str("mongodb://mongodb:27017?replicaSet=rs0")
        .await
        .unwrap();
    let db = client.database("db_data");
    let collection = db.collection::<Data>("data");

    let opt = Opt::from_args();
    match opt.type_.as_str() {
        "producer" => produce(collection).await,
        "consumer1" => watch_and_update(collection, Status::Pending, Status::Approved).await,
        "consumer2" => watch_and_update(collection, Status::Approved, Status::Finalized).await,
        _ => panic!("Invalid type provided."),
    }
}
