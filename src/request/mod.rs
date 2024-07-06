use std::fmt::Debug;

use chrono::{DateTime, Utc};
use mongodb::bson::oid::ObjectId;
use serde::{de::DeserializeOwned, Serialize};

use crate::api::cid::Cid;

pub trait RequestT: DeserializeOwned + Serialize + Unpin + Send + Sync + Debug {
    type Status: StatusT;
    type Payload;

    fn oid(&self) -> &ObjectId;
    fn cid(&self) -> &Cid;
    fn accepted_at(&self) -> &DateTime<Utc>;
    fn status(&self) -> &Self::Status;
    fn payload(&self) -> &Self::Payload;
}

pub trait StatusQueryT: Serialize + DeserializeOwned + Debug {
    fn query_id(&self) -> &str;
}

pub trait StatusT: Serialize + DeserializeOwned + Debug {
    type Query: StatusQueryT;

    fn to_query(&self) -> Self::Query;
}
