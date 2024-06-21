use std::fmt::Debug;

use chrono::{serde::ts_milliseconds, DateTime, Utc};
use derive_getters::Getters;
use mongodb::bson::oid::ObjectId;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::api::cid::Cid;

pub(crate) trait RequestT:
    DeserializeOwned + Serialize + Unpin + Send + Sync + Debug
{
    type Status: StatusT;
    type Payload;

    fn oid(&self) -> &ObjectId;
    fn cid(&self) -> &Cid;
    fn accepted_at(&self) -> &DateTime<Utc>;
    fn status(&self) -> &Self::Status;
    fn payload(&self) -> &Self::Payload;
}

pub(crate) trait StatusQueryT: Serialize + DeserializeOwned + Debug {}

pub(crate) trait StatusT: Serialize + DeserializeOwned + Debug {
    type Query: StatusQueryT;

    fn to_query(&self) -> Self::Query;
}

// Implementations

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Approver(String);

impl From<String> for Approver {
    fn from(s: String) -> Self {
        Self(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Desitnation(String);

impl From<String> for Desitnation {
    fn from(s: String) -> Self {
        Self(s)
    }
}
