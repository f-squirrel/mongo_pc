pub(crate) mod cid;

use serde::de::DeserializeOwned;
use std::fmt::Debug;

use crate::request::RequestT;

pub(crate) trait ApiRequestT: Into<Self::Request> + DeserializeOwned + Debug {
    type Request: RequestT;
}
