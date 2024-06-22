use mongodb::bson::{self, doc, Document};

use crate::request::StatusQueryT;

pub(crate) struct Filter {
    pub(crate) watch_pipeline: Vec<Document>,
    pub(crate) pre_watch_filter: Option<Document>,
}

impl Filter {
    pub(crate) fn builder() -> FilterBuilder {
        FilterBuilder::new()
    }
}

pub(crate) struct FilterBuilder {
    watch_pipeline: Vec<Document>,
    pre_watch_filter: Option<Document>,
}

impl FilterBuilder {
    fn new() -> Self {
        Self {
            watch_pipeline: Vec::new(),
            pre_watch_filter: None,
        }
    }

    pub(crate) fn with_raw_watcher_pipeline(self, pipeline: Vec<Document>) -> Self {
        Self {
            watch_pipeline: pipeline,
            ..self
        }
    }

    pub(crate) fn with_raw_pre_watcher_filter(self, filter: Document) -> Self {
        Self {
            pre_watch_filter: Some(filter),
            ..self
        }
    }

    pub(crate) fn with_insert(self) -> Self {
        let pipeline = vec![doc! {
            "$match" : doc! { "operationType" : "insert" },
        }];
        Self {
            watch_pipeline: pipeline,
            ..self
        }
    }

    pub(crate) fn with_update(self, expected_status: impl StatusQueryT) -> Self {
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
        Self {
            watch_pipeline: pipeline,
            ..self
        }
    }

    pub(crate) fn build(self) -> Filter {
        Filter {
            watch_pipeline: self.watch_pipeline,
            pre_watch_filter: self.pre_watch_filter,
        }
    }
}
