use mongodb::bson::{self, doc, Document};

use crate::request::StatusQueryT;

pub struct Filter {
    pub watch_pipeline: Vec<Document>,
    pub pre_watch_filter: Option<Document>,
}

impl Filter {
    pub fn builder() -> FilterBuilder {
        FilterBuilder::new()
    }
}

pub struct FilterBuilder {
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

    pub fn with_raw_watcher_pipeline(self, pipeline: Vec<Document>) -> Self {
        Self {
            watch_pipeline: pipeline,
            ..self
        }
    }

    pub fn with_raw_pre_watcher_filter(self, filter: Document) -> Self {
        Self {
            pre_watch_filter: Some(filter),
            ..self
        }
    }

    pub fn with_insert(self, expected_status: impl StatusQueryT) -> Self {
        let pipeline = vec![doc! {
            "$match" : doc! { "operationType" : "insert" },
        }];

        let watch_id = expected_status.query_id();
        let from_status = bson::ser::to_bson(&expected_status).unwrap();

        if self.pre_watch_filter.is_none() {
            Self {
                watch_pipeline: pipeline,
                pre_watch_filter: Some(doc! {watch_id: from_status}),
            }
        } else {
            Self {
                watch_pipeline: pipeline,
                ..self
            }
        }
    }

    pub fn with_update(self, expected_status: impl StatusQueryT) -> Self {
        let watch_id = expected_status.query_id();
        let from_status = bson::ser::to_bson(&expected_status).unwrap();
        let pipeline = vec![doc! {
            "$match": {
                "$and": [
                { format!("updateDescription.updatedFields.{watch_id}"): { "$eq": &from_status} },
                { "operationType": "update" },
                ]
        },
        }];

        if self.pre_watch_filter.is_none() {
            Self {
                watch_pipeline: pipeline,
                pre_watch_filter: Some(doc! {watch_id: &from_status}),
            }
        } else {
            Self {
                watch_pipeline: pipeline,
                ..self
            }
        }
    }

    pub fn build(self) -> Filter {
        Filter {
            watch_pipeline: self.watch_pipeline,
            pre_watch_filter: self.pre_watch_filter,
        }
    }
}
