use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Cid(
    // simple: 284c153cc5a642c19555ad27d1792428
    // braced: {27288856-974f-445e-9d88-ca65472a675b}
    // urn: urn:uuid:582205a5-445b-4a48-af37-0cd266a2ee3a
    // compact: bytes
    uuid::Uuid,
);

impl std::fmt::Display for Cid {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Serialize for Cid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.0.to_string().as_str())
    }
}

impl<'de> serde::Deserialize<'de> for Cid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(CidVisitor)
    }
}
struct CidVisitor;

impl<'de> serde::de::Visitor<'de> for CidVisitor {
    type Value = Cid;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a UUID the following format: 3fc178b9-be92-451c-954f-61ef0c6e9bbf")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let uuid = uuid::Uuid::parse_str(value).map_err(serde::de::Error::custom)?;
        Ok(Cid(uuid))
    }
}

impl From<uuid::Uuid> for Cid {
    fn from(uuid: uuid::Uuid) -> Self {
        Cid(uuid)
    }
}
