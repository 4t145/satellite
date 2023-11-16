
pub trait EndPoint {

}

pub struct BoxedEndpoint(pub Box<dyn EndPoint>);

impl EndPoint for BoxedEndpoint {

}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct EndPointAddr {
    pub protocal: String,
    pub address: String,
}

