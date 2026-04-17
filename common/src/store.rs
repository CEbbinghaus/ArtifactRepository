use anyhow::Result;
use crate::{Hash, Object};


pub trait Store {
	fn get_object(&self, hash: &Hash) -> Option<Object>;

	fn put_object(&mut self, hash: &Hash, object: &Object) -> Result<()>;
}

pub struct InMemoryStore {

}

impl Store for InMemoryStore {
	fn get_object(&self, _hash: &Hash) -> Option<Object> {
		todo!()
	}

	fn put_object(&mut self, _hash: &Hash, _object: &Object) -> Result<()> {
		todo!()
	}
}
