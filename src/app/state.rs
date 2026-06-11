use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

/// Type-keyed, cheaply-cloneable shared application state. One value per type.
#[derive(Clone, Default)]
pub struct StateStore {
    values: Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl StateStore {
    pub fn insert<T>(&mut self, value: T)
    where
        T: Send + Sync + 'static,
    {
        Arc::make_mut(&mut self.values).insert(TypeId::of::<T>(), Arc::new(value));
    }

    pub fn get<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|value| Arc::clone(value).downcast::<T>().ok())
    }
}
