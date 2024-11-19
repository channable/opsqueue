use std::fmt::Debug;

pub mod chunk;
pub mod errors;
pub mod submission;

pub trait MayBeZero {
    fn is_zero(&self) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub struct NonZero<T>(T);

impl<'de, T: MayBeZero + Debug + serde::Deserialize<'de>> serde::Deserialize<'de> for NonZero<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let inner = T::deserialize(deserializer)?;
        NonZero::try_from(inner).map_err(|e| {
            serde::de::Error::invalid_value(
                serde::de::Unexpected::Other(&format!("zero (0)-value {e:?}")),
                &&*format!("a non-zero value of type{}", std::any::type_name::<T>()),
            )
        })
    }
}

impl<T> NonZero<T> {
    pub fn inner(&self) -> &T {
        &self.0
    }
    pub fn into_inner(self) -> T {
        self.0
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Given a zero-value where a non-zero value is expected: {0:?}")]
pub struct NonZeroIsZero<T>(T);

impl<T: MayBeZero> NonZero<T> {
    /// builds a new NonZero from a given value.
    ///
    /// Nicer would be to implement TryFrom but we cannot
    /// because of the blanket implementation `TryFrom<T, U: Into<T>> ...` :-(
    pub fn try_from(value: T) -> Result<Self, NonZeroIsZero<T>> {
        if value.is_zero() {
            Err(NonZeroIsZero(value))
        } else {
            Ok(Self(value))
        }
    }
}
