#![cfg_attr(not(test), no_std)]

extern crate alloc;

pub mod fanout;

#[allow(async_fn_in_trait)]
pub trait Task: Send {
    type Error: Send;
    async fn run(self) -> Result<(), Self::Error>;
}
