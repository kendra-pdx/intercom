use core::array;

use async_channel::{Receiver, Sender, bounded};
use either::Either;
use itertools::Itertools;

use crate::Task;

pub struct FanOut<T, const N: usize> {
    fan_tx: [Sender<T>; N],
    rx: Receiver<T>,
}

impl<T, const N: usize> FanOut<T, N>
where
    T: Clone + Send,
{
    pub fn new(
        rx: Receiver<T>,
    ) -> (
        [Receiver<T>; N],
        impl Task<Error = Either<async_channel::SendError<T>, async_channel::RecvError>>,
    ) {
        // the main channel
        let fan_out: [(Sender<T>, Receiver<T>); N] = array::from_fn(|_| bounded(1));
        let fan_tx = fan_out
            .iter()
            .map(|ch| ch.0.clone())
            .collect_array()
            .unwrap();

        let fan_rx = fan_out
            .iter()
            .map(|ch| ch.1.clone())
            .collect_array()
            .unwrap();

        let fan_out = Self { fan_tx, rx };
        (fan_rx, fan_out)
    }
}

impl<T, const N: usize> Task for FanOut<T, N>
where
    T: Clone + Send,
{
    type Error = Either<async_channel::SendError<T>, async_channel::RecvError>;

    async fn run(self) -> Result<(), Self::Error> {
        loop {
            let m = self.rx.recv().await.map_err(Either::Right)?;
            for tx in &self.fan_tx {
                tx.send(m.clone()).await.map_err(Either::Left)?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{join, time::timeout};

    use crate::{Task, fanout::FanOut};

    #[tokio::test]
    async fn fanout() {
        let (tx, rx) = async_channel::bounded(1);
        let ([rx_1, rx_2], fanout) = FanOut::new(rx);

        let max = Duration::from_millis(100);
        let mut completed: bool = false;
        let txrx = timeout(max, async {
            let send = tx.send(1).await;
            let recv_1 = rx_1.recv().await;
            let recv_2 = rx_2.recv().await;

            assert_eq!(send, Ok(()));
            assert_eq!(recv_1, Ok(1));
            assert_eq!(recv_2, Ok(1));

            completed = true;
            tx.close(); // drop the sender to complete the driver
        });

        let (txrx, _) = join!(txrx, fanout.run());
        
        assert!(
            txrx.is_ok(),
            "txrx should not timeout; if so the fanout driver did not run."
        );
        assert!(completed, "all messages must be received");
    }
}
