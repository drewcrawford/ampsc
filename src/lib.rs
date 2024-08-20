/*!
A multi-producer, single-consumer async stream channel.

This channel guarantees reliable delivery.  If you can live with alternate guarantees, you probably
mean to target a different API.
*/

use std::sync::Arc;
use std::task::{Waker};

struct Locked<T> {
    data: Option<T>,
    pending_producers: Vec<Waker>,
    pending_consumer: Option<Waker>,
}
impl<T> Locked<T> {
    fn new() -> Locked<T> {
        Locked {
            data: None,
            pending_producers: Vec::new(),
            pending_consumer: None,
        }
    }
}

struct Shared<T> {
    //todo: optimize
    data: atomiclock_async::AtomicLockAsync<Locked<T>>,
}


struct Channel<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Channel<T> {
    pub fn new() -> Self {
        Channel {
            shared: Arc::new(Shared {
                data: atomiclock_async::AtomicLockAsync::new(Locked::new()),
            }),
        }
    }
    pub fn producer(&self) -> ChannelProducer<T> {
        ChannelProducer {
            shared: self.shared.clone(),
        }
    }

    pub fn consumer(self) -> ChannelConsumer<T> {
        ChannelConsumer {
            shared: self.shared,
        }
    }
}

pub struct ChannelConsumer<T> {
    shared: Arc<Shared<T>>,
}



impl<T> ChannelConsumer<T> {
    pub async fn receive(&self) -> T {
        loop {
            let mut lock = self.shared.data.lock().await;
            match lock.data.take() {
                Some(data) => {
                    if let Some(producer) = lock.pending_producers.pop() {
                        drop(lock);
                        producer.wake();
                    }
                    return data
                }
                None => {
                    let current_waker = with_waker::WithWaker::new().await;
                    lock.pending_consumer = Some(current_waker);
                    //try again
                }
            }
        }



    }
}

pub struct ChannelProducer<T> {
    shared: Arc<Shared<T>>,
}

impl<T> ChannelProducer<T> {
    pub async fn send(&self, data: T) {
        let mut lock = self.shared.data.lock().await;
        match lock.data.as_ref() {
            Some(_) => {
                let current_waker = with_waker::WithWaker::new().await;
                lock.pending_producers.push(current_waker);
            }
            None => {
                lock.data = Some(data);
                if let Some(consumer) = lock.pending_consumer.take() {
                    drop(lock);
                    consumer.wake();
                }
            }
        }
    }
}



#[test] fn test_push() {
    let channel = Channel::new();
    let producer = channel.producer();
    let consumer = channel.consumer();
    afut::block_on::spin_on(producer.send(1));
    let r = afut::block_on::spin_on(consumer.receive());
    assert_eq!(r,1);

}
