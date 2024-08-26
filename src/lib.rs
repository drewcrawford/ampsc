/*!
A multi-producer, single-consumer async stream channel.

This channel guarantees reliable delivery.  If you can live with alternate guarantees, you probably
mean to target a different API.
*/
use dlog::perfwarn;
use std::sync::Arc;
use std::task::{Waker};

#[derive(Debug)]
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

#[derive(Debug)]
struct Shared<T> {
    //todo: optimize
    optimize_data: atomiclock_async::AtomicLockAsync<Locked<T>>,
}

/**
Creates a new channel.
*/
pub fn channel<T>() -> (ChannelProducer<T>, ChannelConsumer<T>) {
    let lock = dlog::perfwarn!("data field may need optimization", {
        atomiclock_async::AtomicLockAsync::new(Locked::new())
    });
    let shared = Arc::new(Shared {
        optimize_data: lock,
    });
    (ChannelProducer { shared: shared.clone() }, ChannelConsumer { shared })
}

#[derive(Debug)]
pub struct ChannelConsumer<T> {
    shared: Arc<Shared<T>>,
}



impl<T> ChannelConsumer<T> {
    pub async fn receive(&mut self) -> T {
        loop {
            perfwarn!("data field may need optimization", {
                let mut lock = self.shared.optimize_data.lock().await;
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
            })

        }



    }
}

#[derive(Debug)]
pub struct ChannelProducer<T> {
    shared: Arc<Shared<T>>,
}

impl<T> ChannelProducer<T> {
    pub async fn send(&mut self, data: T) {
        perfwarn!("data field may need optimization", {
            let mut lock = self.shared.optimize_data.lock().await;
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
        });

    }
}

/*
boilerplate.

1.  Can't clone a consumer, only producer
2.  No copy
3.  We could in theory implement PartialEq/Eq based on same channel, but not sure how critical that is
4.  No ordering
5.  Don't think hash makes a ton of sense for this type, but we could do it based on eq I guess
6.  Default doesn't really make sense for either type, they're created as a pair
7.  Can't see a ton of value in display
8.  In theory could support From/Into for crossing the pipe but might constrain perf somewhat
9.  Not a pointer to anything
10.
 */

impl<T> Clone for ChannelProducer<T> {
    fn clone(&self) -> Self {
        ChannelProducer {
            shared: self.shared.clone(),
        }
    }
}

#[test] fn test_push() {
    let (mut producer,mut consumer) = channel();
    afut::block_on::spin_on(producer.send(1));
    let r = afut::block_on::spin_on(consumer.receive());
    assert_eq!(r,1);

}
