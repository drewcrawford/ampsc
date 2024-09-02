/*!
A multi-producer, single-consumer async stream channel.

This channel guarantees reliable delivery.  If you can live with alternate guarantees, you probably
mean to target a different API.
*/

use std::future::Future;
use std::pin::Pin;
use dlog::perfwarn;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll, Waker};
use async_drop::{AsyncDrop, PhantomDontSyncDrop};

#[derive(thiserror::Error, Debug)]
pub enum SendError {
    #[error("The consumer has hung up")]
    ConsumerHangup,
}

#[derive(Debug)]
struct Locked<T> {
    data: Option<T>,
    pending_producers: Vec<Waker>,
    pending_consumer: Option<Waker>,
    consumer_hangup: bool,

}
impl<T> Locked<T> {
    fn new() -> Locked<T> {
        Locked {
            data: None,
            pending_producers: Vec::new(),
            pending_consumer: None,
            consumer_hangup: false,
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
    (ChannelProducer { shared: shared.clone() }, ChannelConsumer { shared, _phantom_dont_sync_drop: PhantomDontSyncDrop::new() })
}

#[derive(Debug)]
pub struct ChannelConsumer<T> {
    shared: Arc<Shared<T>>,
    _phantom_dont_sync_drop: PhantomDontSyncDrop,
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

pub struct ConsumerDropFuture<T> {
    inner: ChannelConsumer<T>,
}

impl<T> Future for ConsumerDropFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        let pinned = std::pin::pin!(async {
            perfwarn!("data field may need optimization", {
            let mut lock = mut_self.inner.shared.optimize_data.lock().await;
            lock.consumer_hangup = true;
            lock.pending_producers.pop().map(|e| e.wake());
                mut_self.inner._phantom_dont_sync_drop.mark_async_dropped();
        });
        });
        pinned.poll(cx)
    }
}

impl<T> async_drop::AsyncDrop for ChannelConsumer<T> {
    type Future = ConsumerDropFuture<T>;

    fn async_drop(self) -> ConsumerDropFuture<T> {
        ConsumerDropFuture { inner: self }
    }
}

#[derive(Debug)]
pub struct ChannelProducer<T> {
    shared: Arc<Shared<T>>,
}

#[derive(Debug)]
pub struct ChannelProducerSendFuture<'a, T> {
    inner: &'a mut ChannelProducer<T>,
    data: Option<T>,
}



impl<'a, T> Future for ChannelProducerSendFuture<'a, T>
where
    T: Unpin,
{
    type Output = Result<(), SendError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let take_data = self.data.take().expect("Data to send");
        let move_waker = cx.waker();
        let pinned = std::pin::pin!(poll_escape::PollEscape::escape( async {
            let mut lock = self.inner.shared.optimize_data.lock().await;
            if lock.consumer_hangup {
                return poll_escape::Poll::Ready(Err(SendError::ConsumerHangup));
            }
            if let Some(data) = lock.data.as_ref() {
                lock.pending_producers.push(move_waker.clone());
                return poll_escape::Poll::Escape;
            }
            else {
                lock.data = Some(take_data);
                lock.pending_consumer.take().map(|e| e.wake());
                poll_escape::Poll::Ready(Ok(()))
            }
        }));
        pinned.poll(cx)

    }
}



impl<T> ChannelProducer<T> {
    pub fn send(&mut self, data: T) -> ChannelProducerSendFuture<T> {
        ChannelProducerSendFuture {
            inner: self,
            data: Some(data),
        }
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

#[test]
fn test_push() {
    let (mut producer, mut consumer) = channel();
    afut::block_on::spin_on(producer.send(1));
    let r = afut::block_on::spin_on(consumer.receive());
    afut::block_on::spin_on(consumer.async_drop());
    assert_eq!(r, 1);
}
