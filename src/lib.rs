/*!
A multi-producer, single-consumer async stream channel.

This channel guarantees reliable delivery.  If you can live with alternate guarantees, you probably
mean to target a different API.
*/

use std::future::Future;
use std::pin::Pin;
use dlog::perfwarn;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

#[derive(thiserror::Error, Debug)]
pub enum SendError {
    #[error("The consumer has hung up")]
    ConsumerHangup,
}

#[derive(Debug)]
struct Locked<T> {
    data: Option<T>,
    pending_consumer: Option<Waker>,

}
impl<T> Locked<T> {
    fn new() -> Locked<T> {
        Locked {
            data: None,
            pending_consumer: None,
        }
    }
}

#[derive(Debug)]
struct Shared<T> {
    //todo: optimize
    optimize_data: atomiclock_async::AtomicLockAsync<Locked<T>>,
    consumer_hangup: AtomicBool,
    pending_producers: wakelist::WakeList,
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
        consumer_hangup: AtomicBool::new(false),
        pending_producers: wakelist::WakeList::new(),
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
                        self.shared.pending_producers.wake_one();
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

impl<T> Drop for ChannelConsumer<T> {
    fn drop(&mut self) {
        self.shared.consumer_hangup.store(true, std::sync::atomic::Ordering::Release);
        self.shared.pending_producers.wake_all();
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
            if self.inner.shared.consumer_hangup.load(Ordering::Relaxed) {
                return poll_escape::Poll::Ready(Err(SendError::ConsumerHangup));
            }
            if let Some(data) = lock.data.as_ref() {
                self.inner.shared.pending_producers.push(move_waker.clone());
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
    assert_eq!(r, 1);
}
