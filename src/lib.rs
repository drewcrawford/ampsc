/*!
A multi-producer, single-consumer async stream channel.

This channel guarantees reliable delivery.  If you can live with alternate guarantees, you probably
mean to target a different API.
*/

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::task::{Context, Poll};

#[derive(thiserror::Error, Debug)]
pub enum SendError {
    #[error("The consumer has hung up")]
    ConsumerHangup,
}

#[derive(thiserror::Error, Debug)]
pub enum RecvError {
    #[error("The producer has hung up")]
    ProducerHangup,
}

#[derive(Debug)]
struct Locked<T> {
    data: Option<T>,

}
impl<T> Locked<T> {
    fn new() -> Locked<T> {
        Locked {
            data: None,
        }
    }
}

#[derive(Debug)]
struct Shared<T> {
    //todo: optimize
    optimize_data: atomiclock_async::AtomicLockAsync<Locked<T>>,
    consumer_hangup: AtomicBool,
    producer_hangup: AtomicU8,
    pending_producers: wakelist::WakeList,
    pending_consumer: wakelist::WakeOne,
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
        producer_hangup: AtomicU8::new(1),
        pending_consumer: wakelist::WakeOne::new(),
    });
    (ChannelProducer { shared: shared.clone() }, ChannelConsumer { shared })
}

#[derive(Debug)]
pub struct ChannelConsumer<T> {
    shared: Arc<Shared<T>>,
}

pub struct ChannelConsumerRecvFuture<'a, T> {
    inner: &'a mut ChannelConsumer<T>,
}

impl <'a, T> Future for ChannelConsumerRecvFuture<'a, T> {
    type Output = Result<T,RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (sender,receiver) = poll_escape::waker_escape::waker_escape();
        let pinned = std::pin::pin!(poll_escape::PollEscape::escape(sender, async {
            if self.inner.shared.producer_hangup.load(Ordering::Relaxed) == 0 {
                return poll_escape::Poll::Ready(Err(RecvError::ProducerHangup));
            }
            let mut lock = self.inner.shared.optimize_data.lock().await;
            if let Some(data) = lock.data.take() {
                //it's ok to pop here because we always insert fresh wakers
                self.inner.shared.pending_producers.wake_one_pop();
                return poll_escape::Poll::Ready(Ok(data));
            }
            else {
                self.inner.shared.pending_consumer.push_if_empty(&receiver.into_waker());
                poll_escape::Poll::Escape
            }
        }));
        pinned.poll(cx)
    }
}


impl<T> ChannelConsumer<T> {
    pub fn receive(&mut self) -> ChannelConsumerRecvFuture<'_, T> {
        ChannelConsumerRecvFuture {
            inner: self,
        }
    }
}

impl<T> Drop for ChannelConsumer<T> {
    fn drop(&mut self) {
        self.shared.consumer_hangup.store(true, std::sync::atomic::Ordering::Release);
        self.shared.pending_producers.wake_all_drain();
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
        let (sender,receiver) = poll_escape::waker_escape::waker_escape();
        let pinned = std::pin::pin!(poll_escape::PollEscape::escape(sender, async {
            let mut lock = self.inner.shared.optimize_data.lock().await;
            if self.inner.shared.consumer_hangup.load(Ordering::Relaxed) {
                return poll_escape::Poll::Ready(Err(SendError::ConsumerHangup));
            }
            if let Some(_) = lock.data.as_ref() {
                self.inner.shared.pending_producers.push(receiver.into_waker());
                return poll_escape::Poll::Escape;
            }
            else {
                lock.data = Some(take_data);
                self.inner.shared.pending_consumer.wake_by_ref();
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
        //increment the producer hangup count
        self.shared.producer_hangup.fetch_add(1, Ordering::Relaxed);
        ChannelProducer {
            shared: self.shared.clone(),
        }
    }
}

impl <T> Drop for ChannelProducer<T> {
    fn drop(&mut self) {
        let old = self.shared.producer_hangup.fetch_sub(1, Ordering::Relaxed);
        if old == 1 {
            self.shared.pending_consumer.wake_by_ref();
        }
    }
}

#[test]
fn test_push() {
    let (mut producer, mut consumer) = channel();
    afut::block_on::spin_on(producer.send(1)).unwrap();
    let r = afut::block_on::spin_on(consumer.receive()).unwrap();
    assert_eq!(r, 1);
}
