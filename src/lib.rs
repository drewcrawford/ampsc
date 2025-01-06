//SPDX-License-Identifier: MIT OR Apache-2.0
/*!
![logo](art/logo.png)
It's an **a**sync **m**ulti-**p**roducer **s**ingle-**c**onsumer channel (ampsc).

This is the opposite of an [ampsc](https://crates.io/crates/ampsc) channel.
It's also the opposite of an [ampmc](https://crates.io/crates/ampmc) channel.

This crate is completely executor-agnostic.
*/



use std::fmt::Debug;
use std::future::Future;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::task::{Context, Poll};
use atomiclock_async::AtomicLockAsync;
/**
Errors during sending */
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum SendError {
    #[error("The consumer has hung up")]
    ConsumerHangup,
}

/**
Errors during receiving */
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum RecvError {
    #[error("The producer has hung up")]
    ProducerHangup,
}

/**
A structure that can be 'hungup' indicating the data is dropped permanently.
*/
#[derive(Debug)]
struct Hungup<T> {
    data: Option<T>
}
impl<T> Hungup<T> {
    fn expect_mut(&mut self, reason: &str) -> &mut T {
        self.data.as_mut().expect(reason)
    }

    fn new(data: T) -> Self {
        Hungup {
            data: Some(data),
        }
    }

    fn as_mut(&mut self) -> &mut Option<T> {
        &mut self.data
    }
    /**
    If the value is not hungup, replaces the value with the replacement and returns the old value.

    If the value is hungup, returns None, dropping the replacement value.
    */
    fn replace_if_needed(&mut self, replacement: T) -> Option<T> {
        if let Some(data) = &mut self.data {
            let mut replacement = replacement;
            std::mem::swap(data, &mut replacement);
            Some(replacement)
        }
        else {
            None
        }
    }
    fn hangup(&mut self) -> T {
        self.data.take().expect("Already hungup")
    }


}

/*
You might imagine some better datastructure here, like what if we had atomics for insert and remove.

Problems are.

1.  We need to synchronize on both of these, not one or the other.
2.  Reallocation.  Like on the one hand, one could imagine a fixed-sized buffer.  On some level, your whole computer is a fixed-size buffer, so there's always some upper limit.  On the other hand, maybe you wanted to use your memory for something else.
3.  So maybe it occurs to you to use backpressure to store the spill.  But then you're trying to implement a channel in terms of a channel, which is a bit of a problem.
 */
#[derive(Debug)]
struct Locked<T> {
    pending_consumer: Hungup<Option<r#continue::Sender<Result<T,RecvError>>>>,
    pending_producers: Hungup<Vec<PendingProducer<T>>>,
}

#[derive(Debug)]
struct Shared<T> {
    //perfwarn - we want a nonblocking lock I think
    lock: atomiclock_async::AtomicLockAsync<Locked<T>>,
}


#[derive(Debug)]
struct PendingProducer<T> {
    data: T,
    continuation: r#continue::Sender<Result<(),SendError>>,
}

impl<T> PendingProducer<T> {
    fn into_inner(self) -> (T, r#continue::Sender<Result<(),SendError>>) {
        (self.data, self.continuation)
    }
}

/**
Creates a new channel.
*/
pub fn channel<T>() -> (ChannelProducer<T>, ChannelConsumer<T>) {
    let shared = Arc::new(Shared {
        lock: AtomicLockAsync::new(Locked {
            pending_consumer: Hungup::new(None),
            pending_producers: Hungup::new(Vec::new()),
        }),
    });
    (ChannelProducer { shared: shared.clone(), active_producers: Arc::new(AtomicU8::new(1)), async_dropped: false}, ChannelConsumer { shared, async_dropped: false })
}

/**
A consumer (recv-side) for a channel.
*/
#[derive(Debug)]
pub struct ChannelConsumer<T> {
    shared: Arc<Shared<T>>,
    async_dropped: bool,
}

/**
A future that represents an in-flight recieve operation.
*/
pub struct ChannelConsumerRecvFuture<'a, T> {
    future: Box<dyn Future<Output=Result<T,RecvError>> + Send>,
    _inner: &'a mut ChannelConsumer<T>,
}

impl <'a, T> Future for ChannelConsumerRecvFuture<'a, T> {
    type Output = Result<T,RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe{self.map_unchecked_mut(|s| Box::as_mut(&mut s.future))}.poll(cx)

    }
}


impl<T: 'static + Send> ChannelConsumer<T> {
    /**
    Receives a message from the channel.
    */
    pub fn receive(&mut self) -> ChannelConsumerRecvFuture<'_, T> {
        let shared = self.shared.clone();
        ChannelConsumerRecvFuture {
            _inner: self,
            future:Box::new(async move {
                //need a dedicated scope to drop lock before we await
                let future = {
                    let mut lock = shared.lock.lock().await;

                    if let Some(p) = lock.pending_producers.expect_mut("Pending producers hungup").pop() {
                        let (data, continuation) = p.into_inner();
                        continuation.send(Ok(()));
                        return Ok(data);
                    }
                    else {
                        match lock.pending_consumer.as_mut() {
                            Some(p) => {
                                let (sender, future) = r#continue::continuation();
                                *p = Some(sender);
                                future
                            }
                            None => {
                                return Err(RecvError::ProducerHangup);
                            }
                        }
                    }
                };
                future.await

            }),
        }
    }

    pub async fn async_drop(&mut self) {
        assert!(!self.async_dropped);
        let mut lock = self.shared.lock.lock().await;
        lock.pending_consumer.hangup();
        if let Some(producers) = lock.pending_producers.replace_if_needed(Vec::new()) {
            drop(lock);
            for producer in producers {
                producer.continuation.send(Err(SendError::ConsumerHangup));
            }
        }
        self.async_dropped = true;
    }
}

impl<T> Drop for ChannelConsumer<T> {
    fn drop(&mut self) {
        assert!(self.async_dropped, "You must call async_drop on the consumer before dropping it");
    }
}


/**
The producer (send-side) for a [channel].
*/
#[derive(Debug)]
pub struct ChannelProducer<T> {
    shared: Arc<Shared<T>>,
    active_producers: Arc<AtomicU8>,
    async_dropped: bool,
}

/**
A future that represents an in-flight send operation.
*/
pub struct ChannelProducerSendFuture<'a, T> {
    inner: &'a mut ChannelProducer<T>,
    future: Box<dyn Future<Output=Result<(),SendError>> + Send>,
}


impl Debug for ChannelProducerSendFuture<'_, ()> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelProducerSendFuture")
            .field("_inner", self.inner)
            .finish()
    }
}



impl<'a, T> Future for ChannelProducerSendFuture<'a, T>
{
    type Output = Result<(), SendError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe{self.map_unchecked_mut(|s| Box::as_mut(&mut s.future))}.poll(cx)
    }
}



impl<T: Send + 'static> ChannelProducer<T> {
    /**
    Sends a message into the channel.
    */
    pub fn send(&mut self, data: T) -> ChannelProducerSendFuture<T> {
        let move_shared = self.shared.clone();
        ChannelProducerSendFuture {
            inner: self,
            future: Box::new(async move {
                //need to declare a special scope so we can drop our lock before we await
                let future = {
                    let mut lock = move_shared.lock.lock().await;
                    match lock.pending_consumer.as_mut() {
                        Some(maybe_consumer) => {
                            match maybe_consumer.take() {
                                Some(consumer) => {
                                    consumer.send(Ok(data));
                                    return Ok(());
                                }
                                None => {
                                    let (sender, future) = r#continue::continuation();
                                    lock.pending_producers.expect_mut("Pending producers hungup").push(PendingProducer {
                                        data,
                                        continuation: sender,
                                    });
                                    future
                                }
                            }
                        }
                        None => {
                            return Err(SendError::ConsumerHangup);
                        }
                    }

                };
                future.await
            }),
        }
    }

    pub async fn async_drop(&mut self) {
        assert!(!self.async_dropped);
        let old = self.active_producers.fetch_sub(1, Ordering::SeqCst);
        logwise::warn_sync!("old {old}",old=old);
        if old == 1 {
            let mut lock = self.shared.lock.lock().await;
            let old = lock.pending_producers.hangup();
            assert!(old.is_empty());
            if let Some(consumer) = lock.pending_consumer.replace_if_needed(None) {
                drop(lock);
                if let Some(consumer) = consumer {
                    consumer.send(Err(RecvError::ProducerHangup));
                }
            }
        }
        self.async_dropped = true;

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
        ChannelProducer {
            shared: self.shared.clone(),
            active_producers: self.active_producers.clone(),
            async_dropped: false,
        }
    }
}

impl <T> Drop for ChannelProducer<T> {
    fn drop(&mut self) {
        assert!(self.async_dropped, "You must call async_drop on the producer before dropping it");
    }
}

#[test]
fn test_push() {
    logwise::context::Context::reset("test_push");
    let (producer, mut consumer) = channel();

    test_executors::spawn_on("test_push_thread",async move  {
        let mut producer = producer;
        producer.send(1).await.unwrap();
        producer.async_drop().await;
    });

    let r = test_executors::spin_on(consumer.receive()).unwrap();
    assert_eq!(r, 1);

    test_executors::spin_on(consumer.async_drop());
    logwise::context::Context::reset("finished");
}

