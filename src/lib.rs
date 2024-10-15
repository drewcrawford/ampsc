/*!
A multi-producer, single-consumer async stream channel.

This channel guarantees reliable delivery.  If you can live with alternate guarantees, you probably
mean to target a different API.
*/

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::task::{Context, Poll};
use logwise::perfwarn;

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
pub struct Locked<T> {
    pending_consumer: Hungup<Option<continuation::Sender<Result<T,RecvError>>>>,
    pending_producers: Hungup<Vec<PendingProducer<T>>>,
}

#[derive(Debug)]
struct Shared<T> {
    //perfwarn - we want a nonblocking lock I think
    perfwarn_locked: sync::Mutex<Locked<T>>,
}


#[derive(Debug)]
struct PendingProducer<T> {
    data: T,
    continuation: continuation::Sender<Result<(),SendError>>,
}

impl<T> PendingProducer<T> {
    fn into_inner(self) -> (T, continuation::Sender<Result<(),SendError>>) {
        (self.data, self.continuation)
    }
}

/**
Creates a new channel.
*/
pub fn channel<T>() -> (ChannelProducer<T>, ChannelConsumer<T>) {
    let shared = Arc::new(Shared {
        perfwarn_locked: sync::Mutex::new(Locked {
            pending_consumer: Hungup::new(None),
            pending_producers: Hungup::new(Vec::new()),
        }),
    });
    (ChannelProducer { shared: shared.clone(), active_producers: Arc::new(AtomicU8::new(1)) }, ChannelConsumer { shared })
}

#[derive(Debug)]
pub struct ChannelConsumer<T> {
    shared: Arc<Shared<T>>,
}

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
    pub fn receive(&mut self) -> ChannelConsumerRecvFuture<'_, T> {
        let shared = self.shared.clone();
        ChannelConsumerRecvFuture {
            _inner: self,
            future:Box::new(async move {
                //need a dedicated scope to drop lock before we await
                let future = {
                    let _perf = logwise::perfwarn_begin!("ampsc::perfwarn_locked");
                    let mut lock = shared.perfwarn_locked.lock().unwrap();

                    if let Some(p) = lock.pending_producers.expect_mut("Pending producers hungup").pop() {
                        let (data, continuation) = p.into_inner();
                        continuation.send(Ok(()));
                        return Ok(data);
                    }
                    else {
                        match lock.pending_consumer.as_mut() {
                            Some(p) => {
                                let (sender, future) = continuation::continuation();
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
}

impl<T> Drop for ChannelConsumer<T> {
    fn drop(&mut self) {
        let data = perfwarn!("perfwarn_locked", {
            self.shared.perfwarn_locked.lock().unwrap().pending_producers.hangup()
        });
        for producer in data {
            producer.continuation.send(Err(SendError::ConsumerHangup));
        }
    }
}


#[derive(Debug)]
pub struct ChannelProducer<T> {
    shared: Arc<Shared<T>>,
    active_producers: Arc<AtomicU8>,
}

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
    pub fn send(&mut self, data: T) -> ChannelProducerSendFuture<T> {
        let move_shared = self.shared.clone();
        ChannelProducerSendFuture {
            inner: self,
            future: Box::new(async move {
                //need to declare a special scope so we can drop our lock before we await
                let future = {
                    let _perf = logwise::perfwarn_begin!("ampsc::perfwarn_locked");
                    let mut lock = move_shared.perfwarn_locked.lock().unwrap();
                    match lock.pending_consumer.as_mut() {
                        Some(maybe_consumer) => {
                            match maybe_consumer.take() {
                                Some(consumer) => {
                                    consumer.send(Ok(data));
                                    return Ok(());
                                }
                                None => {
                                    let (sender, future) = continuation::continuation();
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
        self.active_producers.fetch_add(1, Ordering::Relaxed);
        ChannelProducer {
            shared: self.shared.clone(),
            active_producers: self.active_producers.clone(),
        }
    }
}

impl <T> Drop for ChannelProducer<T> {
    fn drop(&mut self) {
        let old = self.active_producers.fetch_sub(1, Ordering::Relaxed);
        if old == 1 {
            let pending_consumer = perfwarn!("perfwarn_locked", {
                self.shared.perfwarn_locked.lock().unwrap().pending_consumer.hangup()

            });
            if let Some(consumer) = pending_consumer {
                consumer.send(Err(RecvError::ProducerHangup));
            }

        }
    }
}

#[test]
fn test_push() {
    let (producer, mut consumer) = channel();

    truntime::spawn_on(async move  {
        let mut producer = producer;
        producer.send(1).await.unwrap();
    });

    let r = truntime::spin_on(consumer.receive()).unwrap();
    assert_eq!(r, 1);
}
