/*!
A multi-producer, single-consumer async stream channel.

This channel guarantees reliable delivery.  If you can live with alternate guarantees, you probably
mean to target a different API.
*/

use std::future::Future;
use std::pin::Pin;
use std::sync;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::task::{Context, Poll};
use dlog::perfwarn;
use poll_escape::PollEscapeSend;

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

type DataContinuation<T> = continuation::Sender<Result<T,RecvError>>;

/*
You might imagine some better datastructure here, like what if we had atomics for insert and remove.

Problems are.

1.  We need to synchronize on both of these, not one or the other.
2.  Reallocation.  Like on the one hand, one could imagine a fixed-sized buffer.  On some level, your whole computer is a fixed-size buffer, so there's always some upper limit.  On the other hand, maybe you wanted to use your memory for something else.
3.  So maybe it occurs to you to use backpressure to store the spill.  But then you're trying to implement a channel in terms of a channel, which is a bit of a problem.
 */
#[derive(Debug)]
pub struct Locked<T> {
    pending_consumer: Option<DataContinuation<T>>,
    pending_producers: Vec<continuation::Sender<DataContinuation<T>>>,
}

#[derive(Debug)]
struct Shared<T> {
    //perfwarn - we want a nonblocking lock I think
    perfwarn_locked: sync::Mutex<Locked<T>>,
    consumer_hangup: AtomicBool,
    producer_hangup: AtomicU8,
}

/**
Creates a new channel.
*/
pub fn channel<T>() -> (ChannelProducer<T>, ChannelConsumer<T>) {
    let shared = Arc::new(Shared {
        perfwarn_locked: sync::Mutex::new(Locked {
            pending_consumer: None,
            pending_producers: Vec::new(),
        }),
        consumer_hangup: AtomicBool::new(false),
        producer_hangup: AtomicU8::new(1),
    });
    (ChannelProducer { shared: shared.clone() }, ChannelConsumer { shared })
}

#[derive(Debug)]
pub struct ChannelConsumer<T> {
    shared: Arc<Shared<T>>,
}

pub struct ChannelConsumerRecvFuture<'a, T> {
    future: PollEscapeSend<Result<T,RecvError>>,
    inner: &'a mut ChannelConsumer<T>,
}

impl <'a, T> Future for ChannelConsumerRecvFuture<'a, T> {
    type Output = Result<T,RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe{self.map_unchecked_mut(|s| &mut s.future)}.poll(cx)

    }
}


impl<T: 'static + Send> ChannelConsumer<T> {
    pub fn receive(&mut self) -> ChannelConsumerRecvFuture<'_, T> {
        let shared = self.shared.clone();
        ChannelConsumerRecvFuture {
            inner: self,
            future: PollEscapeSend::escape(async move {
                //need a dedicated scope to drop lock before we await
                let future = {
                    let perf = dlog::perfwarn_begin!("ampsc::perfwarn_locked");
                    let mut lock = shared.perfwarn_locked.lock().unwrap();
                    if shared.producer_hangup.load(Ordering::Relaxed) == 0 {
                        return Err(RecvError::ProducerHangup);
                    }
                    //create a continuation
                    let (sender,future) = continuation::continuation();
                    if let Some(p) = lock.pending_producers.pop() {
                        //provide to producer
                        p.send(sender);
                    }
                    else {
                        lock.pending_consumer = Some(sender);
                    }
                    future
                };

                //fall out of scope and drop lock
                future.await
            }),
        }
    }
}

impl<T> Drop for ChannelConsumer<T> {
    fn drop(&mut self) {
        self.shared.consumer_hangup.store(true, std::sync::atomic::Ordering::Release);

        todo!()

    }
}


#[derive(Debug)]
pub struct ChannelProducer<T> {
    shared: Arc<Shared<T>>,
}

#[derive(Debug)]
pub struct ChannelProducerSendFuture<'a, T> {
    inner: &'a mut ChannelProducer<T>,
    future: PollEscapeSend<Result<(), SendError>>,
}






impl<'a, T> Future for ChannelProducerSendFuture<'a, T>
{
    type Output = Result<(), SendError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
       unsafe{self.map_unchecked_mut(|s| &mut s.future)}.poll(cx)
        }
    }



impl<T: Send + 'static> ChannelProducer<T> {
    pub fn send(&mut self, data: T) -> ChannelProducerSendFuture<T> {
        let move_shared = self.shared.clone();
        ChannelProducerSendFuture {
            inner: self,
            future: PollEscapeSend::escape(async move {
                //need to declare a special scope so we can drop our lock before we await
                let future = {
                    let perf = dlog::perfwarn_begin!("ampsc::perfwarn_locked");
                    let mut lock = move_shared.perfwarn_locked.lock().unwrap();
                    if move_shared.consumer_hangup.load(Ordering::Relaxed) {
                        return Err(SendError::ConsumerHangup);
                    }
                    if lock.pending_consumer.is_some() {
                        lock.pending_consumer.take().unwrap().send(Ok(data));
                        return Ok(())
                    }
                    else {
                        //we need to fall out of this scope so we can drop lock forever.
                        let (sender, future) = continuation::continuation();
                        lock.pending_producers.push(sender);
                        //fall out here and drop our lock
                        future
                    }
                };
                let upstream_continuation = future.await;
                upstream_continuation.send(Ok(data));
                Ok(())
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
            todo!()
        }
    }
}

#[test]
fn test_push() {
    let (mut producer, mut consumer) = channel();

    truntime::spawn_on(async move  {
        let mut producer = producer;
        producer.send(1).await.unwrap();
    });

    let r = truntime::spin_on(consumer.receive()).unwrap();
    assert_eq!(r, 1);
}
