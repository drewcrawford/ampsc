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
use poll_escape::PollEscape;

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
    optimize_data: atomiclock_async::AtomicLockAsync<Locked<T>>,
    consumer_hangup: AtomicBool,
    producer_hangup: AtomicU8,
    /*
    you might imagine some kinda atomic solution here.
     */
    pending_consumer_perfwarn: sync::Mutex<Option<continuation::Sender<Result<(),RecvError>>>>,

    /*
    perfwarn!
    You might expect this to be, say, some fast datastructure.  One idea you might have is that if only we had a kind of lock-free queue that was optimized for this type.
    Reallocation is hard but we have an async context here, maybe we can use backpressure?  Maybe we split sender and receiver?  Behind this door you are trying to implement ampsc in terms of
    this underlying channel, but that is the very channel you need to implement.

    ok so we don't want to spill into the async context.  The other options are:

    1.  Preallocate a fixed size buffer and hope we don't exceed it.  On the one hand, the memory in your system is a fixed-size buffer, there's always some upper limit.  On the other hand, maybe you wanted to use your memory, so wasting it is not ideal.
    2.  Some kind of synchronous solution, like we mostly insert in a lock-free queue but there is a lock to handle reallocation.  Pin is a complication for reallocation, but I think continuation is usually unpin.  Anyway for this we probably want a generic solution with multiple uses.

    Let's punt ont that and use a dumb mutex for now.
     */
    pending_producers_perfwarn: sync::Mutex<Vec<continuation::Sender<Result<(), SendError>>>>,
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
        producer_hangup: AtomicU8::new(1),
        pending_consumer_perfwarn: sync::Mutex::new(None),
        pending_producers_perfwarn: sync::Mutex::new(Vec::new()),
    });
    (ChannelProducer { shared: shared.clone() }, ChannelConsumer { shared })
}

#[derive(Debug)]
pub struct ChannelConsumer<T> {
    shared: Arc<Shared<T>>,
}

pub struct ChannelConsumerRecvFuture<'a, T> {
    future: PollEscape<Result<T,RecvError>>,
    inner: &'a mut ChannelConsumer<T>,
}

impl <'a, T> Future for ChannelConsumerRecvFuture<'a, T> {
    type Output = Result<T,RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe{self.map_unchecked_mut(|s| &mut s.future)}.poll(cx)
        // let (sender,receiver) = poll_escape::waker_escape::waker_escape();
        // let pinned = std::pin::pin!(poll_escape::PollEscape::escape(sender, async {
        //     if self.inner.shared.producer_hangup.load(Ordering::Relaxed) == 0 {
        //         return poll_escape::Poll::Ready(Err(RecvError::ProducerHangup));
        //     }
        //     let mut lock = self.inner.shared.optimize_data.lock().await;
        //     if let Some(data) = lock.data.take() {
        //         //it's ok to pop here because we always insert fresh wakers
        //         self.inner.shared.pending_producers.wake_one_pop();
        //         return poll_escape::Poll::Ready(Ok(data));
        //     }
        //     else {
        //         self.inner.shared.pending_consumer.push_if_empty(&receiver.into_waker());
        //         poll_escape::Poll::Escape
        //     }
        // }));
        // pinned.poll(cx)
    }
}


impl<T: 'static> ChannelConsumer<T> {
    pub fn receive(&mut self) -> ChannelConsumerRecvFuture<'_, T> {
        let shared = self.shared.clone();
        ChannelConsumerRecvFuture {
            inner: self,
            future: PollEscape::escape(async move {
                loop {
                    let mut lock = shared.optimize_data.lock().await;

                    if shared.producer_hangup.load(Ordering::Relaxed) == 0 {
                        return Err(RecvError::ProducerHangup);
                    }

                    if let Some(data) = lock.data.take() {
                        drop(lock);
                        //wake one pop
                        shared.pending_producers_perfwarn.lock().unwrap().pop().map(|sender| {
                            sender.send(Ok(()));
                        });
                        return Ok(data);
                    } else {
                        drop(lock);
                        let (sender,future) = continuation::continuation();
                        perfwarn!("pending_consumer_perfwarn", {
                        let mut l = shared.pending_consumer_perfwarn.lock().unwrap();
                        assert!(l.is_none());
                        *l = Some(sender);
                    });
                        future.await; //block until next wakeup
                    }
                }

            }),
        }
    }
}

impl<T> Drop for ChannelConsumer<T> {
    fn drop(&mut self) {
        self.shared.consumer_hangup.store(true, std::sync::atomic::Ordering::Release);
        perfwarn!("pending_producers_perfwarn", {
            for producer in self.shared.pending_producers_perfwarn.lock().unwrap().drain(..) {
                producer.send(Err(SendError::ConsumerHangup));
            }
        });

    }
}


#[derive(Debug)]
pub struct ChannelProducer<T> {
    shared: Arc<Shared<T>>,
}

#[derive(Debug)]
pub struct ChannelProducerSendFuture<'a, T> {
    inner: &'a mut ChannelProducer<T>,
    future: PollEscape<Result<(), SendError>>,
}






impl<'a, T> Future for ChannelProducerSendFuture<'a, T>
{
    type Output = Result<(), SendError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
       unsafe{self.map_unchecked_mut(|s| &mut s.future)}.poll(cx)
        }


    }



impl<T: 'static> ChannelProducer<T> {
    pub fn send(&mut self, data: T) -> ChannelProducerSendFuture<T> {
        let move_shared = self.shared.clone();
        ChannelProducerSendFuture {
            inner: self,
            future: PollEscape::escape(async move {
                let mut lock = move_shared.optimize_data.lock().await;
                if move_shared.consumer_hangup.load(Ordering::Relaxed) {
                    return Err(SendError::ConsumerHangup);
                }
                else if let Some(_) = lock.data.as_ref() {
                    drop(lock);
                    let (sender,future) = continuation::continuation();
                    perfwarn!("pending_producers_perfwarn", {
                        move_shared.pending_producers_perfwarn.lock().unwrap().push(sender);
                    });
                    return future.await;
                } else {
                    lock.data = Some(data);
                    drop(lock);
                    move_shared.pending_consumer_perfwarn.lock().unwrap().take().map(|sender| {
                        sender.send(Ok(()));
                    });
                    return Ok(());
                }
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
            perfwarn!("pending_consumer_perfwarn", {
                self.shared.pending_consumer_perfwarn.lock().unwrap().take().map(|sender| {
                    sender.send(Err(RecvError::ProducerHangup));
                });
            });
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
