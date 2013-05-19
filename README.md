SimpleMessageQueue
==================

Simple disk based message queue

With this project we want build message queue and to achieve following:

- Messages will be strings. If you need to store something different, is up to you to serialize it.
- A message must be delivered only once and must *NOT* be lost.
- Message can be lost only if there are uncorrectable error with data store.
- Message queue must not get "stuck". This means any errors must be self-correctable.
- Multiple queues must be supported.
- Store unique messages only once, in a way similar to Redis sets - sadd() / spop() / sismember()
- For storage engine we will use memcached. This is because there are lots of compatible services for persistent storage with memcached protocol - memcachedb, tokyocabinet, kyotocabinet, membase etc.

What do not need to be supported

- Messages do not need to be retreived in FIFO order.
- Server does not need to be scalled.

Misc notes.

The queue works in a well known "Snake-Byte" way, there is a pointer to the header and pointer to the tail.

New messages get stored on the head, and old messages are collected from the tail.

However, because we work really concurrent, we can not just put or pop messages. There are few situations when an error can occure and message can be lost.

For example when a message is pop, a tail is increased, then if message is missing, nobody can be sure if this is empty message, or this is running put() request, that still did not saved the message.
One possible correction is to wait 5 seconds or so and try collect the message again. However this method is very slow, especially on near empty queue.

There are lot more problems that can not be solved easyly using pure memcached functions.

For this reason, this implementation employ a per queue lock. At some moment of time, only one put() or get() can be performed. This is slow method, but there is no way to lost a message.

Main way to set a lock is to set a key, and then to delete it. However, if something happen in between, for example connection to the database get down,
the queue will stuck for loooong time.

To avoid this, we employ different kind of lock - we set a key that will expire in 5 seconds or so. 
This makes another problem - when we delete the key, we could delete some other process lock, because our key may be expired already.
This could be fixed using unique ID such UUID4. However, to solve this, we delete the key, only if less 5 seconds passed from accuiring the lock.

This immediately means we can not use memcachedb or tokyocabinet as backend, because they do not have expire functionality.






