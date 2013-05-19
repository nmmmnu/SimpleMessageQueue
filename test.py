#!/usr/bin/python

import simplemessagequeue
import memcache

mqmc = memcache.Client(["localhost:1980"], debug=0)
mq   = simplemessagequeue.SimpleMessageQueue(mqmc, "niki", debug = True)

	
mq.put("aaaa")
print mq.get()

print mq.info()
	
