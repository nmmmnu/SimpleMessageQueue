#!/usr/bin/python

import memcache

from hashlib import md5

import time

#from uuid import uuid4

_debug = False

class SimpleLocker:
	r    = None
		
	def __init__(self, r, key, timeout = 5):
		self.r       = r
		
		self.key     = key
		
		self.timeout = timeout
		self.start   = 0
		self.quant   = 0.33
		
	def debug(self, s):
		if _debug:
			print s
	
	def accuire(self):
		self.start = time.time()
		while True:
			if self.r.add(self.key, 1, self.timeout) :
				self.debug("Lock accuired")
				return True
				
			if time.time() - self.start > self.timeout:
				self.debug("Lock accuire failed")
				return False
			
			time.sleep(self.quant)
			self.debug("Lock wait")

	def release(self):
		if time.time() - self.start < self.timeout - self.quant:
			self.debug("Lock released")
			self.r.delete(self.key)
			return
		
		self.debug("Lock release failed")
			


class SimpleMessageQueue:
	r    = None
	
	def __init__(self, r, name):
		self.r        = r
		self.name     = name
		self.lock     = SimpleLocker(r, "%s:lock" % name)
		
		self.key_head = "%s:%s" % (name, "head")
		self.key_tail = "%s:%s" % (name, "tail")



	def debug(self, s):
		if _debug:
			print s



	def _get_uniq_key(self, message):
		try:
			um = unicode(message).encode('utf8')
		except UnicodeDecodeError:
			um = message
			
		return self.name + ":uniq:" + md5(um).hexdigest()
	
	
	
	def _check_pointer(self, pointer, pointer_name = "pointer"):
		try:
			pointer = long(pointer)
		except ValueError:
			pointer = 0
		except TypeError:
			pointer = 0
		
		if pointer == 0:
			self.debug("%s is 0. Unset or overflow detected" % pointer_name)
			# (remember we are locked)
			return None
		
		return pointer
	
	def _incr_pointer(self, key, pointer_name = "pointer"):
		self.r.add(key, 0)
		pointer = self.r.incr(key)
		
		return self._check_pointer(pointer, pointer_name)
	
	def _get_pointer(self, key, pointer_name = "pointer"):
		pointer = self.r.get(key)
		
		return self._check_pointer(pointer, pointer_name)



	def ismember(self, message):
		uk = self._get_uniq_key(message)
		
		if self.r.get(uk) :
			# Already in the set
			return True

		return False
	
	
	
	def _put(self, message):
		# Incr "Snake-Byte" head
		head = self._incr_pointer(self.key_head, "head")
		
		if head:
			data_key = "%s:%ld" % (self.name, head)
			
			if self.r.set(data_key, message) :
				self.debug("PUT() completed")

				# uniq key is set regardless
				uk = self._get_uniq_key(message)
				self.r.set(uk, 1)

				return True
			
			# Failed, try to decr back
			self.r.decr(self.key_head)
		
		self.debug("PUT() failed")
		
		return False
	
	def put(self, message, uniq=True):
		if uniq:
			# Check uniq key, it does not need lock
			if self.ismember(message) :
				self.debug("Already in the queue")
				return True
		
		# Accuire lock
		if self.lock.accuire() :
			# Put the message
			self._put(message)
			
			# Release the lock
			self.lock.release()
			
			return True

		return False



	def _get(self):
		# Get "Snake-Byte" head.
		head = self._get_pointer(self.key_head, "head")
		
		if not head:
			return False

		# Incr "Snake-Byte" tail.
		tail = self._incr_pointer(self.key_tail, "tail")
		
		if not tail:
			return False
			
		if tail > head:
			self.debug("Empty queue")
			
			# Rollback, remember we are locked
			# Instead of decr,
			# we will implictly set the tail to head,
			# seems more correct.
			self.r.set(self.key_tail, head)
			
			return False

		data_key = "%s:%ld" % (self.name, tail)
		
		message = self.r.get(data_key)

		# Delete data, regardless
		self.r.delete(data_key)
		
		uk = self._get_uniq_key(message)

		self.r.delete(uk)

		return message

	def get(self):
		# Accuire lock
		if self.lock.accuire() :
			# Get the message
			message = self._get()
			
			# Release the lock
			self.lock.release()
			
			return message

		return None



	def info(self):
		# No need lock here.
		head = self._get_pointer(self.key_head, "head")
		tail = self._get_pointer(self.key_tail, "tail")
		
		if head is None :
			head = 0

		if tail is None :
			tail = 0

		size = head - tail
		
		if size < 0 :
			size = 0
		
		return (size, head, tail)





	# For Redis compatibility and Common sense names
	def add(self, message):
		return self.put(message)
		
	def sadd(self, message):
		return self.put(message)
		
	def pop(self):
		return self.get()
		
	def spop(self):
		return self.get()

	def sismember(self, message):
		return self.ismember(message)


if __name__ == "__main__":
	mqmc = memcache.Client(["localhost:1980"], debug=0)
	mq   = SimpleMessageQueue(mqmc, "niki")
	
	"""
	for i in xrange(50000):
		if i % 10000 == 0 :
			print "%8d done" % i
			
		if not mq.put("msg2 # %08d" % i) :
			print "error"
	"""
	
	_debug = True
	
	print mq.get()
	
	"""
	while True:
		msg = mq.get()	
		if msg :
			print msg
		else:
			break
	"""
	
	print mq.info()
	

	

