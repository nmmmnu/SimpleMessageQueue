#!/usr/bin/python

import memcache

from hashlib import md5

from simplelocker import SimpleLocker

class SimpleMessageQueue:
	"""
	Object for getting a lock over memcache
	"""
	r    = None
	
	def __init__(self, r, name, timeout = 5, debug = False):
		"""
		Create a new SimpleLocker object. Will create following memcache keys:
		q:head - queue head
		q:tail - queue tail
		q:lock - queue lock, expiration will be set
		q:uniq:xxxxx - unique lock based on md5 of the message
		q:0001 - queue message
		
		@param r: memcache object
		@param name: name of the queue. Will be used for prefix on queue keys.
		@param timeout: optional timeout for SimpleLocker
		"""
		self.r        = r
		self.name     = name
		self.lock     = SimpleLocker(r, "%s:lock" % name, timeout, debug)
		self.debug2   = debug
		
		self.key_head = "%s:%s" % (name, "head")
		self.key_tail = "%s:%s" % (name, "tail")



	def debug(self, s):
		if self.debug2:
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
		"""
		Check if message is in the queue
		@param message: the message.
		@return: True if message is in the queue, else false
		"""
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
		"""
		Put message is in the queue
		@param message: the message.
		@param uniq: check if message is alreay in the queue and do not insert twice.
		@return: True if message is inserted or in the queue, else false
		"""
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
		"""
		Get message from the queue
		@return: the message or None if an error occure.
		"""
		# Accuire lock
		if self.lock.accuire() :
			# Get the message
			message = self._get()
			
			# Release the lock
			self.lock.release()
			
			return message

		return None



	def info(self):
		"""
		Get info about the queue
		@return: (size of the queue, head, tail)
		"""
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
		"""
		Alias of put()
		"""
		return self.put(message)
		
	def sadd(self, message):
		"""
		Alias of put()
		"""
		return self.put(message)
		
	def pop(self):
		"""
		Alias of get()
		"""
		return self.get()
		
	def spop(self):
		"""
		Alias of get()
		"""
		return self.get()

	def sismember(self, message):
		"""
		Alias of ismember()
		"""
		return self.ismember(message)


if __name__ == "__main__":
	mqmc = memcache.Client(["localhost:1980"], debug=0)
	mq   = SimpleMessageQueue(mqmc, "niki", debug = True)
	
	"""
	for i in xrange(50000):
		if i % 10000 == 0 :
			print "%8d done" % i
			
		if not mq.put("msg2 # %08d" % i) :
			print "error"
	"""
		
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
	

	

