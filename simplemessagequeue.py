#!/usr/bin/python

import memcache

from hashlib import md5

class SimpleMessageQueue:
	r    = None
	
	def __init__(self, r):
		self.r = r



	def _atomic_incr(self, key):
		if self.r.add(key, 1):
			return str(1)

		previous = self.r.get(key)		
		if previous is None:
			previous = 0

		pointer  = self.r.incr(key)

		if pointer is None:
			# this can't happen, unless disconected
			return None
			
		if long(previous) > long(pointer):
			# Overflow detected...
			return None

		return str(pointer)


	
	def _key_uniq(self, name, message):
		try:
			um = unicode(message).encode('utf8')
		except UnicodeDecodeError:
			um = message
			
		return name + ":uniq:" + md5(um).hexdigest()
		
		
		
	def ismember(self, name, message):
		uk = self._key_uniq(name, message)
		
		if self.r.get(uk) :
			# Already in the set
			return True

		return False
		
		
	
	def put(self, name, message, uniq=True):
		if uniq:
			# Check uniq key
			if self.ismember(name, message) :
				# Already in the set
				return True
		
		# uniq key is set regardless
		uk = self._key_uniq(name, message)
		self.r.set(uk, 1)

		# Get "Snake-Byte" head
		head = self._atomic_incr(name + ":head")
		
		if head is None:
			return False
		


		k = name + ":" + head
		if self.r.set(k, message) :
			return True
			
			
		
		return False
	
	
	
	def get(self, name):
		# Get "Snake-Byte" head, we will probably need it.
		head = self.r.get(name + ":head")
		# Get "Snake-Byte" tail.
		tail = self._atomic_incr(name + ":tail")
		
		if tail is None:
			return None
		
		k = name + ":" + tail
		
		message = self.r.get(k)



		# Delete the data, regardless
		self.r.delete(k)



		if message is None:
			# The tail is probably outside the queue.

			if head is not None :
				if long(tail) > long(head) :
					self.r.set(name + ":tail", head)
					
			# No need to delete any keys, since there 

			return None



		# Delete uniq
		uk = self._key_uniq(name, message)

		self.r.delete(uk)
		
		

		return message
		
		
		
	# For Redis compatibility and Common sense names
	def add(self, name, message):
		return self.put(name, message)
		
	def sadd(self, name, message):
		return self.put(name, message)
		
	def pop(self, name):
		return self.get(name)
		
	def spop(self, name):
		return self.get(name)

	def sismember(self, name, message):
		return self.ismember(name, message)



	def info(self, name):
		head = self.r.get(name + ":head")
		tail = self.r.get(name + ":tail")
		
		if head is None :
			head = 0
			size = 0

		if tail is None :
			tail = 0

		head = long(head)
		tail = long(tail)

		size = head - tail
		
		if size < 0 :
			size = 0
		
		return (size, head, tail)
	

