import memcache

import time

#from uuid import uuid4

class SimpleLocker:
	"""
	Object for getting a lock over memcache
	"""
	r    = None
		
	def __init__(self, r, key, timeout = 5, debug = False):
		"""
		Create a new SimpleLocker object
		
		@param r: memcache object
		@param key: name of memcache key
		@param timeout: optional timeout
		@param debug: print debug messages
		"""
		self.r       = r
		
		self.key     = key
		
		self.timeout = timeout
		self.start   = 0
		self.quant   = 0.33
		
		self.debug2  = debug
		
	def debug(self, s):
		if self.debug2:
			print s
	
	def accuire(self):
		"""
		Accuire a lock
		"""
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
		"""
		Release the lock
		"""
		if time.time() - self.start < self.timeout - self.quant:
			self.debug("Lock released")
			self.r.delete(self.key)
			return
		
		self.debug("Lock release failed")
