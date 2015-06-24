import re
from threading import Thread
from time import sleep

# Spark settings
# rdd = sc.textFile("/Users/x36meng/test.txt")

class tests():
	# count query
	def count(self):
		rdd.count()
		
	# filter query: contains certain words: two search engines
	def mention(self):
		rdd.filter(lambda line: "google" in line or "bing" in line).count()
		
	# reg query 1: match against major websites	
	def reg1(self):
		rdd.filter(lambda line: re.search('facebook.[a-zA-Z0-9]|plus.google.[a-zA-Z0-9]|twitter.[a-zA-Z0-9]|[a-zA-Z0-9].qzone.qq.com|habbo.[a-zA-Z0-9]|vk.[a-zA-Z0-9]|linkedin.[a-zA-Z0-9]|instagram.[a-zA-Z0-9]|tagged.[a-zA-Z0-9]|gmail.[a-zA-Z0-9]|box.[a-zA-Z0-9]|apple.[a-zA-Z0-9]|dropbox.[a-zA-Z0-9]|google.[a-zA-Z0-9]|battlenet.[a-zA-Z0-9]|pinterest.[a-zA-Z0-9]|drive.google.[a-zA-Z0-9]',line)).count()

	# reg query 2: match against certain actions for facebook
	def reg2(self):
		rdd.filter(lambda line: re.search('facebook\sdisconnect|facebook\snotfound|facebook\sslow|facebook\sredirect',line)).count()

	# reg query 3: match against certain actions for google
	def reg3(self):
	    rdd.filter(lambda line: re.search('google\sdisconnect|google\snotfound|google\sslow|google\sredirect', line)).count()





	    