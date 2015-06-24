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
	rdd.filter(lambda line: re.search('dropbox.[a-zA-Z0-9]|linkedin.[a-zA-Z0-9]',line)).count()
	
	rdd.map(lambda line: re.search("facebook.[a-zA-Z0-9]|plus.google.[a-zA-Z0-9]|twitter.[a-zA-Z0-9]|[a-z]{2,3}.qzone.qq.com|habbo.[a-z]{2,3}|vk.[a-z]{2,3}|linkedin.[a-z]{2,3}|instagram.[a-z]{2,3}|tagged.[a-z]{2,3}|gmail.[a-z]{2,3}|box.[a-z]{2,3}|apple.[a-z]{2,3}|dropbox.[a-z]{2,3}|google.[a-z]{2,3}|www.battlenet.[a-z]{2,3}|www.pinterest.[a-z]{2,3}|drive.google.[a-z]{2,3}", line)..count()
	
	