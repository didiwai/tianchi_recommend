# -*- coding: utf-8 -*- 

import mysql.connector
import datetime as dt
from sklearn import cross_validation
from sklearn.linear_model import LogisticRegression
from random import shuffle
import datetime

def csvToMysql():
	cnx = mysql.connector.connect(user='root', password='1234', database='tianchi')
	cursor = cnx.cursor()
	add_newline = ("INSERT INTO trainuser_all "
               "(userid, itemid, behavior, usergeohash, itemcategory, oritime, pytime, hour) "
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
	'''
	with open("train_data.csv", "r") as f:
		for line in f.readlines():
			line = line.strip().split(',')
			beh = int(line[2])
			templist = [0,0,0,0]
			templist[beh-1] = 1
			data_newline = (line[0], line[1], beh, templist[0], templist[1], templist[2], templist[3], line[3], line[4], line[5], line[5].split(" ")[0], line[5].split(" ")[1])
			cursor.execute(add_newline, data_newline)
	'''
	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		next(f)
		for line in f:
			line = line.strip().split(',')
			beh = int(line[2])
			data_newline = (line[0], line[1], beh, line[3], line[4], line[5], line[5].split(" ")[0], line[5].split(" ")[1])
			cursor.execute(add_newline, data_newline)
	cnx.commit()
	cursor.close()
	cursor.close()

def createTrainData(filename, splitdate):
	posdict = dict()
	negdict = dict()
	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		for row in f.readlines():
			line = row.strip().split(',')
			datatime = line[5].split(' ')[0]
			node = line[0]+","+line[1]+","+line[2]
			if datatime == splitdate:
				if int(line[2]) == 4:
					if node not in posdict:
						posdict[node] = 1
				else:
					if node not in negdict:
						negdict[node] = 1
	poslist = list(); neglist = list()
	endlist = list()
	for k in posdict:
		poslist.append(k)
		endlist.append(k)
	for k in negdict:
		neglist.append(k)
	print len(posdict)
	print len(poslist)
	print len(negdict)
	print len(neglist)
	number =  (len(poslist)*10) / float(len(neglist))
	X_train, newneglist = cross_validation.train_test_split(neglist, test_size=number)
	print len(newneglist)
	endlist.extend(newneglist)
	shuffle(endlist)
	with open(filename, "w") as fw:
		for i in endlist:
			fw.write(i+"\n")
'''
def createTestData(isSample=True):
	posdict = dict()
	negdict = dict()
	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		for row in f.readlines():
			line = row.strip().split(',')
			datatime = line[5].split(' ')[0]
			node = line[0]+","+line[1]+","+line[2]
			if datatime == '2014-12-18':
				if int(line[2]) == 4:
					if node not in posdict:
						posdict[node] = 1
				else:
					if node not in negdict:
						negdict[node] = 1
	poslist = list(); neglist = list()
	endlist = list()
	for k in posdict:
		poslist.append(k)
		endlist.append(k)
	for k in negdict:
		neglist.append(k)
	print len(posdict)
	print len(poslist)
	print len(negdict)
	print len(neglist)
	if isSample:
		X_train, newneglist = cross_validation.train_test_split(neglist, test_size=0.1)
		print len(newneglist)
		endlist.extend(newneglist)
	else:
		endlist.extend(neglist)
	print "test number is:",len(endlist)
	shuffle(endlist)
	with open("offline_test_data.csv", "w") as fw:
		for i in endlist:
			fw.write(i+"\n")
'''

#预测商品子集
def createTestData(isSample=True):
	itemlist = dict()
	with open("tianchi_mobile_recommend_train_item.csv", "r") as f:
		next(f)
		for line in f:
			line = line.strip().split(',')
			itemlist[line[0]] = 0

	posdict = dict()
	negdict = dict()
	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		next(f)
		for row in f:
			line = row.strip().split(',')
			datatime = line[5].split(' ')[0]
			node = line[0]+","+line[1]+","+line[2]
			if datatime == '2014-12-18' and line[1] in itemlist:
				if int(line[2]) == 4:
					if node not in posdict:
						posdict[node] = 1
				else:
					if node not in negdict:
						negdict[node] = 1
	poslist = list(); neglist = list()
	endlist = list()
	for k in posdict:
		poslist.append(k)
		endlist.append(k)
	for k in negdict:
		neglist.append(k)
	print len(posdict)
	print len(poslist)
	print len(negdict)
	print len(neglist)
	if isSample:
		X_train, newneglist = cross_validation.train_test_split(neglist, test_size=0.1)
		print len(newneglist)
		endlist.extend(newneglist)
	else:
		endlist.extend(neglist)
	print "test number is:",len(endlist)
	shuffle(endlist)
	with open("offline_test_data_in_p.csv", "w") as fw:
		for i in endlist:
			fw.write(i+"\n")

def createToBePredictedData():
	preditemlist = dict()
	with open("tianchi_mobile_recommend_train_item.csv", "r") as f:
		next(f)
		for line in f:
			line = line.strip().split(',')
			preditemlist[line[0]] = 0
	
	userbuylist = dict()
	predictuseritem = dict()
	split_date = datetime.date(2014, 12, 11)
	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		next(f)
		for row in f:
			row = row.strip().split(',')
			node = row[0]+","+row[1]
			pytime = row[5].split(" ")[0]
			ntime = datetime.datetime.strptime(pytime, '%Y-%m-%d').date()
			if ntime > split_date:
				if int(row[2]) == 4:
					userbuylist[node] = 1
				if row[1] in preditemlist and node not in predictuseritem:
					predictuseritem[node] = 1
	useritem = list()
	for node in predictuseritem:
		if node not in userbuylist:
			useritem.append(node)
	shuffle(useritem)
	with open("online_test_data.csv", "w") as fw:
		for node in useritem:
			fw.write(node+"\n")

def createItemList():
	itemlist = dict()
	with open("tianchi_mobile_recommend_train_user.csv") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			if row[1] not in itemlist:
				itemlist[row[1]] = 1
	with open("item.txt","w") as fw:	
		for key in itemlist:
			fw.write(key+"\n")

def createUserList():
	userlist = dict()
	with open("tianchi_mobile_recommend_train_user.csv") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			if row[0] not in userlist:
				userlist[row[0]] = 1
	with open("user.txt","w") as fw:	
		for key in userlist:
			fw.write(key+"\n")

def sampleData():
	poslist = list()
	neglist = list()
	posdict = dict()
	negdict = dict()
	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		next(f)
		for line in f:
			row = line.strip().split(',')
			if int(row[2]) == 4:
				if line not in posdict:
					posdict[line] = 1
				#poslist.append(line)
			else:
				if line not in negdict:
					negdict[line] = 1
				#neglist.append(line)
	for k in posdict:
		poslist.append(k)
	for k in negdict:
		neglist.append(k)
	print "end create pos and neg list"
	print len(poslist)
	print len(neglist)
	X_train, newneglist = cross_validation.train_test_split(neglist, test_size=0.13)
	print len(newneglist)
	with open("train_data.csv", "w") as fw:
		for num, n in enumerate(newneglist):
			if num % 10 == 0 and len(poslist) != 0:
				temp = poslist.pop()
				fw.write(temp)
			fw.write(n)
		if len(poslist) != 0:
			for p in poslist:
				fw.write(p)

if __name__ == "__main__":
	#createTrainData('offline_train_data.csv', '2014-12-17')
	createTrainData('online_train_data.csv', '2014-12-18')
	#createTestData(False)
	#createToBePredictedData()
