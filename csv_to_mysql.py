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
	add_newline = ("INSERT INTO trainuser_new "
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
	itemdict = dict()
	with open('tianchi_mobile_recommend_train_item.csv', "r") as f:
		next(f)
		for row in f:
			row = row.strip().split(',')
			if row[0] not in itemdict:
				itemdict[row[0]] = 1

	posdict = dict()
	negdict = dict()
	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		next(f)
		for row in f:
			line = row.strip().split(',')
			dttime = line[5].split(' ')[0]
			node = line[0]+","+line[1]+","+line[2]
			nodeid = line[0]+"_"+line[1]
			if dttime == splitdate and line[1] in itemdict:
				if int(line[2]) == 4:
					if nodeid not in posdict:
						posdict[nodeid] = node
				else:
					if nodeid not in negdict:
						negdict[nodeid] = node
	poslist = list(); neglist = list()
	endlist = list()
	for k in posdict:
		poslist.append(posdict[k])
		endlist.append(posdict[k])
	for k in negdict:
		if k not in posdict:
			neglist.append(negdict[k])
	print len(posdict)
	print len(poslist)
	print len(negdict)
	print len(neglist)
	number =  (len(poslist)*10) / float(len(neglist))
	X_train, newneglist = cross_validation.train_test_split(neglist, test_size=number,random_state=60)
	print len(newneglist)
	endlist.extend(newneglist)
	shuffle(endlist)
	with open(filename, "w") as fw:
		for i in endlist:
			fw.write(i+"\n")
'''
def createTestData(isSample=False):
	preditemlist = dict()
	with open("tianchi_mobile_recommend_train_item.csv", "r") as f:
		next(f)
		for line in f:
			line = line.strip().split(',')
			preditemlist[line[0]] = 0
	
	userbuylist = dict()
	predictuseritem = dict()
	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		next(f)
		for row in f:
			row = row.strip().split(',')
			node = row[0]+","+row[1]
			dttime = row[5].split(' ')[0]
			if dttime != '2014-12-18' and dttime != '2014-12-12' and row[1] in preditemlist:
				if int(row[2]) == 4:
					userbuylist[node] = 1
				if node not in predictuseritem:
					predictuseritem[node] = 1
	useritem = list()
	for node in predictuseritem:
		if node not in userbuylist:
			useritem.append(node)
	shuffle(useritem)
	with open("offline_test_data.csv", "w") as fw:
		for node in useritem:
			fw.write(node+"\n")
'''
def createTestData(isSample=False):
	preditemlist = dict()
	with open("tianchi_mobile_recommend_train_item.csv", "r") as f:
		next(f)
		for line in f:
			line = line.strip().split(',')
			preditemlist[line[0]] = 0
	
	posdict = dict()
	negdict = dict()
	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		next(f)
		for row in f:
			row = row.strip().split(',')
			nodeid = row[0]+","+row[1]
			node = row[0]+","+row[1]+","+row[2]
			dttime = row[5].split(' ')[0]
			if dttime == '2014-12-18' and row[1] in preditemlist:
				if int(row[2]) == 4:
					if nodeid not in posdict:
						posdict[nodeid] = node
				else:
					if nodeid not in negdict:
						negdict[nodeid] = node
	poslist = list(); neglist = list()
	endlist = list()
	for k in posdict:
		poslist.append(posdict[k])
		endlist.append(posdict[k])
	for k in negdict:
		if k not in posdict:
			neglist.append(negdict[k])
	print len(posdict)
	print len(poslist)
	print len(negdict)
	print len(neglist)
	with open("offline_test_data_in_p.csv", "w") as fw:
		for node in endlist:
			fw.write(node+"\n")

'''
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
'''


def createToBePredictedData():
	preditemlist = dict()
	with open("tianchi_mobile_recommend_train_item.csv", "r") as f:
		next(f)
		for line in f:
			line = line.strip().split(',')
			preditemlist[line[0]] = 0
	
	userbuylist = dict()
	predictuseritem = dict()
	number = 0
	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		next(f)
		for row in f:
			row = row.strip().split(',')
			node = row[0]+","+row[1]
			dttime = row[5].split(' ')[0]
			if dttime != '2014-12-12':
				if int(row[2]) == 4:
					userbuylist[node] = 1
				if row[1] in preditemlist and node not in predictuseritem:
					predictuseritem[node] = 1
			else:
				number += 1
	print number
	useritem = list()
	for node in predictuseritem:
		if node not in userbuylist:
			useritem.append(node)
	shuffle(useritem)
	with open("online_test_data.csv", "w") as fw:
		for node in useritem:
			fw.write(node+"\n")

if __name__ == "__main__":
	#createTrainData('offline_train_data_6.csv', '2014-12-17')
	#createTrainData('online_train_data.csv', '2014-12-18')
	#createTestData(False)
	#createToBePredictedData()
	'''
	templist = list()
	with open("offline_test_data.csv","r") as f:
		for row in f:
			templist.append(row)
	X_train, y_train = cross_validation.train_test_split(templist, test_size=0.2)
	traindict = dict(); testdict = dict()
	with open("offline_test_data_80.csv","w") as fw:
		for i in X_train:
			fw.write(i)
			line = i.strip().split(',')
			node = line[0]+"_"+line[1]
			traindict[node] = 1
	with open("offline_test_data_20.csv","w") as fw:
		for i in y_train:
			fw.write(i)
			line = i.strip().split(',')
			node = line[0]+"_"+line[1]
			testdict[node] = 1
	with open("testdata_feature_offline_80.csv","w") as fw8:
		with open("testdata_feature_offline_20.csv","w") as fw2:
			with open("testdata_feature_offline.csv","r") as f:
				for row in f:
					line = row.strip().split(',')
					node = line[0]+"_"+line[1]
					if node in traindict:
						fw8.write(row)
					else:
						fw2.write(row) 
	'''
	
	with open("testdata_feature_offline_pre_by_other.txt", "w") as fw:
		testdict = dict()
		with open("offline_predict_data.txt", "r") as f:
			for row in f:
				row = row.strip().split(',')
				if float(row[2]) > 0.5:
					node = row[0]+"_"+row[1]
					testdict[node] = 1
		with open("testdata_feature_offline_post.csv", "r") as f:
			for row in f:
				line = row.strip().split(',')
				node = line[0]+"_"+line[1]
				if node in testdict:
					fw.write(row)
	
	'''
	offline = dict()
	with open("offline_test_data_in_p.csv","r") as f:
		for row in f:
			row = row.strip().split(',')
			if int(row[2]) == 4:
				node = row[0]+"_"+row[1]
				offline[node] = 1	
	rfdict = dict()
	with open("offline_predict_rf.txt", "r") as f:
		for row in f:
			row = row.strip().split(',')
			node = row[0]+"_"+row[1]
			if float(row[2]) > 0.5 and node in offline:
				rfdict[node] = 1
	lrdict = dict()
	with open("offline_predict.txt", "r") as f:
		for row in f:
			row = row.strip().split(',')
			node = row[0]+"_"+row[1]
			if float(row[2]) > 0.5 and node in offline:
				lrdict[node] = 1	
	number = 0
	for key in rfdict:
		if key in lrdict:
			number+=1
	print "rf number: "+str(len(rfdict))
	print "lr number: "+str(len(lrdict))
	print "interact number: "+str(number)
	'''
	'''
	useritemlabel = dict()
	with open("offline_predict.txt", "r") as f:
		for row in f:
			row = row.strip().split(',')
			node = row[0]+"_"+row[1]
			if float(row[2]) > 0.5:
				useritemlabel[node] = 1
			else:
				useritemlabel[node] = 0

	inuseritem = dict()
	inuseritem_a = dict()
	with open("offline_test_data_in_p.csv","r") as f:
		for row in f:
			row = row.strip().split(',')
			node = row[0]+"_"+row[1]
			if int(row[2]) == 4:
				inuseritem[node]=1
				if row[0] not in inuseritem_a:
					inuseritem_a[row[0]] = 1

	number_1 = 0 
	number_2 = 0 
	number_3 = 0 
	number_4 = 0
	userdict = dict();posnum=0;negnum=0
	tempa = 0
	with open("newdata/traindata_feature_offline.csv","r") as f:
		for row in f:
			row=row.strip().split(',')
			feat = [float(n) for n in row[2:]]
			node = row[0]+"_"+row[1]
			tempnum = feat[4]
			if tempnum > 500:
				number_1+=1
				if row[0] not in userdict:
					userdict[row[0]] = 1
				if node in inuseritem:
					posnum +=1
				if row[0] in inuseritem_a:
					tempa+=1
			elif tempnum>300 and tempnum<=500:
				number_2+=1
			elif tempnum>100 and tempnum<=300:
				number_3+=1
			else:
				number_4+=1
	print number_1,number_2,number_3,number_4
	print len(userdict)
	print posnum,negnum
	print tempa
	'''
	