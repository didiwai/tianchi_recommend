import mysql.connector
import datetime as dt
from sklearn import cross_validation
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier


def genUserFeature(train):
	userfeat = dict()

	userlist = list()
	with open("user.txt", "r") as f:
		for row in f.readlines():
			row = row.strip()
			userlist.append(row)

	#read feature from db
	print "begin user feature"
	timelist = list()
	start_date = dt.date(2014, 11, 18)
	end_date = dt.date(2014, 12, 19)
	if not train:
		end_date = dt.date(2014, 12, 18)
	while start_date < end_date:
		timelist.append(start_date)
		start_date = start_date + dt.timedelta(days=1)
	
	cnx = mysql.connector.connect(user='root', password='1234', database='tianchi')
	cursor = cnx.cursor()

	if train:
		query = ("SELECT pytime,sum(behbrowse),sum(behcollect),sum(behcart),sum(behbuy) FROM trainuser_new "
         "WHERE userid='%s' group by pytime")
	else:
		query = ("SELECT pytime,sum(behbrowse),sum(behcollect),sum(behcart),sum(behbuy) FROM trainuser_new "
         "WHERE userid='%s' and pytime<>'2014-12-18' group by pytime")

	for num, user in enumerate(userlist):
		if num % 1000 == 0:
			print num, user
		tempuserfeat = dict()
		cursor.execute(query % user)

		for row in cursor:
			tempuserfeat[row[0]] = list()
			tempuserfeat[row[0]].append(int(row[1]))
			tempuserfeat[row[0]].append(int(row[2]))
			tempuserfeat[row[0]].append(int(row[3]))
			tempuserfeat[row[0]].append(int(row[4]))

		tempaddition = [0,0,0,0]
		for t in tempuserfeat:
			tempaddition = [sum(x) for x in zip(tempaddition, tempuserfeat[t])]

		for i in timelist:
			if i in tempuserfeat:
				userfeat.setdefault(user, []).extend(tempuserfeat[i])
			else:
				userfeat.setdefault(user, []).extend([0,0,0,0])
		userfeat[user].extend(tempaddition)
	
	cursor.close()
	cnx.close()
	print "end user feature"
	return userfeat

def genItemFeature(train):
	itemfeat = dict()
	print "begin item feature"
	#read feature from item feature file
	filename = ""
	if train:
		filename = "train_item_feature.csv"
	else:
		filename = "test_item_feature.csv"
	with open(filename, "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			itemfeat[row[0]] = [int(i) for i in row[1:]]
	print "end item feature"
	'''
	itemlist = list()
	with open("item.txt", "r") as f:
		for row in f.readlines():
			row = row.strip()
			itemlist.append(row)
	'''
	'''
	#every item only use four feature, eg: the frequency of 1,2,3,4(browse,collect,cart,buy)
	print "begin item fearure"
	cnx = mysql.connector.connect(user='root', password='1234', database='tianchi')
	cursor = cnx.cursor()

	if train:
		query = ("SELECT sum(behbrowse),sum(behcollect),sum(behcart),sum(behbuy) FROM trainuser_new "
         "WHERE itemid='%s' ")
	else:
		query = ("SELECT sum(behbrowse),sum(behcollect),sum(behcart),sum(behbuy) FROM trainuser_new "
         "WHERE itemid='%s' and pytime<>'2014-12-18' ")
	
	for num, item in enumerate(itemlist):
		if num % 1000 == 0:
			print num, item
		cursor.execute(query % item)
		for row in cursor:
			if row[0] == None:
				continue
			itemfeat[item] = list()
			itemfeat[item].append(int(row[0]))
			itemfeat[item].append(int(row[1]))
			itemfeat[item].append(int(row[2]))
			itemfeat[item].append(int(row[3]))

	cursor.close()
	cnx.close()
	print "end item feature"
	'''
	'''
	#item feature like the user feature, everyday has 4 feature and also have a
	#addition feature
	print "begin item fearure"

	timelist = list()
	start_date = dt.date(2014, 11, 18)
	end_date = dt.date(2014, 12, 19)
	if not train:
		end_date = dt.date(2014, 12, 18)
	while start_date < end_date:
		timelist.append(start_date)
		start_date = start_date + dt.timedelta(days=1)
	
	cnx = mysql.connector.connect(user='root', password='1234', database='tianchi')
	cursor = cnx.cursor()

	if train:
		query = ("SELECT pytime,sum(behbrowse),sum(behcollect),sum(behcart),sum(behbuy) FROM trainuser_new "
         "WHERE itemid='%s' group by pytime")
	else:
		query = ("SELECT pytime,sum(behbrowse),sum(behcollect),sum(behcart),sum(behbuy) FROM trainuser_new "
         "WHERE itemid='%s' and pytime<>'2014-12-18' group by pytime")

	for num, item in enumerate(itemlist):
		if num % 1000 == 0:
			print num
		tempitemfeat = dict()
		cursor.execute(query % item)

		for row in cursor:
			tempitemfeat[row[0]] = list()
			tempitemfeat[row[0]].append(int(row[1]))
			tempitemfeat[row[0]].append(int(row[2]))
			tempitemfeat[row[0]].append(int(row[3]))
			tempitemfeat[row[0]].append(int(row[4]))

		tempaddition = [0,0,0,0]
		for t in tempitemfeat:
			tempaddition = [sum(x) for x in zip(tempaddition, tempitemfeat[t])]

		for i in timelist:
			if i in tempitemfeat:
				itemfeat.setdefault(item, []).extend(tempitemfeat[i])
			else:
				itemfeat.setdefault(item, []).extend([0,0,0,0])

		itemfeat[item].extend(tempaddition)
	print "end item feature"
	cursor.close()
	cnx.close()
	'''
	return itemfeat

def genUserItemCombineFeature(train, user, item):
	useritemlist = list()
	timelist = list()
	start_date = dt.date(2014, 11, 18)
	end_date = dt.date(2014, 12, 19)
	if not train:
		end_date = dt.date(2014, 12, 18)
	while start_date < end_date:
		timelist.append(start_date)
		start_date = start_date + dt.timedelta(days=1)
	
	cnx = mysql.connector.connect(user='root', password='1234', database='tianchi')
	cursor = cnx.cursor()

	if train:
		query = ("SELECT pytime,sum(behbrowse),sum(behcollect),sum(behcart),sum(behbuy) FROM trainuser_new "
         "WHERE userid=%s and itemid=%s group by pytime")
	else:
		query = ("SELECT pytime,sum(behbrowse),sum(behcollect),sum(behcart),sum(behbuy) FROM trainuser_new "
         "WHERE userid=%s and itemid=%s and pytime<>'2014-12-18' group by pytime")

	tempitemfeat = dict()
	data = (user, item)
	cursor.execute(query , data)
	for row in cursor:
		tempitemfeat[row[0]] = list()
		tempitemfeat[row[0]].append(int(row[1]))
		tempitemfeat[row[0]].append(int(row[2]))
		tempitemfeat[row[0]].append(int(row[3]))
		tempitemfeat[row[0]].append(int(row[4]))

	for i in timelist:
		if i in tempitemfeat:
			useritemlist.extend(tempitemfeat[i])
		else:
			useritemlist.extend([0,0,0,0])
	cursor.close()
	cnx.close()
	return useritemlist

def computePrecisionAndRecall(fpname, frname):
	predicttionset = dict()
	referenceset = dict()
	with open(fpname, "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			key = row[0]+"_"+row[1]
			if int(row[2]) == 1 and key not in predicttionset:
				predicttionset[key] = 1

	predinterectref = 0.0
	with open(frname, "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			key = row[0]+"_"+row[1]
			if int(row[2]) == 1 and key not in referenceset:
				referenceset[key] = 1
				if key in predicttionset:
					predinterectref += 1

	prednum = float(len(predicttionset))
	refnum = float(len(referenceset))

	print "prediction set num: "+str(prednum)
	print "reference set num: "+str(refnum)
	print "interact num: "+str(predinterectref)
	precision = predinterectref / prednum
	recall = predinterectref / refnum
	f1 = (2*precision*recall) / (precision+recall)

	print "precision is: " + str(precision)
	print "recall is: " + str(recall)
	print "f1 is: " + str(f1)


def trainModel(train=True):
	userfeat = genUserFeature(train)
	itemfeat = genItemFeature(train)

	print "begin gen train data"
	filename = ""
	if train:
		filename = "train_data.csv"
	else:
		filename = "offline_train_data.csv"
	X_train = list()
	y_train = list()
	with open(filename, "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			posorneg = int(row[2])
			trainlist = list()
			trainlist.extend(userfeat[row[0]])
			trainlist.extend(itemfeat[row[1]])
			useritemfeat = genUserItemCombineFeature(train,row[0],row[1])
			trainlist.extend(useritemfeat)
			X_train.append(trainlist)
			if posorneg == 4:
				y_train.append(1)
			else:
				y_train.append(0)
	print "end gen train data"
	
	print "begin train model"
	clf = LogisticRegression()
	#clf = RandomForestClassifier(max_depth=10)
	clf = clf.fit(X_train, y_train)
	print "end train model"

	fwname = ""
	frname = ""
	if train:
		#online no need to offlien test
		fwname = "online_predict_data.txt"
		frname = "online_tobepredicted_data.csv"
	else:
		#offline test
		fwname = "offline_predict_data.txt"
		frname = "offline_test_data.csv"

	print "begin predict"
	with open(fwname, "w") as fw:
		with open(frname, "r") as f:
			for row in f.readlines():
				row = row.strip().split(',')
				if row[1] not in itemfeat:
					continue
				testlist = list()
				testlist.extend(userfeat[row[0]])
				testlist.extend(itemfeat[row[1]])
				useritemfeat = genUserItemCombineFeature(train,row[0],row[1])
				testlist.extend(useritemfeat)
				pred_label = clf.predict(testlist)[0]
				'''
				pred_pos = clf.predict_proba(testlist)[:,1]
				pred_label = 1
				if pred_pos < 0.7:
					pred_label = 0
				'''
				fw.write(row[0]+","+row[1]+","+str(pred_label)+"\n")
	print "end predict"

	if not train:
		computePrecisionAndRecall("offline_predict_data.txt", "offline_test_data.csv")

def predictDataToSubmitData():
	with open("tianchi_mobile_recommendation_predict.csv", "w") as fw:
		fw.write("user_id,item_id\n")
		with open("online_predict_data.txt", "r") as f:
			for row in f.readlines():
				line = row.strip().split(',')
				if int(line[2]) == 1:
					fw.write(line[0]+","+line[1]+"\n")

if __name__ == "__main__":
	train = False
	trainModel(train)
	if train:
		predictDataToSubmitData()
	#computePrecisionAndRecall("offline_predict_data.txt", "offline_test_data.csv")
	#createItemFeatureToFile(False)
	#genUserItemCombineFeature(False,'87847652','3')
