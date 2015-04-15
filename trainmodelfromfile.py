# -*- coding: utf-8 -*- 

import mysql.connector
import datetime
from sklearn import cross_validation
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
import time


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
			if int(row[2]) == 4 and key not in referenceset:
				referenceset[key] = 1
				if key in predicttionset:
					predinterectref += 1

	prednum = float(len(predicttionset))
	refnum = float(len(referenceset))

	print "prediction set num: "+str(prednum)
	print "reference set num: "+str(refnum)
	print "interact num: "+str(predinterectref)
	precision = predinterectref / prednum * 100
	recall = predinterectref / refnum * 100
	print "precision is: " + str(precision) + "%"
	print "recall is: " + str(recall) + "%"
	f1 = (2*precision*recall) / (precision+recall)
	print "f1 is: " + str(f1) + "%"


def predictDataToSubmitData():
	with open("tianchi_mobile_recommendation_predict.csv", "w") as fw:
		fw.write("user_id,item_id\n")
		with open("online_predict_data.txt", "r") as f:
			for row in f.readlines():
				line = row.strip().split(',')
				if int(line[2]) == 1:
					fw.write(line[0]+","+line[1]+"\n")


def trainModelOnline():
	X_train = list()
	y_train = list()

	print "begin gen train data"
	useritemlabel = dict()
	with open("online_train_data.csv", "r") as f:
		for row in f:
			row = row.strip().split(',')
			node = row[0]+"_"+row[1]
			if int(row[2]) == 4:
				useritemlabel[node] = 1
			else:
				useritemlabel[node] = 0
	trainstarttime = time.time()
	number = 0
	with open("traindata_feature_online.csv", "r") as f:
		for row in f:
			if number % 1000 == 0:
				print number
			number += 1
			row = row.strip().split(',')
			feat = [float(n) for n in row[2:]]
			X_train.append(feat)
			node = row[0]+"_"+row[1]
			y_train.append(useritemlabel[node])

	trainendtime = time.time()
	print "end gen train data"
	
	print "begin train model"
	trainmodelstarttime = time.time()
	#clf = LogisticRegression()
	clf = GradientBoostingClassifier(learning_rate=0.01,n_estimators=500,max_depth=5)
	clf = clf.fit(X_train, y_train)
	trainmodelendtime = time.time()
	print "end train model"


	fwname = "online_predict_data.txt"
	print "begin predict"
	teststarttime = time.time()
	with open(fwname, "w") as fw:
		number = 0
		with open("testdata_feature_online.csv", "r") as f:
			for row in f:
				if number % 1000 == 0:
					print number
				number += 1
				row = row.strip().split(',')
				feat = [float(n) for n in row[2:]]
				pred_label = clf.predict(feat)[0]
				fw.write(row[0]+","+row[1]+","+str(pred_label)+"\n")
	testendtime = time.time()
	print "end predict"
	print "gen train data time is: ",trainendtime - trainstarttime
	print "train model time is: ", trainmodelendtime - trainmodelstarttime
	print "test time is: ", testendtime - teststarttime
	predictDataToSubmitData()


def trainModelOffline():
	X_train = list()
	y_train = list()

	print "begin gen train data"
	useritemlabel = dict()
	with open("offline_train_data.csv", "r") as f:
		for row in f:
			row = row.strip().split(',')
			node = row[0]+"_"+row[1]
			if int(row[2]) == 4:
				useritemlabel[node] = 1
			else:
				useritemlabel[node] = 0
	trainstarttime = time.time()
	number = 0
	with open("traindata_feature_offline.csv", "r") as f:
		for row in f:
			if number % 1000 == 0:
				print number
			number += 1
			row = row.strip().split(',')
			feat = [float(n) for n in row[2:]]
			X_train.append(feat)
			node = row[0]+"_"+row[1]
			y_train.append(useritemlabel[node])

	trainendtime = time.time()
	print "end gen train data"
	
	print "begin train model"
	trainmodelstarttime = time.time()
	#clf = LogisticRegression(penalty='l1')
	#clf = RandomForestClassifier(n_estimators=100)
	clf = GradientBoostingClassifier(learning_rate=0.01,n_estimators=500,max_depth=5)
	clf = clf.fit(X_train, y_train)
	trainmodelendtime = time.time()
	print "end train model"


	fwname = "offline_predict_data.txt"
	print "begin predict"
	teststarttime = time.time()
	with open(fwname, "w") as fw:
		number = 0
		with open("testdata_feature_offline.csv", "r") as f:
			for row in f:
				if number % 1000 == 0:
					print number
				number += 1
				row = row.strip().split(',')
				feat = [float(n) for n in row[2:]]
				pred_label = clf.predict(feat)[0]
				fw.write(row[0]+","+row[1]+","+str(pred_label)+"\n")
	testendtime = time.time()
	print "end predict"
	print "gen train data time is: ",trainendtime - trainstarttime
	print "train model time is: ", trainmodelendtime - trainmodelstarttime
	print "test time is: ", testendtime - teststarttime
	computePrecisionAndRecall("offline_predict_data.txt", "offline_test_data_in_p.csv")


if __name__ == "__main__":
	#trainModelOffline()
	trainModelOnline()
