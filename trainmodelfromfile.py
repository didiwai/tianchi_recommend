# -*- coding: utf-8 -*- 

import mysql.connector
import datetime
from sklearn import cross_validation
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import AdaBoostClassifier
import time
from sklearn.metrics import mean_squared_error
import numpy as np


def computePrecisionAndRecall(fpname, frname):
	predicttionset = dict()
	referenceset = dict()
	with open(fpname, "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			key = row[0]+"_"+row[1]
			#if int(row[2]) == 1 and key not in predicttionset:
			if float(row[2]) > 0.5 and key not in predicttionset:
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
	return f1,prednum,predinterectref


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
	with open("traindata_feature_online_ori.csv", "r") as f:
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
	clf = LogisticRegression()
	#clf = GradientBoostingClassifier(learning_rate=0.01,n_estimators=1000,max_depth=8)
	clf = clf.fit(X_train, y_train)
	trainmodelendtime = time.time()
	print "end train model"


	fwname = "online_predict_data.txt"
	print "begin predict"
	teststarttime = time.time()
	with open(fwname, "w") as fw:
		number = 0
		with open("testdata_feature_online_ori.csv", "r") as f:
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
	#clf = RandomForestClassifier(n_estimators=10,max_depth=10)
	#clf = GradientBoostingClassifier(learning_rate=0.01,n_estimators=900,max_depth=1)
	clf = AdaBoostClassifier(learning_rate=0.1,n_estimators=100)
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


def ensembleMethod():
	predfilelist = ["offline_predict_lr.txt","offline_predict_gbdt.txt",
					"offline_predict_rf.txt","offline_predict_adb.txt"]
	'''
	classifiers = [LogisticRegression(penalty='l1'),
				   GradientBoostingClassifier(learning_rate=0.01,n_estimators=900,max_depth=1),
				   RandomForestClassifier(),
				   AdaBoostClassifier(learning_rate=0.1,n_estimators=100)]
	useritemlabel = dict()
	with open("offline_train_data.csv", "r") as f:
		for row in f:
			row = row.strip().split(',')
			node = row[0]+"_"+row[1]
			if int(row[2]) == 4:
				useritemlabel[node] = 1
			else:
				useritemlabel[node] = 0
	for predfilename, clf in zip(predfilelist, classifiers):
		print "begin gen train data model"
		X_train = list()
		y_train = list()
		with open("traindata_feature_offline.csv", "r") as f:
			for row in f:
				row = row.strip().split(',')
				feat = [float(n) for n in row[2:]]
				X_train.append(feat)
				node = row[0]+"_"+row[1]
				y_train.append(useritemlabel[node])
		
		clf = clf.fit(X_train, y_train)
		print "end gen train data model"
		print "begin predict model"
		number = 0
		with open(predfilename, "w") as fw:
			with open("testdata_feature_offline.csv", "r") as f:
				for row in f:
					if number % 1000 == 0:
						print number
					number += 1
					row = row.strip().split(',')
					feat = [float(n) for n in row[2:]]
					#pred_label = clf.predict(feat)[0]
					#fw.write(row[0]+","+row[1]+","+str(pred_label)+"\n")
					pred_pos = clf.predict_proba(feat)[:,1][0]
					fw.write(row[0]+","+row[1]+","+str(pred_pos)+"\n")
		print "end predict model"
	'''
	
	print "begin to ensemble method"
	predfilelist = ["offline_predict_1.txt","offline_predict_2.txt","offline_predict_3.txt","offline_predict_4.txt","offline_predict_5.txt","offline_predict_6.txt"]
	ensembledict = dict()
	for filename in predfilelist:
		with open(filename, "r") as f:
			for row in f:
				row = row.strip().split(',')
				node = row[0]+"_"+row[1]
				if node not in ensembledict:
					ensembledict[node] = list()
				ensembledict[node].append(float(row[2]))

	value1 = 0.1
	value2 = 0.5
	value3 = 0.4
	value4 = 0.0
	'''
	with open("try_value.txt","w") as f:
		for value1 in range(0, 100, 5):
			value1 = value1/100.0
			print "value1:",value1
			for value2 in range(0, 100, 5):
				value2=value2/100.0
				for value3 in range(0, 100, 5):
					value3=value3/100.0
					if value1+value2+value3>1:
						continue
					else:
						value4=1-value1-value2-value3
						with open("offline_predict_data.txt", "w") as fw:
							for key in ensembledict:
								#temp = ensembledict[key][0]*value1 + ensembledict[key][1]*value2
								temp = ensembledict[key][0]*value1+ensembledict[key][1]*value2+ensembledict[key][2]*value3+ensembledict[key][3]*value4
								pred_label = 0
								if temp > 0.5:
									pred_label = 1
								row = key.split('_')
								fw.write(row[0]+","+row[1]+","+str(pred_label)+"\n")
						f1,prednum,predinterectref = computePrecisionAndRecall("offline_predict_data.txt", "offline_test_data_in_p.csv")
						f.write(str(value1)+","+str(value2)+","+str(value3)+","+str(value4)+"\n")
						f.write("predict number is: "+str(prednum)+"\t"+"interact number is: "+str(predinterectref)+"\t"+"F1 is: "+str(f1)+"\n")
	'''
	tempnum = 0
	with open("offline_predict_data.txt", "w") as fw:
		for key in ensembledict:
			#temp = ensembledict[key][0]*value1+ensembledict[key][1]*value2+ensembledict[key][2]*value3+ensembledict[key][3]*value4
			temp = ensembledict[key][0]+ensembledict[key][1]+ensembledict[key][2]+ensembledict[key][3]+ensembledict[key][4]+ensembledict[key][5]
			temp = temp / 6.0
			#pred_label = 0
			#if temp > 0.5:
			#	pred_label = 1
			#	tempnum += 1
			row = key.split('_')
			fw.write(row[0]+","+row[1]+","+str(temp)+"\n")
	print tempnum
	computePrecisionAndRecall("offline_predict_data.txt", "offline_test_data_in_p.csv")
	

def ensembleMethodOnline():
	predfilelist = ["online_predict_lr.txt","online_predict_gbdt.txt",
					"online_predict_rf.txt","online_predict_adb.txt"]
	'''
	classifiers = [LogisticRegression(C=0.08),
				   GradientBoostingClassifier(learning_rate=0.01,n_estimators=500,max_depth=2),
				   RandomForestClassifier(n_estimators=500),
				   AdaBoostClassifier(learning_rate=0.1,n_estimators=500)]
	useritemlabel = dict()
	with open("offline_train_data.csv", "r") as f:
		for row in f:
			row = row.strip().split(',')
			node = row[0]+"_"+row[1]
			if int(row[2]) == 4:
				useritemlabel[node] = 1
			else:
				useritemlabel[node] = 0
	for predfilename, clf in zip(predfilelist, classifiers):
		print "begin gen train data model"
		X_train = list()
		y_train = list()
		with open("traindata_feature_offline.csv", "r") as f:
			for row in f:
				row = row.strip().split(',')
				feat = [float(n) for n in row[2:]]
				X_train.append(feat)
				node = row[0]+"_"+row[1]
				y_train.append(useritemlabel[node])
		
		clf = clf.fit(X_train, y_train)
		print "end gen train data model"
		print "begin predict model"
		number = 0
		posnum = 0
		with open(predfilename, "w") as fw:
			with open("testdata_feature_online_pre_by_lr.txt", "r") as f:
				for row in f:
					if number % 1000 == 0:
						print number
					number += 1
					row = row.strip().split(',')
					feat = [float(n) for n in row[2:]]
					pred_pos = clf.predict_proba(feat)[:,1][0]
					if pred_pos > 0.5:
						posnum += 1
					fw.write(row[0]+","+row[1]+","+str(pred_pos)+"\n")
		print posnum
		print "end predict model"
	'''
	print "begin to ensemble method"
	ensembledict = dict()
	for filename in predfilelist:
		with open(filename, "r") as f:
			for row in f:
				row = row.strip().split(',')
				node = row[0]+"_"+row[1]
				if node not in ensembledict:
					ensembledict[node] = list()
				ensembledict[node].append(float(row[2]))

	value1 = 0.1
	value2 = 0.5
	value3 = 0.4
	value4 = 0.0
	with open("online_predict_data.txt", "w") as fw:
		for key in ensembledict:
			temp = ensembledict[key][0]*value1+ensembledict[key][1]*value2+ensembledict[key][2]*value3+ensembledict[key][3]*value4
			pred_label = 0
			if temp > 0.5:
				pred_label = 1
			row = key.split('_')
			fw.write(row[0]+","+row[1]+","+str(pred_label)+"\n")
	predictDataToSubmitData()
	

def postFeature(frname,fwname,featlist):
	with open(fwname,"w") as fw:
		with open(frname,"r") as f:
			for row in f:
				row = row.strip().split(',')
				feat = [float(i) for i in row[2:]]
				newfeat = list()
				for f in range(len(feat)):
					if f not in featlist:
						newfeat.append(feat[f])
				#print len(feat),len(newfeat)
				fw.write(row[0]+","+row[1]+","+",".join([str(j) for j in newfeat])+"\n")


if __name__ == "__main__":
	#trainModelOffline()
	#trainModelOnline()
	#ensembleMethod()
	#ensembleMethodOnline()

	#featlist = [4,5,6,7,8,9,10,11,19,20,21,22,23,24,25,26,34,35,36,37,38,39,40,41,49,50,51,52,53,54,55,56,64,65,66,67,68,69,70,71,79,80,81,82,83,84,85,86,94,95,96,97,98,99,100,101]
	#featlist = [0,1,2,3,5,7,9,10,11,12,14,15,16,17,18,20,22,24,26,29,33,40,45,46,48,50,54,63,65,74,78,85,91,101,106,109,110,111,112,114,115,116,117,118,119,120,121,122,123,124,125,127,128,129,131,132,133,134,135,136,137,138,140,141,142,143,144,145,148,149,150,152,153,154,156,157,158,159,160,161,162,163,164,165,166,167,169,170,171,172,173,174,175,177,178,179,182,183,184,185,186,187,188,190,191,192,194,195,196,198,199,200,203,204,205,206,207,208,211,212,213,215,216,217,219,220,221,224,225,226,227,228,229,230,232,233,234,236,237,238,240,241,242,243,244,245,247,249,250,253,254,255,257,258,259,260,262,263,264,265,266,267,268,269,271,273,274,275,276,277,278,279,281,282,284,285,286,287,288,289,291,294,295,296,297,298,299,300,302,303,304,305,307,308,309,310,311,312,313,314,316,317,318,320,321,322]
	#featlist = [0,1,2,3,5,6,7,10,11,12,13,14,17,18,20,22,24,27,30,31,32,33,35,39,40,41,42,44,46,48,52,59,61,62,63,65,77,78,84,91,109,110,111,112,113,114,116,118,119,120,121,122,123,124,125,127,128,129,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,152,153,154,156,157,158,159,160,161,162,163,164,165,166,169,170,171,173,174,175,177,178,179,180,182,183,184,185,186,187,188,190,191,192,194,195,196,198,199,200,201,202,203,204,205,206,207,208,209,211,212,213,215,216,217,219,221,222,224,225,226,227,228,229,230,232,233,234,236,238,240,241,242,243,244,245,246,247,248,249,250,253,254,255,256,257,258,259,260,262,263,264,266,267,268,269,271,273,274,275,277,278,281,282,283,284,285,286,287,289,290,291,294,295,296,297,298,299,300,302,304,305,307,309,310,311,312,313,314,316,317,318,319,320,321,322]
	#postFeature("testdata_feature_offline_post.csv","testdata_feature_offline_post_fs.csv",featlist)
	#postFeature("traindata_feature_offline_post.csv","traindata_feature_offline_post_fs.csv",featlist)
	
	useritemlabel = dict()
	with open("offline_train_data_1.csv", "r") as f:
		for row in f:
			row = row.strip().split(',')
			node = row[0]+"_"+row[1]
			if int(row[2]) == 4:
				useritemlabel[node] = 1
			else:
				useritemlabel[node] = 0
	print "begin gen train data model"
	X_train = list()
	y_train = list()
	with open("traindata_feature_offline_1_post.csv", "r") as f:
		for row in f:
			row = row.strip().split(',')
			feat = [float(n) for n in row[2:]]
			X_train.append(feat)
			node = row[0]+"_"+row[1]
			y_train.append(useritemlabel[node])
	#clf = AdaBoostClassifier(learning_rate=0.5,n_estimators=800)
	#clf = LogisticRegression(C=0.08) C=0.05,penalty='l1'
	#clf = GradientBoostingClassifier(learning_rate=0.08,n_estimators=500,max_depth=2)
	#clf = RandomForestClassifier(n_estimators=100)
	clf = GradientBoostingClassifier(learning_rate=0.1,n_estimators=500,max_depth=10)
	#clf = LogisticRegression()
	#clf = RandomForestClassifier(n_estimators=400,max_depth=6)
	clf = clf.fit(X_train, y_train)
	print "end gen train data model"
	print "begin predict model"
	number = 0
	tempnum = 0
	tempnum_1 = 0
	with open("offline_predict_1.txt", "w") as fw:
		with open("testdata_feature_offline_post.csv", "r") as f:
			for row in f:
				if number % 1000 == 0:
					print number
				number += 1
				row = row.strip().split(',')
				feat = [float(n) for n in row[2:]]
				#pred_label = clf.predict(feat)[0]
				pred_pos = clf.predict_proba(feat)[:,1][0]
				if pred_pos> 0.5:
					tempnum += 1
				fw.write(row[0]+","+row[1]+","+str(pred_pos)+"\n")
	print "end predict model"
	print tempnum,tempnum_1
	computePrecisionAndRecall("offline_predict_1.txt", "offline_test_data_in_p.csv")
	
	'''
	feat_import = clf.feature_importances_
	print clf.feature_importances_
	with open("feature_import_3.txt","w") as fw:
		with open("feature_import_4.txt","w") as fw1:
			meannum = np.mean(feat_import)
			number = 0
			for f in range(len(feat_import)):
				tempw = str(number)
				if feat_import[f]>meannum:
					tempw = tempw+"+"+str(feat_import[f])+"\n"
				else:
					tempw = tempw+"-"+str(feat_import[f])+"\n"
					fw1.write(str(number)+"\n")
				fw.write(tempw)
				number += 1
	'''