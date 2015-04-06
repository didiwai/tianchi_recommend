import mysql.connector
import datetime as dt
from sklearn import cross_validation
from sklearn.linear_model import LogisticRegression

def csvToMysql():
	cnx = mysql.connector.connect(user='root', password='1234', database='tianchi')
	cursor = cnx.cursor()
	add_newline = ("INSERT INTO trainuser_new "
               "(userid, itemid, behbrowse, behcollect, behcart, behbuy, usergeohash, itemcategory, oritime, pytime, hour) "
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		next(f)
		for line in f:
			line = line.strip().split(',')
			beh = int(line[2])
			templist = [0,0,0,0]
			templist[beh-1] = 1
			data_newline = (line[0], line[1], templist[0], templist[1], templist[2], templist[3], line[3], line[4], line[5], line[5].split(" ")[0], line[5].split(" ")[1])
			cursor.execute(add_newline, data_newline)

	cnx.commit()
	cursor.close()
	cursor.close()

def createUserFeature():
	userlist = list()
	with open("user.txt", "r") as f:
		for row in f.readlines():
			row = row.strip()
			userlist.append(row)
	
	
	useradditionfeat = dict()
	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		next(f)
		for line in f:
			line = line.strip().split(',')
			index = int(line[2]) - 1
			user = line[0]
			useradditionfeat.setdefault(user, [0,0,0,0])[index] += 1
	print "end addtion feature"
	
	timelist = list()
	start_date = dt.date(2014, 11, 18)
	end_date = dt.date(2014, 12, 19)
	while start_date < end_date:
		timelist.append(start_date)
		start_date = start_date + dt.timedelta(days=1)

	print "connect to mysql"
	cnx = mysql.connector.connect(user='root', password='1234', database='tianchi')
	cursor = cnx.cursor()

	query = ("SELECT pytime,sum(behbrowse),sum(behcollect),sum(behcart),sum(behbuy) FROM trainuser_new "
         "WHERE userid='%s' group by pytime")

	with open("user_fearure.txt", "w") as fw:
		for num, user in enumerate(userlist):
			print num, user
			userfeat = dict()
			cursor.execute(query % user)

			linenum = 1
			for row in cursor:
				userfeat[row[0]] = str(row[1])+","+str(row[2])+","+str(row[3])+","+str(row[4])
				linenum+=1
			
			tempwrite = ""
			for i in timelist:
				if i in userfeat:
					tempwrite += "," + userfeat[i]
				else:
					tempwrite += "," + "0,0,0,0"

			tempwrite = user+tempwrite+","+",".join(str(n) for n in useradditionfeat[user])+"\n"
			#print tempwrite
			fw.write(tempwrite)
	'''
	query = ("SELECT behaviortype, datetime FROM trainuser "
         "WHERE userid='%s' ")

	with open("user_fearure.txt", "w") as fw:
		for num, user in enumerate(userlist):
			print num, user
			userfeat = dict()
			cursor.execute(query % user)

			linenum = 1
			for (behaviortype, datetime) in cursor:
				userfeat.setdefault(datetime, [0,0,0,0])[behaviortype-1] += 1
				linenum+=1
			tempwrite = ""
			for i in timelist:
				tempwrite += "," + ",".join([str(n) for n in userfeat.setdefault(i ,[0,0,0,0])])
			
			print tempwrite
			tempwrite = user+","+tempwrite+","+",".join(str(n) for n in useradditionfeat[user])+"\n"
			fw.write(tempwrite)
	'''
	cursor.close()
	cnx.close()

def combineUserItemFeature():
	print "begin item fearure"
	itemfeat = dict()
	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		next(f)
		for line in f:
			line = line.strip().split(',')
			index = int(line[2]) - 1
			item = line[1]
			itemfeat.setdefault(item, [0,0,0,0])[index] += 1
	print "end item feature"

	print "begin user feature"
	userfeat = dict()
	with open("user_fearure.txt", "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			userfeat[row[0]] = [int(n) for n in row[1:]]
	print "end user feature"

	preditemlist = dict()
	with open("tianchi_mobile_recommend_train_item.csv", "r") as f:
		next(f)
		for line in f:
			line = line.strip().split(',')
			preditemlist[line[0]] = 0

	X_train = list()
	y_train = list()
	predictuseritem = dict()
	print "begin gen train data"
	with open("train_data.csv", "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			posorneg = int(row[2])
			templist = list()
			templist.extend(userfeat[row[0]])
			templist.extend(itemfeat[row[1]])
			X_train.append(templist)
			if posorneg == 4:
				y_train.append(1)
			else:
				y_train.append(0)
			node = row[0]+","+row[1]
			if row[1] in preditemlist and node not in predictuseritem:
				predictuseritem[node] = templist
	print "end gen train data"

	print "begin train lr"
	lr = LogisticRegression()
	lr = lr.fit(X_train, y_train)
	print "end train lr"

	print "begin predict"
	'''
	with open("predict_prob.txt", "w") as fw:
		for key in predictuseritem:
			prob_pos = lr.predict_proba(predictuseritem[key])[:, 1]
			fw.write(key+","+str(prob_pos)+"\n")
	'''
	with open("predict_label.txt", "w") as fw:
		for key in predictuseritem:
			prob_pos = lr.predict(predictuseritem[key])[0]
			fw.write(key+","+str(prob_pos)+"\n")	
	print "end predict"
'''
def createTrainData():
	with open("offline_trian_data.csv", "w") as fwtrain:
		with open("offline_test_data.csv", "w") as fwtest:
			with open("train_data.csv", "r") as f:
				for row in f.readlines():
					line = row.strip().split(',')
					datatime = line[5].split(' ')[0]
					if datatime == '2014-12-18':
						if int(line[2]) == 4:
							fwtest.write(line[0]+","+line[1]+",1\n")
						else:
							fwtest.write(line[0]+","+line[1]+",0\n")
					else:
						fwtrain.write(row)
'''

def createTrainData():
	with open("offline_trian_data.csv", "w") as fwtrain:
		with open("train_data.csv", "r") as f:
			for row in f.readlines():
				line = row.strip().split(',')
				datatime = line[5].split(' ')[0]
				if datatime == '2014-12-18':
					pass
				else:
					fwtrain.write(row)

def createTestData():
	with open("offline_test_data.csv", "w") as fwtest:
		with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
			for row in f.readlines():
				line = row.strip().split(',')
				datatime = line[5].split(' ')[0]
				if datatime == '2014-12-18':
					if int(line[2]) == 4:
						fwtest.write(line[0]+","+line[1]+",1\n")
					else:
						fwtest.write(line[0]+","+line[1]+",0\n")

def createToBePredictedData():
	preditemlist = dict()
	with open("tianchi_mobile_recommend_train_item.csv", "r") as f:
		next(f)
		for line in f:
			line = line.strip().split(',')
			preditemlist[line[0]] = 0

	with open("online_tobepredicted_data.csv", "w") as fw:
		predictuseritem = dict()
		with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
			for row in f.readlines():
				row = row.strip().split(',')
				node = row[0]+","+row[1]
				if row[1] in preditemlist and node not in predictuseritem:
					predictuseritem[node] = 0
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
	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		next(f)
		for line in f:
			row = line.strip().split(',')
			pydate = row[5].split(' ')[0]
			if pydate == '2014-12-12':
				continue
			if int(row[2]) == 4:
				poslist.append(line)
			else:
				neglist.append(line)
	print "end create pos and neg list"
	X_train, newneglist = cross_validation.train_test_split(neglist, test_size=0.14, random_state=0)
	with open("train_data.csv", "w") as fw:
		for num, n in enumerate(newneglist):
			if num % 10 == 0 and len(poslist) != 0:
				temp = poslist.pop()
				fw.write(temp)
			fw.write(n)



if __name__ == "__main__":
	csvToMysql()
