import mysql.connector
import datetime as dt
from sklearn import cross_validation
from sklearn.linear_model import LogisticRegression

def csvToMysql():
	cnx = mysql.connector.connect(user='root', password='1234', database='tianchi')
	cursor = cnx.cursor()
	add_newline = ("INSERT INTO trainuser_sample "
               "(userid, itemid, behavior, behbrowse, behcollect, behcart, behbuy, usergeohash, itemcategory, oritime, pytime, hour) "
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

	with open("train_data.csv", "r") as f:
		for line in f.readlines():
			line = line.strip().split(',')
			beh = int(line[2])
			templist = [0,0,0,0]
			templist[beh-1] = 1
			data_newline = (line[0], line[1], beh, templist[0], templist[1], templist[2], templist[3], line[3], line[4], line[5], line[5].split(" ")[0], line[5].split(" ")[1])
			cursor.execute(add_newline, data_newline)

	cnx.commit()
	cursor.close()
	cursor.close()

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
				d = line[5].split(' ')[0]
				if d == '2014-12-18':
					pass
				else:
					fwtrain.write(row)

def createTestData():
	predictItemList = dict()
	'''
	with open("tianchi_mobile_recommend_train_item.csv", "r") as f:
		next(f)
		for line in f:
			line = line.strip().split(',')
			item = line[0]
			if item not in predictItemList:
				predictItemList[item] = 1
	'''
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
			next(f)
			for row in f:
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
	'''
	predictItemList = dict()
	with open("tianchi_mobile_recommend_train_item.csv", "r") as f:
		next(f)
		for line in f:
			line = line.strip().split(',')
			item = line[0]
			if item not in predictItemList:
				predictItemList[item] = 1
	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		next(f)
		for line in f:
			row = line.strip().split(',')
			pydate = row[5].split(' ')[0]
			if pydate == '2014-12-12':
				continue
			if row[1] in predictItemList:
				if int(row[2]) == 4:
					poslist.append(line)
				else:
					neglist.append(line)
	'''
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
	createTrainData()
