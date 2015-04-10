import datetime
import mysql.connector
'''
def createItemFeatureToFile():
	itempair = dict()
	filename = "offline_train_data.csv"
	topredictfilename = "offline_test_data.csv"
	with open(filename, "r") as fui:
		for row in fui.readlines():
			row= row.strip().split(',')
			if row[1] not in itempair:
				itempair[row[1]] = 1
	#gen predict datas
	with open(topredictfilename, "r") as fui:
		for row in fui.readlines():
			row = row.strip().split(',')
			if row[1] not in itempair:
				itempair[row[1]] = 1
	#item feature like the user feature, everyday has 4 feature and also have a
	#addition feature
	print "begin item fearure"
	timelist = list()
	start_date = dt.date(2014, 11, 18)
	end_date = dt.date(2014, 12, 18)
	while start_date < end_date:
		timelist.append(start_date)
		start_date = start_date + dt.timedelta(days=1)
	cnx = mysql.connector.connect(user='root', password='1234', database='tianchi')
	cursor = cnx.cursor()
	query = ("SELECT pytime,sum(behbrowse),sum(behcollect),sum(behcart),sum(behbuy) FROM trainuser_new "
     "WHERE itemid='%s' and pytime<>'2014-12-18' group by pytime")
	with open("itemfeature.txt" ,"w") as fw:
		itemfeat = list()
		num = 0
		for item in itempair:
			if num % 1000 == 0:
				print num
			num += 1
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
					itemfeat.extend(tempitemfeat[i])
				else:
					itemfeat.extend([0,0,0,0])
			itemfeat.extend(tempaddition)
			fw.write(item+","+','.join([str(f) for f in itemfeat])+"\n")
	print "end item feature"
	cursor.close()
	cnx.close()

	itemfeat = dict()
	itemlist = list()
	with open("item.txt", "r") as f:
		for row in f.readlines():
			row = row.strip()
			itemlist.append(row)
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
'''
def genUserFeature(timelist, user, cursor):
	userfeat = list()
	behlist = [1,2,3,4]
	query = ("SELECT count(id),count(distinct itemid) FROM trainuser_sample "
         "WHERE userid=%s and behavior=%s and pytime between %s and %s")
	for t in timelist:
		itemnum=[0,0,0,0];diffitemnum=[0,0,0,0]
		for b in behlist:
			data = (user, b,t, end_date)
			cursor.execute(query, data)
			rs = cursor.fetchall()
			itemnum[b-1] = rs[0][0]; diffitemnum[b-1] = rs[0][1]
		userfeat.extend(itemnum)
		userfeat.extend(diffitemnum)
		#buy/click
		b_c = itemnum[3] / itemnum[0]
		userfeat.append(b_c)
		#buy difference brand/click all brand
		bb_cb = diffitemnum[3] / diffitemnum[0]
		userfeat.append(bb_cb)

	return userfeat

def genItemFeature(timelist, item, cursor):
	itemfeat = list()
	behlist = [1,2,3,4]
	query = ("SELECT count(id),count(distinct userid) FROM trainuser_sample "
         "WHERE itemid=%s and behavior=%s and pytime between %s and %s")
	for t in timelist:
		itemnum=[0,0,0,0];diffitemnum=[0,0,0,0]
		for b in behlist:
			data = (item, b, t, end_date)
			cursor.execute(query, data)
			rs = cursor.fetchall()
			itemnum[b-1] = rs[0][0]; diffitemnum[b-1] = rs[0][1]
		itemfeat.extend(itemnum)
		itemfeat.extend(diffitemnum)
		#buy/click
		#buy difference brand/click all brand
		for c in [1,2,3]:
			temp = itemnum[c] / itemnum[0]
			itemfeat.append(temp)
		for c in [1,2,3]:
			temp = diffitemnum[3] / diffitemnum[0]
			itemfeat.append(temp)

	return itemfeat

def genUserItemFeature(timelist, user, item, cursor):
	useritemfeat = list()
	behlist = [1,2,3,4]
	query = ("SELECT count(id) FROM trainuser_sample "
         "WHERE userid=%s and itemid=%s and behavior=%s and pytime between<%s")
	itemnum=[0,0,0,0]
	for b in behlist:
		data = (user, item, b, end_date)
		cursor.execute(query, data)
		rs = cursor.fetchall()
		itemnum[b-1] = rs[0][0]

	return useritemfeat

if __name__ == "__main__":
	timelist = list()
	time_to_subtract = [1,3,7,15,30]
	for t in time_to_subtract:
		temp_date = end_date - datetime.timedelta(days=t)
		timelist.append(temp_date)
	cnx = mysql.connector.connect(user='root', password='1234', database='tianchi')
	cursor = cnx.cursor()
	cursor.close()
	cnx.close()
	end_date = datetime.date(2014, 12, 17)
	genUserFeature(end_date, userdict)