# -*- coding: utf-8 -*-

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

def genUserFeatureToFile():
	userlist = list()
	with open("user.txt", "r") as f:
		for row in f.readlines():
			row = row.strip()
			userlist.append(row)

	behlist = [1,2,3,4]
	timelist = list()
	end_date = datetime.date(2014, 12, 17)
	time_to_subtract = [1,3,7,15,30]
	for t in time_to_subtract:
		temp_date = end_date - datetime.timedelta(days=t)
		timelist.append(temp_date)

	query = ("SELECT count(id),count(distinct itemid) FROM trainuser_sample "
         "WHERE userid=%s and behavior=%s and pytime between %s and %s")
	print "begin create user feature"
	cnx = mysql.connector.connect(user='root', password='1234', database='tianchi')
	cursor = cnx.cursor()
	with open("userfeature_offline.csv", "w") as fw:
		for num, user in enumerate(userlist):
			if num % 1000 == 0:
				print num
			feat = list()
			for t in timelist:
				#itemnum表示用户购买、浏览、购物车和收藏的总数
				#diifitemnum表示用户购买、浏览等不同品牌总数
				itemnum=[0,0,0,0];diffitemnum=[0,0,0,0]
				for b in behlist:
					data = (user, b, t, end_date)
					cursor.execute(query, data)
					rs = cursor.fetchall()
					itemnum[b-1] = rs[0][0]; diffitemnum[b-1] = rs[0][1]
				feat.extend(itemnum)
				feat.extend(diffitemnum)
				#用户购买数/用户浏览数
				if itemnum[0] == 0:
					feat.append(0)
				else:
					b_c = float(itemnum[3]) / itemnum[0]
					feat.append(b_c)
				#用户购买的品牌数/用户浏览的品牌数
				if diffitemnum[0] == 0:
					feat.append(0)
				else:
					bb_cb = float(diffitemnum[3]) / diffitemnum[0]
					feat.append(bb_cb)
			fw.write(user+","+",".join([str(i) for i in feat])+"\n")
	cursor.close()
	cnx.close()

def genItemFeatureToFile():
	itemlist = dict()
	with open("offline_train_data.csv", "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			if row[1] not in itemlist:
				itemlist[row[1]] = 1
	with open("offline_test_data.csv", "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			if row[1] not in itemlist:
				itemlist[row[1]] = 1

	behlist = [1,2,3,4]
	timelist = list()
	end_date = datetime.date(2014, 12, 17)
	time_to_subtract = [1,3,7,15,30]
	for t in time_to_subtract:
		temp_date = end_date - datetime.timedelta(days=t)
		timelist.append(temp_date)

	query = ("SELECT count(id),count(distinct userid) FROM trainuser_sample "
         "WHERE itemid=%s and behavior=%s and pytime between %s and %s")
	cnx = mysql.connector.connect(user='root', password='1234', database='tianchi')
	cursor = cnx.cursor()
	with open("itemfeature_offline.csv", "w") as fw:
		num = 0
		for item in itemlist:
			if num % 1000 == 0:
				print num
			num += 1
			feat = list()
			for t in timelist:
				#itemnum表示品牌购买（等）数量
				#diffitemnum表示购买（等）该品牌的人数
				itemnum=[0,0,0,0];diffitemnum=[0,0,0,0]
				for b in behlist:
					data = (item, b, t, end_date)
					cursor.execute(query, data)
					rs = cursor.fetchall()
					itemnum[b-1] = rs[0][0]; diffitemnum[b-1] = rs[0][1]
				feat.extend(itemnum)
				feat.extend(diffitemnum)
				#销量（购物车、收藏）/浏览量
				if itemnum[0] == 0:
					feat.extend([0,0,0])
				else:
					for c in [1,2,3]:
						temp = float(itemnum[c]) / itemnum[0]
						feat.append(temp)
				#购买人数（购物车人数、收藏人数）/浏览人数
				if diffitemnum[0] == 0:
					feat.extend([0,0,0])
				else:
					for c in [1,2,3]:
						temp = float(diffitemnum[3]) / diffitemnum[0]
						feat.append(temp)
			fw.write(item+","+",".join([str(i) for i in feat])+"\n")

	cursor.close()
	cnx.close()

def genFinalFeatureToFile():
	behlist = [1,2,3,4]
	timelist = list()
	end_date = datetime.date(2014, 12, 17)
	time_to_subtract = [1,3,7,15,30]
	for t in time_to_subtract:
		temp_date = end_date - datetime.timedelta(days=t)
		timelist.append(temp_date)

	cnx = mysql.connector.connect(user='root', password='1234', database='tianchi')
	cursor = cnx.cursor()

	print "begin gen user and item data"
	useritempair = dict()
	with open("offline_train_data.csv", "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			node = row[0]+"_"+row[1]
			if node not in useritempair:
				useritempair[node] = 1
	with open("offline_test_data.csv", "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			node = row[0]+"_"+row[1]
			if node not in useritempair:
				useritempair[node] = 1
	print "end gen user and item data"
	print "begin gen user item feature"
	with open("allfeature_offline.csv", "w") as fw:
		number = 0
		for node in useritempair:
			if number % 1000 == 0:
				print number
			number += 1
			node = node.split('_')
			user = node[0];item = node[1]
			feat = list()
			#生成用户特征
			query = ("SELECT count(id),count(distinct itemid) FROM trainuser_sample "
		         "WHERE userid=%s and behavior=%s and pytime between %s and %s")
			useraction = dict()
			for num, t in enumerate(timelist):
				#itemnum表示用户购买、浏览、购物车和收藏的总数
				#diifitemnum表示用户购买、浏览等不同品牌总数
				itemnum=[0,0,0,0];diffitemnum=[0,0,0,0]
				for b in behlist:
					data = (user, b, t, end_date)
					cursor.execute(query, data)
					rs = cursor.fetchall()
					itemnum[b-1] = rs[0][0]; diffitemnum[b-1] = rs[0][1]
				useraction[num] = itemnum
				feat.extend(itemnum)
				feat.extend(diffitemnum)
				#用户购买数/用户浏览数
				if itemnum[0] == 0:
					feat.append(0)
				else:
					b_c = float(itemnum[3]) / itemnum[0]
					feat.append(b_c)
				#用户购买的品牌数/用户浏览的品牌数
				if diffitemnum[0] == 0:
					feat.append(0)
				else:
					bb_cb = float(diffitemnum[3]) / diffitemnum[0]
					feat.append(bb_cb)
			
			#生成品牌特征
			query = ("SELECT count(id),count(distinct userid) FROM trainuser_sample "
		         "WHERE itemid=%s and behavior=%s and pytime between %s and %s")
			for num, t in enumerate(timelist):
				#itemnum表示品牌购买（等）数量
				#diffitemnum表示购买（等）该品牌的人数	
				itemnum=[0,0,0,0];diffitemnum=[0,0,0,0]
				for b in behlist:
					data = (item, b, t, end_date)
					cursor.execute(query, data)
					rs = cursor.fetchall()
					itemnum[b-1] = rs[0][0]; diffitemnum[b-1] = rs[0][1]
				feat.extend(itemnum)
				feat.extend(diffitemnum)
				#销量（购物车、收藏）/浏览量
				if itemnum[0] == 0:
					feat.extend([0,0,0])
				else:
					for c in [1,2,3]:
						temp = float(itemnum[c]) / itemnum[0]
						feat.append(temp)
				#购买人数（购物车人数、收藏人数）/浏览人数
				if diffitemnum[0] == 0:
					feat.extend([0,0,0])
				else:
					for c in [1,2,3]:
						temp = float(diffitemnum[3]) / diffitemnum[0]
						feat.append(temp)
			
			#生成用户-品牌特征
			query = ("SELECT count(id) FROM trainuser_sample "
		         "WHERE userid=%s and itemid=%s and behavior=%s and pytime between %s and %s ")
			useritemaction = dict()
			for num, t in enumerate(timelist):
				#itemnum表示用户购买（等）品牌数量
				itemnum=[0,0,0,0]
				for b in behlist:
					data = (user, item, b, t, end_date)
					cursor.execute(query, data)
					rs = cursor.fetchall()
					itemnum[b-1] = rs[0][0]
				useritemaction[num] = itemnum
			#截止到最后一天用户购买（添加购物车、浏览、收藏）该物品的数量
			templist = useritemaction[4]
			feat.extend(templist)
			for num, t in enumerate(timelist):
				#用户购买该品牌的次数/用户总购买次数
				if useraction[num][3] == 0:
					feat.append(0)
				else:
					feat.append(float(useritemaction[num][3])/useraction[num][3])
				#用户点击、购买、添加购物车、收藏该物品的次数/用户总访问次数
				if useraction[num][0] == 0:
					feat.extend([0,0,0,0])
				else:
					feat.append(float(useritemaction[num][0])/useraction[num][0])
					feat.append(float(useritemaction[num][1])/useraction[num][0])
					feat.append(float(useritemaction[num][2])/useraction[num][0])
					feat.append(float(useritemaction[num][3])/useraction[num][0])
			fw.write(user+","+item+",".join([str(i) for i in feat])+"\n")
	print "end gen user item feature"
	cursor.close()
	cnx.close()

if __name__ == "__main__":
	genUserFeatureToFile()

