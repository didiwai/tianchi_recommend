# -*- coding: utf-8 -*-

import datetime
import mysql.connector

def genUserFeatureToFile():
	userlist_dict = dict()
	with open("online_train_data.csv", "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			if row[0] not in userlist_dict:
				userlist_dict[row[0]] = 1
	with open("online_test_data.csv", "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			if row[0] not in userlist_dict:
				userlist_dict[row[0]] = 1
	userlist = list()
	for user in userlist_dict:
		userlist.append(user)

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
	with open("userfeature.csv", "w") as fw:
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
	with open("online_train_data.csv", "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			if row[0] not in itemlist:
				itemlist[row[0]] = 1
	with open("online_test_data.csv", "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			if row[0] not in itemlist:
				itemlist[row[0]] = 1

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

def genFinalFeatureToFile(end_date, frname, fwname):
	behlist = [1,2,3,4]
	timelist = list()
	#end_date = datetime.date(2014, 12, 18)
	time_to_subtract = [0, 2, 6, 14, 30]
	for t in time_to_subtract:
		temp_date = end_date - datetime.timedelta(days=t)
		timelist.append(temp_date)

	cnx = mysql.connector.connect(user='root', password='1234', database='tianchi')
	cursor = cnx.cursor()

	print "begin gen user and item data"
	useritempair = dict()
	numline = 0
	#with open("online_test_data.csv", "r") as f:
	with open(frname, "r") as f:
		for row in f.readlines():
			row = row.strip().split(',')
			node = row[0]+"_"+row[1]
			numline+=1
			if node not in useritempair:
				useritempair[node] = 1
	print numline,len(useritempair)
	print "end gen user and item data"
	print "begin gen user item feature"
	#with open("testdata_feature_online.csv", "w") as fw:
	with open(fwname, "w") as fw:
		number = 0
		for node in useritempair:
			if number % 1000 == 0:
				print number
			number += 1
			node = node.split('_')
			user = node[0];item = node[1]
			feat = list()
			#生成用户特征
			query = ("SELECT count(id),count(distinct itemid) FROM trainuser_all "
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
			query = ("SELECT count(id),count(distinct userid) FROM trainuser_all "
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
			query = ("SELECT count(id) FROM trainuser_all "
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
			fw.write(user+","+item+","+",".join([str(i) for i in feat])+"\n")
	print "end gen user item feature"
	cursor.close()
	cnx.close()

if __name__ == "__main__":
	'''
	#create online test data feature
	end_date = datetime.date(2014, 12, 18)
	genFinalFeatureToFile(end_date, "online_test_data.csv", "testdata_feature_online.csv")
	'''
	'''
	#create offline test data feature
	end_date = datetime.date(2014, 12, 17)
	genFinalFeatureToFile(end_date, "offline_test_data_in_p.csv", "testdata_feature_offline.csv")
	#create offline train data feature
	end_date = datetime.date(2014, 12, 16)
	genFinalFeatureToFile(end_date, "offline_train_data.csv", "traindata_feature_offline.csv")
	'''
	#create online train data feature
	end_date = datetime.date(2014, 12, 17)
	genFinalFeatureToFile(end_date, "online_train_data.csv", "traindata_feature_online.csv")
