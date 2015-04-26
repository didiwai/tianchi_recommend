# -*- coding: utf-8 -*-

import datetime
import mysql.connector
import datetime
import math


'''
#add category
def genFinalFeatureToFile(end_date, frname, fwname):
	behlist = [1,2,3,4]
	timelist = list()
	#end_date = datetime.date(2014, 12, 18)
	time_to_subtract = [0,1,2,3,4,5,6, 14, 30]
	for t in time_to_subtract:
		temp_date = end_date - datetime.timedelta(days=t)
		timelist.append(temp_date)

	itemcategory = dict()
	with open("tianchi_mobile_recommend_train_item.csv", "r") as f:
		next(f)
		for row in f:
			row = row.strip().split(',')
			itemcategory[row[0]] = row[2]

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
			if number % 100 == 0:
				print number
			number += 1
			node = node.split('_')
			user = node[0];item = node[1]; cate = itemcategory[item]
			feat = list()

			#生成用户特征
			query = ("SELECT count(id),count(distinct itemid) FROM trainuser_new "
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
			query = ("SELECT count(id),count(distinct userid) FROM trainuser_new "
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
			
			#生成category特征
			query = ("SELECT count(id),count(distinct userid) FROM trainuser_new "
		         "WHERE itemcategory=%s and behavior=%s and pytime between %s and %s")
			for num, t in enumerate(timelist):
				#itemnum表示品牌购买（等）数量
				#diffitemnum表示购买（等）该品牌的人数	
				itemnum=[0,0,0,0];diffitemnum=[0,0,0,0]
				for b in behlist:
					data = (cate, b, t, end_date)
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
			query = ("SELECT count(id) FROM trainuser_new "
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
			
			#生成用户-cate特征
			query = ("SELECT count(id) FROM trainuser_new "
		         "WHERE userid=%s and itemcategory=%s and behavior=%s and pytime between %s and %s ")
			useritemaction = dict()
			for num, t in enumerate(timelist):
				#itemnum表示用户购买（等）品牌数量
				itemnum=[0,0,0,0]
				for b in behlist:
					data = (user, cate, b, t, end_date)
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
'''


def genFinalFeatureToFile(end_date, frname, fwname):
	behlist = [1,2,3,4]
	timelist = list()
	#end_date = datetime.date(2014, 12, 18)
	time_to_subtract = [0,1,2,3,4,5,6, 13,28]
	special_date = datetime.date(2014, 12, 12)
	for t in time_to_subtract:
		temp_date = end_date - datetime.timedelta(days=t)
		if temp_date == special_date:
			continue
		else:
			timelist.append(temp_date)
	if len(timelist) == 6:
		temp_date = end_date - datetime.timedelta(days=7)
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
			query = ("SELECT count(id),count(distinct itemid),count(distinct pytime) FROM trainuser_new "
		         "WHERE userid=%s and behavior=%s and pytime between %s and %s")
			query_1 = ("SELECT count(distinct pytime) FROM trainuser_new "
		         "WHERE userid=%s and pytime between %s and %s")
			query_2 = ("SELECT min(pytime),max(pytime) FROM trainuser_new "
		         "WHERE userid=%s and behavior=%s and pytime<%s")
			useraction = dict()
			useractiveday_dict = dict()
			for num, t in enumerate(timelist):
				#itemnum表示用户购买、浏览、购物车和收藏的总数
				#diifitemnum表示用户购买、浏览等不同品牌总数
				itemnum=[0,0,0,0];diffitemnum=[0,0,0,0]
				#daynum表示用户购买、浏览、购物车和收藏任意品牌的天数
				daynum = [0,0,0,0]
				for b in behlist:
					data = (user, b, t, end_date)
					cursor.execute(query, data)
					rs = cursor.fetchall()
					itemnum[b-1] = rs[0][0]; diffitemnum[b-1] = rs[0][1]
					daynum[b-1] = rs[0][2]
				#最近1/3/5/７/14/30天
				feat.extend(daynum)
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
				#用户购买的天数/用户活跃的天数
				user_active_day = 0
				data = (user, t, end_date)
				cursor.execute(query_1, data)
				rs = cursor.fetchall()
				user_active_day = rs[0][0]
				useractiveday_dict[num] = user_active_day
				if user_active_day == 0:
					feat.append(0)
				else:
					ub_ua = float(daynum[3]) / user_active_day
					feat.append(ub_ua)
			#用户最近一次/第一次交互任意品牌的时间
			start_date = datetime.date(2015,1,1)
			if end_date == datetime.date(2014,12,16):
				start_date = datetime.date(2015,1,1)
			elif end_date == datetime.date(2014,12,17):
				start_date = datetime.date(2015,1,2)
			else:
				start_date = datetime.date(2015,1,3)
			data = (user, 1, end_date)
			cursor.execute(query_2, data)
			rs = cursor.fetchall()
			user_first_interact = rs[0][0];user_last_interact = rs[0][1]
			if user_first_interact == None:
				feat.append(0)
				if user_last_interact == None:
					feat.append(0)
				else:
					user_last_feat = math.log((start_date - user_last_interact).days)
					feat.append(user_last_feat)
			elif user_last_interact == None:
				if user_first_interact == None:
					feat.append(0)
				else:
					user_first_feat = math.log((start_date - user_first_interact).days)
					feat.append(user_first_feat)
				feat.append(0)
			else:
				user_first_feat = math.log((start_date - user_first_interact).days)
				user_last_feat = math.log((start_date - user_last_interact).days)
				feat.append(user_first_feat)
				feat.append(user_last_feat)
			#用户最近一次/第一次购买任意品牌的时间
			data = (user, 4, end_date)
			cursor.execute(query_2, data)
			rs = cursor.fetchall()
			user_first_buy = rs[0][0];user_last_buy = rs[0][1]
			if user_first_buy == None:
				feat.append(0)
				if user_last_buy == None:
					feat.append(0)
				else:
					user_last_feat = math.log((start_date - user_last_buy).days)
					feat.append(user_last_feat)
			elif user_last_buy == None:
				if user_first_buy == None:
					feat.append(0)
				else:
					user_first_feat = math.log((start_date - user_first_buy).days)
					feat.append(user_first_feat)
				feat.append(0)
			else:
				user_first_feat = math.log((start_date - user_first_buy).days)
				user_last_feat = math.log((start_date - user_last_buy).days)
				feat.append(user_first_feat)
				feat.append(user_last_feat)

			#生成品牌特征
			query = ("SELECT count(id),count(distinct userid) FROM trainuser_new "
		         "WHERE itemid=%s and behavior=%s and pytime between %s and %s")
			query_1 = ("SELECT count(id) FROM trainuser_new "
		         "WHERE itemid=%s and behavior=%s and pytime between %s and %s group by userid")
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
				#重复购买该物品的用户数
				user_repeat_buy_num = 0
				data = (item, 4, t, end_date)
				cursor.execute(query_1, data)
				rs = cursor.fetchall()
				for row in rs:
					if row[0] > 1:
						user_repeat_buy_num += 1
				feat.append(user_repeat_buy_num)
				#重复购买该物品的用户数／总数
				if diffitemnum[3] == 0:
					feat.append(0)
				else:
					temp = float(user_repeat_buy_num) / diffitemnum[3]
					feat.append(temp)
				#活跃度(周期内有5次及以上点击的⽤用户数/总⽤用户数)
				user_third_click_num = 0
				data = (item, 1, t, end_date)
				cursor.execute(query_1, data)
				rs = cursor.fetchall()
				for row in rs:
					if row[0] > 5:
						user_third_click_num += 1
				if diffitemnum[0] == 0:
					feat.append(0)
				else:
					temp = float(user_third_click_num) / diffitemnum[0]
					feat.append(temp)	
				#⼈均点击数、⼈均购买量、人均收藏量、人均购物⻋车量			
				for tempindex in range(4):
					if diffitemnum[tempindex] == 0:
						feat.append(0)
					else:
						temp = float(itemnum[tempindex]) / diffitemnum[tempindex]
						feat.append(temp)

			#生成用户-品牌特征
			query = ("SELECT count(id),count(distinct pytime) FROM trainuser_new "
		         "WHERE userid=%s and itemid=%s and behavior=%s and pytime between %s and %s ")
			useritemaction = dict()
			useritem_interact_day_dict = dict()
			for num, t in enumerate(timelist):
				#itemnum表示用户购买（等）品牌数量
				#user_interact_item_day表示用户访问（等）品牌天数
				itemnum=[0,0,0,0]; user_interact_item_day = [0,0,0,0]
				for b in behlist:
					data = (user, item, b, t, end_date)
					cursor.execute(query, data)
					rs = cursor.fetchall()
					itemnum[b-1] = rs[0][0]; user_interact_item_day[b-1] = rs[0][1]
				useritemaction[num] = itemnum
				useritem_interact_day_dict[num] = user_interact_item_day
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
				#用户访问该品牌的天数/用户总活跃天数
				if useractiveday_dict[num] == 0:
					feat.extend([0,0,0,0])
				else:
					feat.append(float(useritem_interact_day_dict[num][0])/useractiveday_dict[num])
					feat.append(float(useritem_interact_day_dict[num][1])/useractiveday_dict[num])
					feat.append(float(useritem_interact_day_dict[num][2])/useractiveday_dict[num])
					feat.append(float(useritem_interact_day_dict[num][3])/useractiveday_dict[num])
			#⽤户第一次交互/购买当前品牌和最后一次交互当/购买前品牌相隔多少天
			query_1 = ("SELECT min(pytime),max(pytime) FROM trainuser_new "
		         "WHERE userid=%s and itemid=%s and behavior=%s and pytime<%s")
			data = (user, item, 1, end_date)
			cursor.execute(query_1, data)
			rs = cursor.fetchall()
			user_first_click_item = rs[0][0];user_last_click_item = rs[0][1]
			
			if user_first_click_item == None:
				feat.append(0)
				if user_last_click_item == None:
					feat.append(0)
				else:
					temp_day = math.log((start_date - user_last_click_item).days)
					feat.append(temp_day)
				feat.append(0)
			else:
				temp_day = math.log((start_date - user_first_click_item).days)
				feat.append(temp_day)
				if user_last_click_item == None:
					feat.append(0)
					feat.append(0)
				else:
					temp_day = math.log((start_date - user_last_click_item).days)
					feat.append(temp_day)
					temp_day = (user_last_click_item - user_first_click_item).days + 1
					feat.append(temp_day)

			data = (user, item, 4, end_date)
			cursor.execute(query_1, data)
			rs = cursor.fetchall()
			user_first_buy_item = rs[0][0];user_last_buy_item = rs[0][1]

			if user_first_buy_item == None:
				feat.append(0)
				if user_last_buy_item == None:
					feat.append(0)
				else:
					temp_day = math.log((start_date - user_last_buy_item).days)
					feat.append(temp_day)
				feat.append(0)
			else:
				temp_day = math.log((start_date - user_first_buy_item).days)
				feat.append(temp_day)
				if user_last_buy_item == None:
					feat.append(0)
					feat.append(0)
				else:
					temp_day = math.log((start_date - user_last_buy_item).days)
					feat.append(temp_day)
					temp_day = (user_last_buy_item - user_first_buy_item).days + 1
					feat.append(temp_day)

			#用户最后⼀次交互当前品牌距离⽤户最后⼀次交互任意品牌相隔多少天
			#if user_last_interact == None or user_last_click_item == None:
			#	feat.append(0)
			#else:
			#	temp = (user_last_interact - user_last_click_item).days
			#	feat.append(temp)

			fw.write(user+","+item+","+",".join([str(i) for i in feat])+"\n")
	print "end gen user item feature"
	cursor.close()
	cnx.close()



if __name__ == "__main__":
	'''
	#create offline train data feature
	end_date = datetime.date(2014, 12, 16)
	genFinalFeatureToFile(end_date, "offline_train_data.csv", "traindata_feature_offline_add.csv")	
	'''
	#create offline test data feature
	end_date = datetime.date(2014, 12, 17)
	genFinalFeatureToFile(end_date, "offline_test_data.csv", "testdata_feature_offline_add.csv")
	
	#create online train data feature
	end_date = datetime.date(2014, 12, 17)
	genFinalFeatureToFile(end_date, "online_train_data.csv", "traindata_feature_online_add.csv")
	
	#create online test data feature
	end_date = datetime.date(2014, 12, 18)
	genFinalFeatureToFile(end_date, "online_test_data.csv", "testdata_feature_online_add.csv")
	


