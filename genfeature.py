def createItemFeatureToFile(train):
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
