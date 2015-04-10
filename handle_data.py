def handledata():
	useritem = dict()
	with open("tianchi_mobile_recommend_train_user.csv", "r") as f:
		next(f)
		for line in f:
			line = line.strip().split(',')
			beh = int(line[2])
			ndoe = line[0]+"_"+line[1]
			


if __name__ == "__main__":