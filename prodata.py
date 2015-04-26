import numpy as np
from sklearn.preprocessing import StandardScaler

def analysisDataFeature(fname1, fname2):
	feat1 = dict()
	with open(fname1, "r") as f:
		for row in f:
			row = row.strip().split(',')
			feat = [float(i) for i in row[2:]]
			for j in range(len(row[2:])):
				feat1.setdefault(j, []).append(feat[j])
	with open(fname2, "w") as fw:
		for key in feat1:
			#fw.write("feature "+str(key)+"\n")
			feat = feat1[key]
			minnum = min(feat); maxnum = max(feat); avgnum = np.mean(feat)
			#fw.write("minnum: "+str(minnum)+"\t"+"maxnum: "+str(maxnum)+"\t"+"avgnum: "+str(avgnum)+"\n")			
			fw.write(str(avgnum)+"\n")

def processFeatureData(fname1,fname2):
	feat1 = dict()
	with open(fname1, "r") as f:
		for row in f:
			row = row.strip().split(',')
			feat = [float(i) for i in row[2:]]
			for j in range(len(row[2:])):
				feat1.setdefault(j, []).append(feat[j])
	col = len(feat1)
	newfeat = dict()
	scaler = StandardScaler()
	for key in feat1:
		newfeat[key] = scaler.fit_transform(feat1[key])
	with open(fname2,"w") as fw:
		with open(fname1,"r") as f:
			number = 0
			for row in f:
				row = row.strip().split(',')
				templist = list()
				for t in range(col):
					templist.append(newfeat[t][number])
				fw.write(row[0]+","+row[1]+","+",".join([str(nf) for nf in templist])+"\n")
				number += 1

def proOriData(filename1, filename2):
	with open(filename2,"w") as fw:
		with open(filename1,"r") as f:
			for row in f:
				line = row.strip().split(',')
				feat = [float(n) for n in line[2:]]
				tempnum = feat[4]
				if tempnum < 500 :
					fw.write(row)

				



if __name__ == "__main__":
	proOriData("traindata_feature_offline_add.csv","traindata_feature_offline_add_1.csv")
	proOriData("testdata_feature_offline_add.csv","testdata_feature_offline_add_1.csv")

	#analysisDataFeature("data/traindata_feature_online.csv", "feature_analysis.txt")
	processFeatureData("traindata_feature_offline_add_1.csv","traindata_feature_offline_add_post.csv")
	processFeatureData("testdata_feature_offline_add_1.csv","testdata_feature_offline_add_post.csv")
