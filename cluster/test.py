from sklearn import cluster, datasets

def main():
	iris = datasets.load_iris()
	X_iris = iris.data
	y_iris = iris.target

	k_means = cluster.KMeans(n_clusters=3)
	k_means.fit(X_iris) 
	cluster.KMeans(copy_x=True, init='k-means++')
	print(k_means.labels_[::10])
	print(y_iris[::10])

	

if __name__ == "__main__":
	main()