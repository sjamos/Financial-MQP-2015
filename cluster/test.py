from sklearn import cluster, datasets

def main1():
	iris = datasets.load_iris()
	X_iris = iris.data
	y_iris = iris.target
	print str(iris.data)
	print "YAY"

	k_means = cluster.KMeans(n_clusters=3)
	k_means.fit(X_iris) 
	cluster.KMeans(copy_x=True, init='k-means++')
	print(k_means.labels_[::10])
	print(y_iris[::10])

def main2():
	from sklearn.decomposition import PCA
	from sklearn.cluster import KMeans
	from sklearn.datasets import load_iris
	import pylab as pl

	iris = load_iris()
	print type(iris.data)
	print type(iris.data[0])
	print type(iris.data[0][0])
	print iris.data
	pca = PCA(n_components=2).fit(iris.data)
	pca_2d = pca.transform(iris.data)
	pl.figure('Reference Plot')
	pl.scatter(pca_2d[:, 0], pca_2d[:, 1], c=iris.target)
	
	kmeans = KMeans(n_clusters=3, random_state=111)
	kmeans.fit(iris.data)
	pl.figure('K-means with 3 clusters')
	pl.scatter(pca_2d[:, 0], pca_2d[:, 1], c=kmeans.labels_)
	pl.show()

if __name__ == "__main__":
	main2()