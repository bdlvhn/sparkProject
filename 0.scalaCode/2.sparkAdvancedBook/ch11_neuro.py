# 파이스파크와 썬더로 신경 영상 데이터 분석하기
# 11.3 썬더로 데이 터읽어 들이기
import thunder as td
data = td.images.fromtif('/user/ds/neuro/fish', engine=sc)

print data
print type(data.values)
print data.values._rdd
print data.values._rdd.first()
print data.shape

import matplotlib.pyplot as plt
img = data.first()
plt.imshow(img[:, : ,0], interpolation='nearest',
	aspect='equal', cmap='gray')

subsampled = data.subsample((1, 5, 5))
plt.imshow(subsampled.first()[1][:, : ,0], interpolation='nearest',
	aspect='equal', cmap='gray')
print subsampled.shape

series = data.toseries()
print series.shape
print series.index
print series.count()
print series.values._rdd.takeSample(False, 1)[0]
print series.max().values

stddev = series.map(lambda s: s.std())
print stddev.values._rdd.take(3)
print stddev.shape

repacked = stddev.toarray()
plt.imshow(repacked[:,:,0], interpolation='nearest',
	cmap='gray', aspect='equal')
print type(repacked)
print repacked.shape

plt.plot(series.center().sample(50).toarray().T)
series.map(lambda x: x.argmin())

# 11.4 썬더로 신경 세포 유형 분류하기

images = td.images.frombinary(
    '/user/ds/neuro/fish-long', order='F', engine=sc)
series = images.toseries()

normalized = series.normalize(method='mean')
stddevs = (normalized
    .map(lambda s: s.std())
    .sample(1000))
plt.hist(stddevs.values, bins=20)

plt.plot(
    normalized
        .filter(lambda s: s.std() >= 0.1)
        .sample(50)
        .values.T)

from pyspark.mllib.clustering import KMeans
ks = [5, 10, 15, 20, 30, 50, 100, 200]
models = []
for k in ks:
    models.append(KMeans.train(normalized.values._rdd.values(), k))

def model_error_1(model):
    def series_error(series):
        cluster_id = model.predict(series)
        center = model.centers[cluster_id]
        diff = center - series
        return diff.dot(diff) ** 0.5

    return (normalized
        .map(series_error)
        .toarray()
        .sum())

def model_error_2(model):
    return model.computeCost(normalized.values._rdd.values())

errors_1 = np.asarray(map(model_error_1, models))
errors_2 = np.asarray(map(model_error_2, models))
plt.plot(
    ks, errors_1 / errors_1.sum(), 'k-o',
    ks, errors_2 / errors_2.sum(), 'b:v')

model20 = models[3]
plt.plot(np.asarray(model20.centers).T)

from matplotlib.colors import ListedColormap
cmap_cat = ListedColormap(sns.color_palette("hls", 10), name='from_list')
by_cluster = normalized.map(lambda s: model20.predict(s)).toarray()
plt.imshow(by_cluster[:, :, 0], interpolation='nearest',
    aspect='equal', cmap='gray')