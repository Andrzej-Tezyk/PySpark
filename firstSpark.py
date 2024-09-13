from pyspark import SparkConf, SparkContext
import collections

# for all spark scripts -> create conf and context
# SparkConf() is empty -> parameters are set using methods
# local - use only one machine
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# when configuration created -> pass to SparkContext
sc = SparkContext(conf = conf)

# breakes data into lines
lines = sc.textFile("ml-100k/u.data")
# lambda -> map function
# splits each line on " " -> takes third field -> puts into a new dataset
# old RDD remains untouched -> new is created
ratings = lines.map(lambda x: x.split()[2])
# count how many times each ration occured
result = ratings.countByValue()
print(result)

# sort results to show 1 first, 2 secont, ...
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
