from pyspark import SparkContext
import os
if __name__ == "__main__":
    print(os.getcwd())
    sc = SparkContext(master='local[*]', appName="pythonwordcount")
    lines = sc.textFile('file:%s/words.txt' %(os.getcwd()))
    result = lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).collect()

    for (word, count) in result:
        print("%s, %d"%(word, count))

    sc.stop()