from pyspark import SparkContext
if __name__ == "__main__":
    sc = SparkContext(master='local[*]', appName="pythonwordcount")
    lines = sc.textFile('file:/D:/code/day_ue/IdeaProjects/pythoncode/com/yuepengfei/spark/words.txt')
    result = lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).collect()

    for (word, count) in result:
        print("%s, %d"%(word, count))

    sc.stop()