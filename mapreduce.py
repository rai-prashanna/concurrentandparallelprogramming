import os
os.environ['JAVA_HOME']='/home/prra9532/programs/jdk1.8.0_191'


from pyspark import SparkContext

sc = SparkContext("local", "Simple App")

## MapReduce Framework
def initialise(sc, inputFile, prepare):
    """Open a file and apply the prepare function to each line"""
    input = sc.textFile(inputFile)
    return input.map(prepare)

def finalise(data, outputFile):
    """store data in given file"""
    data.saveAsTextFile(outputFile)

class Mapper:
    def __init__(self):
        self.out = []

    def emit(self, key, val):
        self.out.append((key, val))

    def result(self):
        return self.out

class Reducer:
    def __init__(self):
        self.out = []

    def emit(self, key, val):
        self.out.append((key, val))

    def result(self):
        return self.out

def transform(input, mapper, reducer):
    """map reduce framework"""
    return input.flatMap(lambda kd: mapper().map(kd[0], kd[1]).result()) \
                .groupByKey() \
                .flatMap(lambda kd: reducer().reduce(kd[0], kd[1]).result())
### End of MapReduce Framework


## Complete Worked Example

class WCMapper(Mapper):
    def __init__(self):
        super().__init__()

    def map(self, key, data):
        for x in data.split(" "):
            self.emit(x, 1)
        return self

class WCReducer(Reducer):
    def __init__(self):
        super().__init__()

    def reduce(self, key, datalist):
        self.emit(key, sum(datalist))
        return self

# wordcount
def wordcount(sc, inputFile, outputFile):
    rdd = initialise(sc, inputFile, lambda line: ("NoKey", line))
    result = transform(rdd, WCMapper, WCReducer)    # passing class NOT object (which would be WCMapper() etc)
    finalise(result, outputFile)

wordcount(sc, "text.txt", "count.out")     # will not work if count.out already exists
