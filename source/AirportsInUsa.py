import sys
from pyspark import SparkContext, SparkConf
import shutil
import re

class Utils():

    COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')

def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[2])

if __name__ == "__main__":
    conf = SparkConf().setAppName("airports").setMaster("local[4]")
    sc = SparkContext(conf = conf)

    airports = sc.textFile("res/airports.text")
    airportsInUSA = airports.filter(lambda line : Utils.COMMA_DELIMITER.split(line)[3] == "\"United States\"")

    # Remove folder if exist
    shutil.rmtree('out/airports_in_usa.text', ignore_errors=True)

    airportsNameAndCityNames = airportsInUSA.map(splitComma)
    airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text")
