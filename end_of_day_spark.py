"""For End of Day Transaction
"""

import datetime
import pyspark


def main():
    sc = pyspark.SparkContext()
    # here we are outputting in the form of key value the date format(2018-12-03) and amount assuming for withdrawal amount will be in -ve
    rdd = sc.textFile('/data/transaction.csv')
    tranlist = rdd.map(lambda x: (x[0], x[2]))
    tranByDate = tranlist.reduceByKey(
        lambda a, b: a + b).sortBy(key=lambda x: x[0])
    tArray = tranByDate.collect()
    result = []
    balance = 0
    for tran in tArray:
        balance = tran[1] + balance
        result.append(tran[0], balance)

    sc.parallelize(result).saveAsTextFile('end_of_day_balance')


if __name__ == '__main__':
    main()
