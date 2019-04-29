"""For End of Day Transaction
"""
from mrjob.job import MRJob
import datetime


class End_of_Day(MRJob):

    # this class will define two additional methods: the mapper method goes here
    # here we are outputting in the form of key value the date format(2018-12-03) and amount assuming for withdrawal amount will be in -ve
    def mapper(self, _, rows):
        row = rows.split(',')
        date_tran_time = datetime.datetime.strptime(
            row[0], '%Y-%m-%d %H:%M:%S')
        amount = float(row[2])
        date_tran = str(date_tran_time.date())
        yield (date_tran, amount)

    # and the reducer method goes after this line here we get the date as key and amount as value
    def reducer(self, date, amount):
        yield(date, sum(amount))


if __name__ == '__main__':
    End_of_Day.run()
