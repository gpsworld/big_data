import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime


class MonthlySales(MRJob):
    def mapper(self,_,line):
        row = line.strip().split(",")
        orderdate = row[3]
        month = datetime.strptime(orderdate, "%Y-%m-%d").month
        amount = row[-1]
        yield month, amount
    
    def reducer(self,key,values):
        yield key, sum([float(amount) for amount in values])

if __name__ == "__main__":
    MonthlySales().run()
