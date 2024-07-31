import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep

class WordCount(MRJob):
    
    def mapper(self, _ , line):
        row = line.strip().split()
        for word in row:
            
            yield word,1
    def reducer(self,key,values):
        yield key,sum(values)

if __name__ == "__main__":
    WordCount().run()



