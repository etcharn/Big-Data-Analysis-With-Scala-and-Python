from mrjob.compat import jobconf_from_env
from mrjob.job import MRJob
import math
import re

class TermWeightsComputation(MRJob):


    def mapper_init(self):
        self.Dict = {}

    def mapper(self, _, line):

        date, termString = line.strip().split(",")
        # extract year
        year = date[0:4]
        # extract term
        terms = termString.strip().split(" ")
        for term in terms:
            if len(term):
                key = term + "," + year
                self.Dict[key] = self.Dict.get(key, 0) + 1
        self.Dict = dict(sorted(self.Dict.items()))

    def mapper_final(self):
        for key in self.Dict:
            term, year = key.split(",")
            freq = self.Dict[key]
            yield key, self.Dict[key]
            yield term + ",*", 1
            
    def reducer_init(self):
        self.marginal = 0 
        self.year_count = int(jobconf_from_env('myjob.settings.years', 3))
        
    def reducer(self, key, val):
        term, year = key.split(",")
        freq = list(val)
        if year == "*":
            self.marginal = sum(freq)
        else:
            count = sum(freq)
            TF = count
            IDF = math.log10(self.year_count/self.marginal)
            Weight = TF * IDF
            weight_str = year + "," + str(Weight)
            yield term, weight_str
        
    SORT_VALUES = True

    JOBCONF = {
        'map.output.key.field.separator': ',',
        'mapred.reduce.tasks': 2,
        'mapreduce.partition.keypartitioner.options': '-k1,2',
        'partition': 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner',
        'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
        'mapreduce.partition.keycomparator.options':'-k1,1'
    }

if __name__ == '__main__':
    TermWeightsComputation.run()


