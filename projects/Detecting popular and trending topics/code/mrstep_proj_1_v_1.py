from mrjob.compat import jobconf_from_env
from mrjob.job import MRJob
from mrjob.step import MRStep
import math
import re


class TermWeightsComputation(MRJob):

    def mapper_1(self, _, line):

        date, termString = line.strip().split(",")
        # extract year
        year = date[0:4]
        # extract term
        terms = termString.strip().split(" ")
        for term in terms:
            if len(term):
                yield term + "," + year, 1
                
    def reducer_1(self, key, val):
        yield key, sum(val)

    def reducer_2(self, key, val):
        term, year = key.split(",")
        freq_list = list(val)
        freq = sum(freq_list)
        yield term, (year, freq)
        
    def reducer_3(self, key, val):
        [term, yearFreq_dict] = key, dict(val)
        year_countAll = int(jobconf_from_env('myjob.settings.years', 3))
        yearWeightString = ""
        sortedYearFreq = sorted(yearFreq_dict)
        for year in sortedYearFreq:
            # number of key = # of years having term
            year_occur = len(yearFreq_dict)
            # use year as a key to get frequency
            TF = yearFreq_dict[year]
            IDF = math.log10(year_countAll / year_occur)
            weight = TF * IDF
            yearWeightString = yearWeightString + year + ',' + str(weight)
            # if i is last index, we don't print
            if year != sortedYearFreq[-1]:
                yearWeightString = yearWeightString + ';'
            else:
                break

        yield term, yearWeightString
            
    def steps(self):
        return [
            MRStep(mapper=self.mapper_1,
                   reducer=self.reducer_1),
            MRStep(reducer=self.reducer_2),
            MRStep(reducer=self.reducer_3)
        ]
        
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
            