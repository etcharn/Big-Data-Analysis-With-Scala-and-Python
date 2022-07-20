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

    def mapper_2(self, key, val):
        term, year = key.split(",")
        freq = int(val)
        yield term, (year, freq)

    def reducer_2(self, key, val):
        [term, yearFreq_dict] = key, dict(val)
        year_countAll = int(jobconf_from_env('myjob.settings.years', 3))
        yearWeightString = ""
        for year in yearFreq_dict:
            # number of key = # of years having term
            year_occur = len(yearFreq_dict)
            # use year as a key to get frequency
            TF = yearFreq_dict[year]
            IDF = math.log10(year_countAll / year_occur)
            weight = TF * IDF
            yearWeightString = yearWeightString + \
                year + ',' + str(weight) + ";"

        yield term, yearWeightString[:-1]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_1,
                   reducer=self.reducer_1),
            MRStep(mapper=self.mapper_2,
                   reducer=self.reducer_2),
        ]

    SORT_VALUES = True

    JOBCONF = {

        'map.output.key.field.separator': ',',
        'mapred.reduce.tasks': 2,
        'mapreduce.partition.keypartitioner.options': '-k1,2',
        'partition': 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner',
        'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
        'mapreduce.partition.keycomparator.options': '-k1,1 -k2,2n'
    }


if __name__ == '__main__':
    TermWeightsComputation.run()
