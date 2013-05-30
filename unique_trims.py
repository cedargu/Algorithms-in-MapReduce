import MapReduce
import sys

"""
Given a set of key-value pairs where each key is a seqeunce id and each value is a string of nuclieotides, e.g., GCTTCCFAAATGCTCGAA...

Removes the last 10 characters from each string of nucleotides, then removes
any duplicates generated.

To run, use: python unique_trims.py dna.json
"""

mr = MapReduce.MapReduce()

# The input is a 2 element list [sequence id, nucleotides]
# sequence id: unique identifier formatted as a string
# nucleotides: Sequences of nucleotides formatted as a string
def mapper(record):

	sequenceID = record[0]
	nucleotide = record[1]

	trimednucleotide = nucleotide[:-10]

	mr.emit_intermediate(trimednucleotide,0)

# The ouput is a unique trimmed nucleotide strings
def reducer(key, list_of_values):

	mr.emit(key)

if __name__ == '__main__':
	inputdata = open(sys.argv[1])
	mr.execute(inputdata, mapper, reducer)
