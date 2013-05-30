import MapReduce
import sys

"""
Computes the number of friends each person has in a social network dataset consisting of key-value pairs where each key is a person and each value is a friend of that person.

To run, use: python friend_count.py friends.json
"""

mr = MapReduce.MapReduce()

# The input is a 2 element list: [personA, personB]
# personA: Name of a peson formatted as a string
# personB: Name of one of personA's friends formatted as a string
# This implies  that personB is a friend of personA, but does not imply that personA is a friend of personB
def mapper(record):

	personA = record[0]

	mr.emit_intermediate(personA, 1)

# The output is a (person, friend count) tuple. 
def reducer(key, list_of_values):

	total = 0

	for v in list_of_values:
		total += v

	mr.emit((key, total))

if __name__ == '__main__':

	inputdata = open(sys.argv[1])

	mr.execute(inputdata, mapper, reducer)
