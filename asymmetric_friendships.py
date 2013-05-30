import MapReduce
import sys
from collections import namedtuple

"""
Generates a list of all non-symmetric friend relationships.

To run, use: python asymmetric_friendships.py friends.json
"""

mr = MapReduce.MapReduce()

# The input is a 2 element list: [personA, personB]
# personA: Name of a person formatted as a string
# personB: Name of one of personA's friends formatted as a string
# This implies  that personB is a friend of personA, but does not imply that personA is a friend of personB
def mapper(record):

	sortedrecord = sorted(record)

	peepA = sortedrecord[0]
	peepB = sortedrecord[1]

	Friends = namedtuple("People",["PersonA","PersonB"])
	f = Friends(PersonA = peepA, PersonB = peepB)

	mr.emit_intermediate(f,1)

# The output is the (person, friend) and (friend, person) tuples for each
# asymmetric friendship
def reducer(key, list_of_values):

	total = 0
	for v in list_of_values:
		total += v

	if total < 2:
		mr.emit(tuple(key))
		mr.emit(tuple(sorted(tuple(key),reverse = True)))

if __name__ == '__main__':

	inputdata = open(sys.argv[1])

	mr.execute(inputdata, mapper, reducer)
