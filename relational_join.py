import MapReduce
import sys
import pprint
import itertools

"""
Implements a relational join as a MapReduce query.

To run, use: python join.py records.json
"""

mr = MapReduce.MapReduce()

# The input is a database records formatted as lists of Strings.
def mapper(record):

	key = record[1]
	value = record

	mr.emit_intermediate(key,value)

# The output is a joined record. The result is a single list of length 27 that 
# contains the fields from the order record followed by fields from the line item 
# record. Each list element is a string.
def reducer(key, list_of_values):

	keydict = {}

	if key not in keydict:
		keydict[key] = list_of_values 
	elif key in keydict:
		keydict[key] += list_of_values

	for i in range(1,len(keydict[key])):
		mr.emit(keydict[key][0] + keydict[key][i])

if __name__ == '__main__':

	inputdata = open(sys.argv[1])

	mr.execute(inputdata, mapper, reducer)
