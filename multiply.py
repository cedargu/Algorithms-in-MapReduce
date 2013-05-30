import MapReduce
import sys
import pprint

"""
Computes the matrix multiplication: A x B, assuming there are two matrices A and B in a sparse matrix format, hwere each record is of the form i, j, value.

To run, use: python multiply.py matrix.json
"""

mr = MapReduce.MapReduce()

# The input is a matrix row records formatted as lists. Each list has the format
# [matrix,i,j,value] where matrix is a string and i,j, and value are integers.
def mapper(record):

	if record[0] == "a":
		for k in range(5):
			M = record[0]
			i = record[1]
			j = record[2]
			mij = record[3]
			key = (i,k)
			value = [M,j,mij]
			mr.emit_intermediate(key,value)

	elif record[0] == "b":
		for i in range(5):
			N = record[0]
			j = record[1]
			k = record[2]
			njk = record[3]
			key = (i,k)
			value = [N,j,njk]
			mr.emit_intermediate(key,value)

# The output is a matrix row records formatted as tuples. Each tuple is the format
# (i,j,value) where each element is an integer
def reducer(key, list_of_values):

	val = list_of_values

	a = {y:z for x,y,z in list_of_values if x == u'a'}
	b = {y:z for x,y,z in list_of_values if x == u'b'}

	total = 0

	for i in a:
		if i in b:
			total += a[i]*b[i]

	keyli = list(key)
	keyli.insert(2, total)
	tupkeyli = tuple(keyli)
	mr.emit(tupkeyli)

if __name__ == '__main__':

	inputdata = open(sys.argv[1])

 	mr.execute(inputdata, mapper, reducer)
