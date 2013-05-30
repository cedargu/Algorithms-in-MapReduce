import MapReduce
import sys

"""
Creates an inverted index. Given a set of documents, an inverted index is a dicitonary where each word is associated with a list of the document identifieres in which that word appears.

To run, use: python inverted_index.py books.json
"""

mr = MapReduce.MapReduce()

# The input is a 2 element list: [document_id, text] 
# document_id: docuemnt identifier formatted as a string
# text: text of the doument formatted as a string
def mapper(record):

	doc_ID = record[0]
	value = record[1]

	words = value.split()

	for word in words:
		mr.emit_intermediate(word, doc_ID)

# The output is a (word, document ID list) tuple where word is a String and
# document ID list is a list of Strings
def reducer(word, doc_ID):

	id_list = []

	for i in doc_ID:
		if i not in id_list:
			id_list.append(i)

	mr.emit((word, id_list))

if __name__ == '__main__':

	inputdata = open(sys.argv[1])

	mr.execute(inputdata, mapper, reducer)
