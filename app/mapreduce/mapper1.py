#!/usr/bin/env python3

import sys
import os
import re

# # Define a function to tokenize text into words
# def tokenize(text):
#     # Use regex to extract alphanumeric words
#     return re.findall(r'\b\w+\b', text.lower())

# # Get the document ID (filename) from the environment variable
# document_id = os.environ.get('mapreduce_map_input_file', 'unknown')

# # Read input line by line from stdin
# for line in sys.stdin:
#     # Tokenize the line into words
#     words = tokenize(line.strip())
    
#     # Emit each word with the document ID as the value
#     for word in words:
#         print(f"{word}\t{document_id}")



import sys
import csv
import re


def tokenize(text):
    return re.findall(r'\w+', text.lower())


def main():
    reader = csv.DictReader(sys.stdin)
    for row in reader:
        doc_id = row['id']
        content = row['content']
        words = tokenize(content)
        for word in words:
            print(f"{word}\t{doc_id}\t1")


if __name__ == "__main__":
    main()
