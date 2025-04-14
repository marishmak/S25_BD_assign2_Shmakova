#!/usr/bin/env python3

import sys

# Initialize variables to track the current word and its associated document IDs
current_word = None
document_ids = set()

# Read input line by line from stdin
for line in sys.stdin:
    # Parse the input (word, document_id)
    try:
        word, document_id = line.strip().split('\t')
    except ValueError:
        # Skip malformed lines
        continue
    
    # If the word changes, emit the previous word's results
    if current_word and current_word != word:
        print(f"{current_word}\t{','.join(sorted(document_ids))}")
        document_ids.clear()
    
    # Update the current word and add the document ID to the set
    current_word = word
    document_ids.add(document_id)

# Emit the last word's results
if current_word:
    print(f"{current_word}\t{','.join(sorted(document_ids))}")