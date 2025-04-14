#!/usr/bin/env python3

# import sys

# # Initialize variables to track the current word and its associated document IDs
# current_word = None
# document_ids = set()

# # Read input line by line from stdin
# for line in sys.stdin:
#     # Parse the input (word, document_id)
#     try:
#         word, document_id = line.strip().split('\t')
#     except ValueError:
#         # Skip malformed lines
#         continue
    
#     # If the word changes, emit the previous word's results
#     if current_word and current_word != word:
#         print(f"{current_word}\t{','.join(sorted(document_ids))}")
#         document_ids.clear()
    
#     # Update the current word and add the document ID to the set
#     current_word = word
#     document_ids.add(document_id)

# # Emit the last word's results
# if current_word:
#     print(f"{current_word}\t{','.join(sorted(document_ids))}")

#!/usr/bin/env python3

import sys
from cassandra.cluster import Cluster


def import_cassandra(session, term, document_frequency, document_list):
    session.execute(
        """
        INSERT INTO vocabulary (term, document_frequency, document_list)
        VALUES (%s, %s, %s)
        """,
        (term, document_frequency, document_list)
    )


def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('search_engine')

    current_word = None
    current_doc_list = {}

    for line in sys.stdin:
        line = line.strip()
        word, doc_id, count = line.split('\t', 2)
        count = int(count)

        if current_word == word:
            current_doc_list[doc_id] = current_doc_list.get(doc_id, 0) + count
        else:
            if current_word:
                doc_freq = len(current_doc_list)
                import_cassandra(session, current_word, doc_freq, current_doc_list)

            current_word = word
            current_doc_list = {doc_id: count}

    if current_word:
        doc_freq = len(current_doc_list)
        import_cassandra(session, current_word, doc_freq, current_doc_list)


if __name__ == "__main__":
    main()
