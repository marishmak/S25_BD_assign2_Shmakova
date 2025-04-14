import sys

current_term = None
current_doc_id = None
term_frequency = 0

for line in sys.stdin:
    try:
        term, doc_id, count = line.strip().split('\t')
        count = int(count)

        if term != current_term or doc_id != current_doc_id:
            if current_term and current_doc_id:
                print(f"{current_term}\t{current_doc_id}\t{term_frequency}")
            current_term = term
            current_doc_id = doc_id
            term_frequency = 0

        term_frequency += count
    except Exception as e:
        print(f"Error processing line: {line}", file=sys.stderr)

if current_term and current_doc_id:
    print(f"{current_term}\t{current_doc_id}\t{term_frequency}")