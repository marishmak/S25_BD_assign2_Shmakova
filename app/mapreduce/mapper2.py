import sys

for line in sys.stdin:
    try:
        doc_id, title, text = line.strip().split('\t')
        for term in text.split():
            print(f"{term}\t{doc_id}\t1")
    except Exception as e:
        print(f"Error processing line: {line}", file=sys.stderr)