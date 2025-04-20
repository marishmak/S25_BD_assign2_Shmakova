#!/usr/bin/env python3
import sys
import re
from collections import defaultdict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def tokenize(text):
    """Tokenize text into words"""
    try:
        words = re.findall(r'\w+', text.lower())
        return words
    except Exception as e:
        logger.error(f"Tokenization error: {str(e)}")
        return []

def main():
    for line in sys.stdin:
        try:
            # Split the line into doc_id, title, and text
            parts = line.strip().split('\t')
            if len(parts) < 3:
                logger.warning(f"Skipping malformed line: {line[:100]}...")
                continue
                
            doc_id, title, text = parts[0], parts[1], parts[2]
            
            # Tokenize the text
            words = tokenize(text)
            
            # Count term frequencies
            term_freq = defaultdict(int)
            for word in words:
                term_freq[word] += 1
            
            # Emit term, doc_id and frequency
            for term, freq in term_freq.items():
                print(f"{term}\t{doc_id}\t{freq}")
                
        except Exception as e:
            logger.error(f"Error processing line: {str(e)}")
            continue

if __name__ == "__main__":
    main()