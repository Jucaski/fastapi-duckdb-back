import pandas as pd
import chardet
from io import StringIO
import os
import unicodedata

def detect_encoding(file_path, sample_size=1000000):
    """Detect file encoding from a sample."""
    with open(file_path, 'rb') as f:
        sample = f.read(sample_size)
    result = chardet.detect(sample)
    return result['encoding'], result['confidence']

#def normalize(string_chunk):
    #if isinstance(string_chunk, str):
        #norm = unicodedata.normalize('NFD', string_chunk)
        #return ''.join([c for c in norm if not unicodedata.combining(c)])
    #return string_chunk

def clean_csv_in_chunks(first_chunk, input_path, output_path, chunk_size=100000):
    """Clean CSV in chunks to handle large files."""
    # First, detect encoding
    #encoding, confidence = detect_encoding(input_path)
    #print(f"Detected encoding: {encoding} with confidence: {confidence}")
    
    # Process in chunks
    chunks = pd.read_csv(input_path, chunksize=chunk_size, encoding='utf-8', encoding_errors='replace',
                        on_bad_lines='warn', dtype=str, engine='python', low_memory=True)

    # Write header to output file
    
    for i, chunk in enumerate(chunks):
        # Clean chunk data
        # 1. Remove control characters
        for col in chunk.columns:
            #print("Before: " + str(chunk[col]))
            #encode_result = chardet.detect(str(chunk[col]).encode("utf-8"))
            #print(encode_result['encoding'])
            #print("After: " + str(chunk[col]))
            #chunk[col].apply(lambda e: str(e).encode("utf-8", "xmlcharrefreplace").decode('utf-8', "xmlcharrefreplace"))
            if chunk[col].dtype == 'object':
                chunk[col].apply(lambda x: ''.join(ch for ch in str(x) if ord(ch) >= 32 or ch in '\n\r\t'))
            
        
        # 2. Handle mixed encodings if needed
        # This is often unnecessary if read_csv with latin-1 worked properly

        #chunk.apply(lambda s: normalize(s))

        # 3. Fix date formats, numeric values, etc. as needed for your data
        
        # Write to output file
        mode = 'w' if first_chunk else 'a'
        chunk.to_csv(output_path, mode=mode, index=False, header=first_chunk, encoding='utf-8')
        first_chunk = False
        
        # Status update
        print(f"Processed chunk {i+1} ({chunk_size * (i+1)} rows)")

# Example usage