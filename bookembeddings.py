import os
import uuid
import pandas as pd
import pytesseract
import pdfplumber
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np

MODEL_NAME = "sentence-transformers/all-mpnet-base-v2"
CHUNK_SIZE = 800
CHUNK_OVERLAP = 200
BOOK_PATH = "invoice_book.pdf"
OUTPUT_CSV = "book_embeddings.csv"

model = SentenceTransformer(MODEL_NAME)

def extract_text_with_ocr(pdf_path):
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text or len(text.strip()) == 0:
                img = page.to_image(resolution=300).original
                text = pytesseract.image_to_string(img)
            pages.append(text or "")
    return pages

def chunk_text(text, size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    chunks = []
    start = 0
    while start < len(text):
        end = start + size
        chunks.append(text[start:end])
        start += size - overlap
    return chunks

def build_index(pages_text):
    data, embeddings = [], []

    for page_i, txt in enumerate(pages_text, start=1):
        chunks = chunk_text(txt)
        embs = model.encode(chunks, normalize_embeddings=True)

        for idx, (chunk, emb) in enumerate(zip(chunks, embs)):
            uid = str(uuid.uuid4())
            data.append({
                "id": uid,
                "page": page_i,
                "chunk_idx": idx,
                "text": chunk,
                "embedding": emb.tolist()
            })
            embeddings.append(emb)

    df = pd.DataFrame(data)
    vecs = np.array(embeddings, dtype="float32")
    index = faiss.IndexFlatIP(vecs.shape[1])
    index.add(vecs)
    return df, index, vecs

def generate_book_embeddings(pdf_path=BOOK_PATH, output_csv=OUTPUT_CSV):
    pages = extract_text_with_ocr(pdf_path)
    df, idx, vecs = build_index(pages)
    
    # Save to CSV
    df.to_csv(output_csv, index=False)
    print(f"Book embeddings saved to {output_csv}")
    
    return df

if __name__ == "__main__":
    generate_book_embeddings()
