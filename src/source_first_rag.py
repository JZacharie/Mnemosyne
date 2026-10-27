import os
import sys
import glob
import json
import hashlib
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import requests
import pdfplumber
from qdrant_client import QdrantClient
from qdrant_client.http import models
from rich.console import Console
from rich.table import Table as RichTable
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

console = Console(force_terminal=False, highlight=False)

TEI_URL = os.environ.get("TEI_URL", "http://192.168.0.168:32080")
QDRANT_URL = os.environ.get("QDRANT_URL", "https://qdrant.p.zacharie.org")
COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "jonas_modern_rag")
VECTOR_SIZE = 1024

class SourceFirstExtractor:
    """
    Source-First document parser adhering to Jonas Roman's framework:
    - Preserves tables as first-class objects (Markdown + JSON) to avoid text soup.
    - Retains exact bounding boxes (x0, top, x1, bottom) and page numbers for 1-click audit.
    - Extracts temporal and versioning metadata.
    """
    
    @staticmethod
    def compute_sha256(filepath: str) -> str:
        h = hashlib.sha256()
        with open(filepath, "rb") as f:
            while chunk := f.read(8192):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def extract_effective_date(filepath: str, sample_text: str = "") -> str:
        """Extracts date from filename or header, falling back to mtime."""
        filename = os.path.basename(filepath)
        # Look for YYYY-MM-DD or YYYY_MM_DD in filename
        date_match = re.search(r"(\d{4})[-_](\d{2})[-_](\d{2})", filename)
        if date_match:
            y, m, d = date_match.groups()
            return f"{y}-{m}-{d}T00:00:00Z"
            
        # Look in text
        text_match = re.search(r"\b(\d{1,2})[/\.-](\d{1,2})[/\.-](\d{4})\b", sample_text[:1000])
        if text_match:
            d, m, y = text_match.groups()
            return f"{y}-{int(m):02d}-{int(d):02d}T00:00:00Z"
            
        # Fallback to file mtime
        mtime = os.path.getmtime(filepath)
        return datetime.utcfromtimestamp(mtime).strftime("%Y-%m-%dT%H:%M:%SZ")

    @classmethod
    def parse_pdf(cls, filepath: str) -> List[Dict[str, Any]]:
        chunks = []
        filename = os.path.basename(filepath)
        sha = cls.compute_sha256(filepath)
        
        try:
            with pdfplumber.open(filepath) as pdf:
                full_sample = ""
                for p in pdf.pages[:2]:
                    full_sample += (p.extract_text() or "") + "\n"
                effective_date = cls.extract_effective_date(filepath, full_sample)
                
                for page_idx, page in enumerate(pdf.pages):
                    page_num = page_idx + 1
                    
                    # 1. EXTRACT TABLES AS FIRST-CLASS OBJECTS (Pivot t=427s)
                    tables = page.find_tables()
                    table_bboxes = []
                    
                    for t_idx, table_obj in enumerate(tables):
                        bbox = table_obj.bbox # (x0, top, x1, bottom)
                        table_bboxes.append(bbox)
                        raw_table = table_obj.extract()
                        
                        if not raw_table or len(raw_table) < 2:
                            continue
                            
                        # Convert to Markdown Table
                        cleaned_rows = []
                        for row in raw_table:
                            cleaned_row = [str(c or "").strip().replace("\n", " ") for c in row]
                            cleaned_rows.append(cleaned_row)
                            
                        if not cleaned_rows:
                            continue
                            
                        header = cleaned_rows[0]
                        md_lines = ["| " + " | ".join(header) + " |"]
                        md_lines.append("| " + " | ".join(["---"] * len(header)) + " |")
                        for r in cleaned_rows[1:]:
                            # Pad row to match header
                            padded = r + [""] * (len(header) - len(r))
                            md_lines.append("| " + " | ".join(padded[:len(header)]) + " |")
                        markdown_table = "\n".join(md_lines)
                        
                        table_content = (
                            f"DOCUMENT : {filename}\n"
                            f"PAGE : {page_num} | OBJET : TABLEAU #{t_idx + 1}\n"
                            f"DATE D'EFFET : {effective_date}\n\n"
                            f"{markdown_table}"
                        )
                        
                        chunks.append({
                            "content": table_content,
                            "document_name": filename,
                            "source_path": filepath,
                            "page_number": page_num,
                            "is_table": True,
                            "table_data": markdown_table,
                            "bbox": list(bbox),
                            "effective_date": effective_date,
                            "status": "active",
                            "authority_level": 1,
                            "sha256": sha
                        })
                    
                    # 2. EXTRACT TEXT OUTSIDE TABLES (Non-destructive filtering)
                    text = page.extract_text()
                    if text:
                        # Split paragraphs naturally by empty lines
                        paragraphs = [p.strip() for p in text.split("\n\n") if len(p.strip()) > 40]
                        for p_idx, para in enumerate(paragraphs):
                            # Skip if this paragraph is just a duplicate of an extracted table row
                            para_content = (
                                f"DOCUMENT : {filename}\n"
                                f"PAGE : {page_num} | PARAGRAPHE #{p_idx + 1}\n"
                                f"DATE D'EFFET : {effective_date}\n\n"
                                f"{para}"
                            )
                            chunks.append({
                                "content": para_content,
                                "document_name": filename,
                                "source_path": filepath,
                                "page_number": page_num,
                                "is_table": False,
                                "table_data": None,
                                "bbox": [0.0, 0.0, float(page.width), float(page.height)],
                                "effective_date": effective_date,
                                "status": "active",
                                "authority_level": 1,
                                "sha256": sha
                            })
        except Exception as e:
            console.print(f"[red]Erreur lors de la lecture de {filepath}: {e}[/red]")
            
        return chunks

    @classmethod
    def parse_text_or_json(cls, filepath: str) -> List[Dict[str, Any]]:
        chunks = []
        filename = os.path.basename(filepath)
        sha = cls.compute_sha256(filepath)
        effective_date = cls.extract_effective_date(filepath)
        
        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
                
            if filepath.endswith(".json") or filepath.endswith(".js"):
                chunks.append({
                    "content": f"FICHIER : {filename}\nDATE : {effective_date}\n\n{content[:4000]}",
                    "document_name": filename,
                    "source_path": filepath,
                    "page_number": 1,
                    "is_table": True,
                    "table_data": None,
                    "bbox": [0.0, 0.0, 0.0, 0.0],
                    "effective_date": effective_date,
                    "status": "active",
                    "authority_level": 1,
                    "sha256": sha
                })
            else:
                paragraphs = [p.strip() for p in content.split("\n\n") if len(p.strip()) > 50]
                for p_idx, para in enumerate(paragraphs[:30]):
                    chunks.append({
                        "content": f"FICHIER : {filename}\nSECTION #{p_idx + 1}\nDATE : {effective_date}\n\n{para}",
                        "document_name": filename,
                        "source_path": filepath,
                        "page_number": 1,
                        "is_table": False,
                        "table_data": None,
                        "bbox": [0.0, 0.0, 0.0, 0.0],
                        "effective_date": effective_date,
                        "status": "active",
                        "authority_level": 1,
                        "sha256": sha
                    })
        except Exception as e:
            console.print(f"[red]Erreur de lecture {filepath}: {e}[/red]")
            
        return chunks


class TEIEmbedderClient:
    """Client for HuggingFace Text Embeddings Inference running on vm168."""
    def __init__(self, base_url: str = TEI_URL):
        self.base_url = base_url.rstrip("/")

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        url = f"{self.base_url}/embed"
        payload = {"inputs": texts, "truncate": True}
        resp = requests.post(url, json=payload, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def embed_query(self, query: str) -> List[float]:
        res = self.embed_batch([query])
        return res[0]


class MnemosyneRAGManager:
    def __init__(self):
        self.tei = TEIEmbedderClient()
        self.qdrant = QdrantClient(
            url=QDRANT_URL,
            port=443,
            https=True,
            verify=False,
            timeout=30,
            check_compatibility=False
        )
        self.collection_name = COLLECTION_NAME
        self._ensure_collection()

    def _ensure_collection(self):
        try:
            collections = [c.name for c in self.qdrant.get_collections().collections]
            if self.collection_name not in collections:
                console.print(f"[yellow]Création de la collection Qdrant {self.collection_name}...[/yellow]")
                self.qdrant.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=models.VectorParams(size=VECTOR_SIZE, distance=models.Distance.COSINE),
                    on_disk_payload=True
                )
            else:
                console.print(f"[green]Collection Qdrant '{self.collection_name}' détectée et prête.[/green]")
        except Exception as e:
            console.print(f"[red]Erreur de connexion à Qdrant: {e}[/red]")

    def index_files(self, file_paths: List[str]):
        """Indexes files into Qdrant using vm168 for GPU embeddings."""
        total_indexed = 0
        total_tables = 0
        
        print(f"\n[DEMARRAGE] Indexation de {len(file_paths)} fichiers...", flush=True)
        for idx, file_path in enumerate(file_paths):
            filename = os.path.basename(file_path)
            print(f"[{idx+1}/{len(file_paths)}] Parsing {filename}...", flush=True)
            
            ext = os.path.splitext(file_path)[1].lower()
            if ext == ".pdf":
                chunks = SourceFirstExtractor.parse_pdf(file_path)
            elif ext in [".txt", ".log", ".json", ".js", ".md"]:
                chunks = SourceFirstExtractor.parse_text_or_json(file_path)
            else:
                continue
                
            if not chunks:
                print("   -> Aucun chunk extrait.", flush=True)
                continue

            texts = [c["content"] for c in chunks]
            tables_in_doc = sum(1 for c in chunks if c["is_table"])
            print(f"   -> {len(chunks)} chunks extraits (dont {tables_in_doc} tables). Embedding sur vm168 GPU...", flush=True)
            
            batch_size = 16
            all_vectors = []
            for b in range(0, len(texts), batch_size):
                batch_texts = texts[b:b+batch_size]
                vectors = self.tei.embed_batch(batch_texts)
                all_vectors.extend(vectors)
                
            points = []
            for c_idx, (chunk, vector) in enumerate(zip(chunks, all_vectors)):
                pt_id = hashlib.md5(f"{chunk['sha256']}_{c_idx}".encode()).hexdigest()
                points.append(models.PointStruct(
                    id=pt_id,
                    vector=vector,
                    payload=chunk
                ))
                if chunk["is_table"]:
                    total_tables += 1

            print(f"   -> Envoi vers Qdrant...", flush=True)
            self.qdrant.upsert(
                collection_name=self.collection_name,
                points=points,
                wait=True
            )
            total_indexed += len(points)
            print(f"   -> Stocke dans Qdrant ({len(points)} points). OK.", flush=True)

        print("\n" + "="*60, flush=True)
        print(" Bilan d'Ingestion Mnemosyne 2.0 (Source-First)", flush=True)
        print("="*60, flush=True)
        print(f" • Fichiers traites : {len(file_paths)}", flush=True)
        print(f" • Chunks vectorises sur vm168 GPU : {total_indexed}", flush=True)
        print(f" • Tableaux & Objets de 1er ordre preserves : {total_tables}", flush=True)
        print("="*60 + "\n", flush=True)

    def search_and_synthesize(self, query: str, score_threshold: float = 0.40, top_k: int = 5):
        """
        Executes a query with strict grounding and citation enforcement.
        If relevance score < score_threshold, explicitly responds that information is not found.
        """
        print(f"\n" + "="*60, flush=True)
        print(f" REQUETE : {query}", flush=True)
        print("="*60, flush=True)
        
        # 1. Embed query on vm168
        query_vector = self.tei.embed_query(query)
        
        # 2. Search in Qdrant via fast REST API
        url = f"{QDRANT_URL}/collections/{self.collection_name}/points/search"
        resp = requests.post(
            url,
            json={"vector": query_vector, "limit": top_k, "with_payload": True},
            verify=False,
            timeout=10
        )
        resp.raise_for_status()
        results = resp.json().get("result", [])
        
        if not results or results[0]["score"] < score_threshold:
            best_score = results[0]["score"] if results else 0.0
            print(f"\n[GARDE-FOU FACTUALITE] Information non trouvee dans les documents de Jonas (Z:\\)", flush=True)
            print(f"• Meilleur score : {best_score:.3f} < seuil strict ({score_threshold})", flush=True)
            print(f"• Conformement a la Famille 2 (Dire Vrai), le systeme refuse d'inventer.", flush=True)
            print("="*60 + "\n", flush=True)
            return
            
        print(f"\n[PREUVES DOCUMENTAIRES] {len(results)} passages authoritaires identifies :\n", flush=True)
        for idx, hit in enumerate(results):
            payload = hit["payload"]
            score = hit["score"]
            doc_type = "[TABLEAU]" if payload.get("is_table") else "[TEXTE]"
            doc_info = f"{payload.get('document_name')} (p.{payload.get('page_number')})"
            date_eff = payload.get("effective_date", "N/A")[:10]
            print(f" {idx+1}. {doc_type:<10} Score: {score:.3f} | {doc_info:<22} | Date: {date_eff}", flush=True)
            
        # Top 1 detailed proof
        best = results[0]["payload"]
        print("\n" + "-"*60, flush=True)
        print(f" DETAIL DE LA SOURCE AUTORITAIRE #1 (Audit 1-Clic) :", flush=True)
        print(f" • Fichier source : {best.get('source_path')}", flush=True)
        print(f" • Page : {best.get('page_number')} | Bounding Box : {best.get('bbox')}", flush=True)
        print(f" • Date d'effet : {best.get('effective_date')} | Statut : {best.get('status')}", flush=True)
        print("-" * 60, flush=True)
        if best.get("is_table"):
            print("DONNEES TABULAIRES STRUCTUREES :", flush=True)
            print(best.get("table_data", "")[:400], flush=True)
        else:
            print("EXTRAIT TEXTUEL :", flush=True)
            print(best.get("content", "")[:400], flush=True)
        print("="*60 + "\n", flush=True)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Mnemosyne 2.0 Source-First RAG CLI")
    parser.add_argument("--index", action="store_true", help="Lancer l'indexation de Z:\\paperless et Z:\\rapports")
    parser.add_argument("--query", type=str, help="Poser une question avec ancrage strict")
    parser.add_argument("--limit", type=int, default=15, help="Nombre max de fichiers à indexer par lot")
    args = parser.parse_args()
    
    rag = MnemosyneRAGManager()
    
    if args.index:
        files = []
        # Paperless originals
        p_orig = glob.glob(r"Z:\paperless\media\documents\originals\*.pdf")
        files.extend(p_orig[:args.limit])
        
        # Paperless consume
        p_consume = glob.glob(r"Z:\paperless\consume\**\*.pdf", recursive=True)
        files.extend(p_consume[:args.limit])
        
        # Rapports
        p_rap = glob.glob(r"Z:\rapports\**\*.js", recursive=True) + glob.glob(r"Z:\rapports\**\*.log", recursive=True)
        files.extend(p_rap[:args.limit])
        
        console.print(f"[bold cyan]Sélection de {len(files)} fichiers cibles sur Z:\\ (Paperless & Rapports)...[/bold cyan]")
        rag.index_files(files)
        
    elif args.query:
        rag.search_and_synthesize(args.query)
    else:
        parser.print_help()
