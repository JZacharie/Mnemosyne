import os
import glob
import logging
import argparse
from typing import List, Optional
from pathlib import Path

import litellm
from rich.logging import RichHandler
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from langchain_community.document_loaders import (
    PyPDFLoader, 
    TextLoader, 
    UnstructuredMarkdownLoader
)
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import PGVector
from langchain_core.documents import Document

# Setup Rich Logging
logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)
log = logging.getLogger("mnemosyne-indexer")
console = Console()

class MnemosyneEmbeddings:
    """Wrapper for LiteLLM embeddings to be used with LangChain."""
    def __init__(self, model: str, api_base: str, api_key: str):
        self.model = model
        self.api_base = api_base
        self.api_key = api_key

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        res = litellm.embedding(
            model=self.model, 
            input=texts, 
            api_base=self.api_base, 
            api_key=self.api_key
        )
        return [d["embedding"] for d in res.data]

    def embed_query(self, text: str) -> List[float]:
        res = litellm.embedding(
            model=self.model, 
            input=[text], 
            api_base=self.api_base, 
            api_key=self.api_key
        )
        return res.data[0]["embedding"]

class MnemosyneIndexer:
    def __init__(self, config: argparse.Namespace):
        self.config = config
        self.embeddings = MnemosyneEmbeddings(
            model=config.embedding_model,
            api_base=config.litellm_url,
            api_key=config.litellm_api_key
        )
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=config.chunk_size,
            chunk_overlap=config.chunk_overlap
        )

    def load_document(self, file_path: str) -> List[Document]:
        """Loads a document based on its extension."""
        ext = os.path.splitext(file_path)[1].lower()
        try:
            if ext == ".pdf":
                loader = PyPDFLoader(file_path)
            elif ext == ".md":
                loader = UnstructuredMarkdownLoader(file_path)
            elif ext in [".txt", ".log"]:
                loader = TextLoader(file_path)
            else:
                log.debug(f"Skipping unsupported file type: {file_path}")
                return []
            
            return loader.load()
        except Exception as e:
            log.error(f"Error loading {file_path}: {e}")
            return []

    def process_file(self, file_path: str, base_path: str) -> List[Document]:
        """Loads, splits and adds metadata to a file."""
        documents = self.load_document(file_path)
        if not documents:
            return []

        chunks = self.text_splitter.split_documents(documents)
        
        file_name = os.path.basename(file_path)
        relative_path = os.path.relpath(file_path, base_path)
        stats = os.stat(file_path)
        
        # Determine source label from path if possible
        source_label = os.path.basename(base_path) if base_path != "/" else "root"

        for chunk in chunks:
            chunk.metadata.update({
                "source_path": relative_path,
                "file_name": file_name,
                "pvc_name": self.config.pvc_name if self.config.pvc_name != "unknown" else source_label,
                "file_size": stats.st_size,
                "last_modified": stats.st_mtime,
            })
        
        return chunks

    def run(self):
        console.print(f"\n[bold magenta]🧠 Mnemosyne Indexing Job[/bold magenta]")
        log.info(f"Scanning directories: [bold yellow]{self.config.paths}[/bold yellow]")
        
        for path in self.config.paths:
            if not os.path.exists(path):
                log.warning(f"⚠️ Path does not exist: {path}")
                continue

            log.info(f"🔍 Scanning [bold blue]{path}[/bold blue]...")
            files = []
            for ext in ["**/*.pdf", "**/*.md", "**/*.txt", "**/*.log"]:
                files.extend(glob.glob(os.path.join(path, ext), recursive=True))
            
            if not files:
                log.debug(f"No documents found in {path}")
                continue

            log.info(f"Found [bold cyan]{len(files)}[/bold cyan] documents in {path}.")

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console
            ) as progress:
                task = progress.add_task(f"[cyan]Indexing {os.path.basename(path)}...", total=len(files))

                for file_path in files:
                    file_name = os.path.basename(file_path)
                    progress.update(task, description=f"[cyan]Processing: {file_name}")
                    
                    chunks = self.process_file(file_path, path)
                    if chunks:
                        try:
                            PGVector.from_documents(
                                embedding=self.embeddings,
                                documents=chunks,
                                collection_name=self.config.collection_name,
                                connection_string=self.config.database_url,
                            )
                            log.info(f"✅ Indexed: {file_name} ({len(chunks)} chunks)")
                        except Exception as e:
                            log.error(f"❌ Error inserting {file_name} into DB: {e}")
                    
                    progress.advance(task)

        console.print("[bold green]✨ Mnemosyne Indexing complete![/bold green]\n")

def main():
    parser = argparse.ArgumentParser(description="Mnemosyne RAG Indexer")
    parser.add_argument("--paths", nargs="+", default=[os.environ.get("NFS_PATH", "/data/nfs")], help="Paths to scan for documents")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"), help="PostgreSQL connection string")
    parser.add_argument("--collection-name", default=os.environ.get("COLLECTION_NAME", "mnemosyne_docs"), help="PGVector collection name")
    parser.add_argument("--embedding-model", default=os.environ.get("EMBEDDING_MODEL", "zembed-132k"), help="LiteLLM embedding model")
    parser.add_argument("--litellm-url", default=os.environ.get("LITELLM_URL", "http://litellm.litellm.svc.cluster.local:4000"), help="LiteLLM API URL")
    parser.add_argument("--litellm-api-key", default=os.environ.get("LITELLM_API_KEY"), help="LiteLLM API Key")
    parser.add_argument("--pvc-name", default=os.environ.get("PVC_NAME", "unknown"), help="Name of the PVC being indexed")
    parser.add_argument("--chunk-size", type=int, default=1000, help="Document chunk size")
    parser.add_argument("--chunk-overlap", type=int, default=100, help="Document chunk overlap")
    
    args = parser.parse_args()
    
    if not args.database_url:
        log.error("DATABASE_URL is required.")
        return

    indexer = MnemosyneIndexer(args)
    indexer.run()

if __name__ == "__main__":
    main()
