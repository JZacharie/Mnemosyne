import requests
import os
from pathlib import Path

# Configuration
QDRANT_URL = "http://qdrant.qdrant.svc.cluster.local:6333"
COLLECTION_NAME = "mnemosyne_docs"
NFS_PATH = "/mnt/xpool" # Default path

def get_indexed_files():
    """Fetches all source_path metadata from Qdrant"""
    indexed_files = set()
    offset = None
    
    while True:
        payload = {
            "limit": 100,
            "with_payload": ["source_path"],
            "with_vector": False
        }
        if offset:
            payload["offset"] = offset
            
        response = requests.post(f"{QDRANT_URL}/collections/{COLLECTION_NAME}/points/scroll", json=payload)
        response.raise_for_status()
        data = response.json()["result"]
        
        for point in data["points"]:
            path = point.get("payload", {}).get("source_path")
            if path:
                indexed_files.add(path)
                
        offset = data.get("next_page_offset")
        if not offset:
            break
            
    return indexed_files

def scan_local_files(base_path):
    """Scans the local directory for indexable files"""
    local_files = set()
    extensions = {'.pdf', '.md', '.txt', '.log'}
    
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if Path(file).suffix.lower() in extensions:
                full_path = os.path.join(root, file)
                local_files.add(full_path)
                
    return local_files

def main():
    print(f"Connecting to Qdrant at {QDRANT_URL}...")
    try:
        indexed = get_indexed_files()
        print(f"Found {len(indexed)} files already indexed.")
    except Exception as e:
        print(f"Error connecting to Qdrant: {e}")
        return

    print(f"Scanning local files in {NFS_PATH}...")
    local = scan_local_files(NFS_PATH)
    print(f"Found {len(local)} indexable files on disk.")

    missing = local - indexed
    
    if missing:
        print(f"\nFound {len(missing)} files NOT indexed:")
        for file in sorted(list(missing)):
            print(f"- {file}")
            
        with open("non_indexed_files.txt", "w") as f:
            for file in sorted(list(missing)):
                f.write(f"{file}\n")
        print(f"\nList saved to non_indexed_files.txt")
    else:
        print("\nAll files are already indexed!")

if __name__ == "__main__":
    main()
