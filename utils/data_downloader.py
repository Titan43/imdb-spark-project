import requests
import os

class Downloader:
    def __init__(self, cache_dir: str):
        self.cache_dir = cache_dir
    
    def download_file(self, url: str):
        """
        Downloads a .tsv.gz file from the URL and caches it locally.
        If the file is already cached, it does nothing.

        :param url: URL pointing to the .tsv.gz file
        :return: The local cache path of the downloaded file
        """
        filename = url.split('/')[-1].replace(".tsv.gz", "")
        cache_path = os.path.join(self.cache_dir, f"{filename}.tsv.gz")
        
        if os.path.exists(cache_path):
            print(f"File already cached: {filename}")
            return cache_path
        
        print(f"Downloading {filename}...")
        try:
            response = requests.get(url)
            response.raise_for_status()  
            
            os.makedirs(self.cache_dir, exist_ok=True)
            
            with open(cache_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded and cached: {filename}")
            return cache_path
        
        except Exception as e:
            print(f"Error: failed to download {filename} — {e}")
            return None
