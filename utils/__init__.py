def get_filename_from_url(url: str):
    return url.split('/')[-1].replace(".tsv.gz", "")