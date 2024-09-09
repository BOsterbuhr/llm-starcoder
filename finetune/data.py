import os
import shutil

import pachyderm_sdk
from pachyderm_sdk.api.pfs import File, FileType


# ======================================================================================================================



def safe_open_wb(path):
    ''' Open "path" for writing, creating any parent directories as needed.
    '''
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return open(path, 'wb')


def download_pach_repo(
    pachyderm_host,
    pachyderm_port,
    repo,
    branch,
    root,
    token,
    fileset_id,
    datum_id,
    cache_location
):
    print(f"Starting to download dataset: {repo}@{branch} --> {root}")
    datum_path = f"/pfs/{datum_id}"

    if not os.path.exists(root):
        os.makedirs(root)

    if not os.path.exists(f"{root}/pfs/{datum_id}"):
        os.makedirs(f"{root}/pfs/{datum_id}")
    
    client = pachyderm_sdk.Client(
        host=pachyderm_host, port=pachyderm_port, auth_token=token
    )
    
    client.storage.assemble_fileset(
        fileset_id,
        path=datum_path,
        destination=root,
        cache_location=cache_location,
    )
    return [(os.path.join(root, file), file) for file in os.listdir(root)]



# ========================================================================================================
