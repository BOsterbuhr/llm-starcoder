import os
import shutil

import pachyderm_sdk
from pachyderm_sdk.api.pfs import File, FileType
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import HTTPError

# ======================================================================================================================



def safe_open_wb(path):
    ''' Open "path" for writing, creating any parent directories as needed.
    '''
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return open(path, 'wb')


# Adding retry logic to handle 503 errors
@retry(
    retry=retry_if_exception_type(HTTPError),
    stop=stop_after_attempt(5),  # Retry up to 5 times
    wait=wait_exponential(multiplier=1, min=2, max=60),  # Exponential backoff
)


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
    print(f"Downloading {datum_path} to {root}")
    if not os.path.exists(root):
        os.makedirs(root)

    if not os.path.exists(f"{root}/pfs/{datum_id}"):
        os.makedirs(f"{root}/pfs/{datum_id}")
    
    client = pachyderm_sdk.Client(
        host=pachyderm_host, port=pachyderm_port, auth_token=token
    )
    
    try:
        client.storage.assemble_fileset(
            fileset_id,
            path=datum_path,
            destination=root,
            cache_location=cache_location,
        )
    except HTTPError as e:
        if e.response.status_code == 503:
            print("503 error encountered during download. Retrying...")
            raise  # Trigger retry for 503 errors
        else:
            raise  # Raise any other errors

    # Move the files to the root directory and remove the pfs/datum_id directory
    for file in os.listdir(f"{root}/pfs/{datum_id}"):
        print(f"Moving {file} from {root}/pfs/{datum_id} to {root}")
        shutil.move(f"{root}/pfs/{datum_id}/{file}", f"{root}/{file}")

    return [(os.path.join(root, file), file) for file in os.listdir(root)]



# ========================================================================================================
