import os

from tqdm import tqdm
from google_authentication import get_drive_service
from gdrive_duplicates_cleaner import constants


import os
from gdrive_duplicates_cleaner.google_authentication import get_drive_service
from tqdm import tqdm


def download_file(file_id, file_name, local_folder):
    service = get_drive_service()

    request = service.files().get_media(fileId=file_id)
    response = request.execute()

    file_size = response.info().get("Content-Length")
    remaining = file_size

    with open(os.path.join(local_folder, file_name), "wb") as f:
        with tqdm(
            total=file_size, unit="B", unit_scale=True, desc=f"Downloading {file_name}"
        ) as pbar:
            for chunk in iter(lambda: response.read(1024), b""):
                f.write(chunk)
                pbar.update(len(chunk))
                remaining -= len(chunk)
                pbar.set_postfix_str(f"Remaining: {remaining}")

                print(f"Downloading file: {file_name} (ID: {file_id})")


def list_and_download_files(local_folder):
    service = get_drive_service()

    results = service.files().list(pageSize=100).execute()
    files = results.get("files", [])

    for file in files:
        file_id = file["id"]
        file_name = file["name"]

        print(f"Downloading file: {file_name}")
        download_file(file_id, file_name, local_folder)


if __name__ == "__main__":
    output = constants.LOCAL_FOLDER
    list_and_download_files(output)
