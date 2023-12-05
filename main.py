from gdrive_duplicates_cleaner.main import downloader as dl, move_duplicates as md

if __name__ == "__main__":
    DOWNLOADS_FOLDER = "/home/anderdam/gdrive_downloads"
    DUPLICADOS_FOLDER = "/home/anderdam/drive_duplicados"
    dl()
    md(source_folder=DOWNLOADS_FOLDER, target_folder=DUPLICADOS_FOLDER)
