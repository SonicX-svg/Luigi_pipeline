# Для запуска - python3 LUigiPip.py LuigiPIP --url "https://www.ncbi.nlm.nih.gov/geo/download/?acc=GSE68849&format=file" --output-dir data --local-scheduler 

import luigi
import requests
import tarfile
import os
import gzip
import shutil
import subprocess
from tqdm import tqdm

class LuigiPIP(luigi.Task):
    url = luigi.Parameter()
    output_dir = luigi.Parameter()

    def run(self):
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        # File path for the downloaded tar file
        tar_file_path = os.path.join(self.output_dir, 'archive.tar')

        # Download the file with progress bar
        self.download_with_progress(self.url, tar_file_path)

        # Verify TAR integrity
        if not self.check_tar_integrity(tar_file_path):
            raise Exception(f"TAR file integrity check failed for {tar_file_path}")

        # Extract all files, including nested .tar files
        extracted_files = self.extract_tar_recursive(tar_file_path)

        # Organize and extract .gz files into folders, and process each file
        self.organize_and_extract_files(extracted_files)

    def download_with_progress(self, url, output_path):
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024  # 1 KB
        t = tqdm(total=total_size, unit='iB', unit_scale=True, desc="Downloading")
        with open(output_path, 'wb') as f:
            for data in response.iter_content(block_size):
                t.update(len(data))
                f.write(data)
        t.close()
        if total_size != 0 and t.n != total_size:
            raise Exception("Download failed: Size mismatch")

    def check_tar_integrity(self, tar_file_path):
        """
        Check the integrity of the downloaded TAR file by attempting to read its members.
        """
        try:
            with tarfile.open(tar_file_path, 'r') as tar_ref:
                for member in tar_ref.getmembers():
                    tar_ref.extractfile(member)  # Test extraction
            print(f"TAR file {tar_file_path} passed integrity check.")
            return True
        except Exception as e:
            print(f"TAR file {tar_file_path} failed integrity check: {e}")
            return False

    def extract_tar_recursive(self, tar_file_path):
        """
        Recursively extracts tar files. If an extracted file is another tar, it is also extracted.
        """
        print(f"Extracting TAR file {tar_file_path}...")
        extracted_files = []

        def extract(tar_path, output_path):
            with tarfile.open(tar_path, 'r') as tar_ref:
                for member in tar_ref.getmembers():
                    tar_ref.extract(member, path=output_path)
                    extracted_file = os.path.join(output_path, member.name)
                    if member.isfile():
                        extracted_files.append(extracted_file)
                    elif member.name.endswith('.tar'):
                        print(f"Found nested tar file: {extracted_file}")
                        extract(extracted_file, output_path)

        extract(tar_file_path, self.output_dir)
        print(f"Extraction complete. Total files: {len(extracted_files)}")
        return extracted_files


    def organize_and_extract_files(self, file_paths):
        """
        Organizes each file into its own folder named after the file (without the extension).
        If the file is a .gz file, decompress it into the corresponding folder.
        If the file is a .txt or .tsv file, call external script to process it.
        """
        print("Organizing files and extracting .gz files...")
        
        # List to hold paths of all the extracted files (directories)
        extracted_directories = []

        for file_path in file_paths:
            # Get the base name of the file (without the directory and extension)
            file_name, file_ext = os.path.splitext(os.path.basename(file_path))
            folder_path = os.path.join(self.output_dir, file_name)

            # Create a folder with the file's base name
            os.makedirs(folder_path, exist_ok=True)

            print(f"Processing file: {file_path}")

            if file_ext == ".gz":  # If the file is a .gz archive, decompress it
                print(f"Decompressing {file_path} into {folder_path}...")
                decompressed_path = os.path.join(folder_path, file_name)  # Remove .gz extension
                with gzip.open(file_path, 'rb') as f_in:
                    with open(decompressed_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(file_path)  # Remove the .gz file after decompression
                file_path = decompressed_path  # Now work with the decompressed file
                print(f"Decompressed to {decompressed_path}")
            
            # Add the folder of the decompressed or original file to the list
            extracted_directories.append(folder_path)
            
        print("Files have been organized and processed. list extracted_directories ==============!!! ", str(extracted_directories))
        # Now, run external script for all extracted directories
 
        self.run_external_script(extracted_directories)


    def run_external_script(self, extracted_directories):
        """
        Call an external Python script to process files from the given directories.
        """
        script_name = 'to_tabs.py'  # External script you want to call
        for directory in extracted_directories: # Check if the path is a file before opening
            # Extract the file name
            file_path = directory + "/" + os.path.basename(directory)
            if os.path.isfile(file_path):
                with open(file_path, 'r') as f:
                    # process the file
                    print("!")
            else:
                print(f"{file_path} is a directory or does not exist.")  
            try:
                # Call the external script using subprocess for each directory
                subprocess.run(['python3', script_name, file_path], check=True)
                print(f"Successfully processed files in {directory} with external script.")
            except subprocess.CalledProcessError as e:
                print(f"Error occurred while processing {directory}: {e}")


if __name__ == "__main__":
    luigi.run()

