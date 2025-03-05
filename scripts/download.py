# Imports
import subprocess
import pyzenodo3
import os
import pandas as pd
import argparse
from pyunpack import Archive

# Custom Imports
from scripts.printer import CustomPrinter

class Download_LUNA25:
    def __init__(self):
        # Printer
        self.printer = CustomPrinter()
        
        # Links
        self.links = {
            "images": "10.5281/zenodo.14223624",
            "annotations": "10.5281/zenodo.14673658"
        }
        
        self.naming_convention = {
            "nodules": "nodules",
            "images": "images",
            "annotations": "annotations"
        }
        
        # Get Records
        with self.printer: print(f'Getting Records...')
        self.zen = pyzenodo3.Zenodo()
        self.record_images = self.zen.find_record_by_doi(self.links["images"]).data['files']
        self.record_annotations = self.zen.find_record_by_doi(self.links["annotations"]).data['files']

        # Limit Number of Downloads
        self.limit = -1
    
    def download(self):
        # Convert to pandas DataFrame
        df_images = pd.DataFrame(self.record_images)

        # Sort by 'key'
        df_images = df_images.sort_values(by='key')

        # Convert back to list of dictionaries
        self.record_images = df_images.to_dict(orient='records')

        image_count = 0

        for record in self.record_images:
            # Get the name and download url
            name = record['key']
            download_url = record['links']['self']
            
            # If "nodule" in name, then it is a nodule image
            if "nodule" in name:
                download_dir = self.naming_convention["nodules"]
            else:
                if self.limit != -1 and image_count >= self.limit:
                    # Go to Next Item in For Loop.
                    continue
                download_dir = self.naming_convention["images"]
                image_count += 1
            
            # Create the download directory
            download_dir = os.path.join("data", download_dir)
            os.makedirs(download_dir, exist_ok=True)
            
            # Check if the file already exists
            if os.path.exists(os.path.join(download_dir, name)):
                with self.printer: print(f'{name} already exists. Skipping...')
                continue
            
            # Print
            with self.printer: print(f'Now Downloading {name}...')
            
            # Download
            subprocess.run(["wget", "-q", "-O", os.path.join(download_dir, name), download_url])

        print(f'\n\n#############################')
        print(f'##    Images Downloaded    ##')
        print(f'#############################')

        # Download Location
        download_dir = f'{self.naming_convention["annotations"]}'
        download_dir = os.path.join("data", download_dir)
        os.makedirs(download_dir, exist_ok=True)

        for record in self.record_annotations:
            # Get the name and download url
            name = record['key']
            download_url = record['links']['self']
            
            # Check if the file already exists
            if os.path.exists(os.path.join(download_dir, name)):
                with self.printer: print(f'{name} already exists. Skipping...')
                continue
            
            # Print
            with self.printer: print(f'Now Downloading {name}...')
            
            # Download
            subprocess.run(["wget", "-q", "-O", os.path.join(download_dir, name), download_url])

        print(f'\n\n############################')
        print(f'## Annotations Downloaded ##')
        print(f'############################')
    
    def extract(self, data_folder, extract_folder):
        # Get All Items in the Various Folders
        images = os.listdir(os.path.join(data_folder, self.naming_convention["images"]))
        nodules = os.listdir(os.path.join(data_folder, self.naming_convention["nodules"]))
        
        extract_images = os.path.join(extract_folder, self.naming_convention["images"])
        extract_nodules = os.path.join(extract_folder, self.naming_convention["nodules"])
        
        # Ensure destination directories exist
        os.makedirs(extract_images, exist_ok=True)
        os.makedirs(extract_nodules, exist_ok=True)
        
        # Helper function to concatenate and extract multipart zip files
        def unpack_split_zip(files_list, source_folder, destination_folder):
            # Sort files to ensure correct order (lexicographic order)
            with self.printer: print(f'Now Sorting Files...')
            files_list.sort()
            
            # Concatenate split zip files into a single zip file
            with self.printer: print(f'Concatenating Files, Please Wait...')
            concatenated_zip_path = os.path.join(source_folder, "combined.zip")
            with open(concatenated_zip_path, "wb") as combined_file:
                for part_file in files_list:
                    part_path = os.path.join(source_folder, part_file)
                    with open(part_path, "rb") as part:
                        combined_file.write(part.read())
            
            # Extract the concatenated zip file using pyunpack
            with self.printer: print(f'Extracting Files, Please Wait...')
            Archive(concatenated_zip_path).extractall(destination_folder)
            
            # # Optionally clean up the temporary combined zip file
            # os.remove(concatenated_zip_path)
        
        def clean_up(image_folder, nodule_folder, extract_images, extract_nodules):
            # Clean Up All Files within.
            with self.printer: print(f'Cleaning Up...')
            for file in os.listdir(image_folder):
                os.remove(os.path.join(image_folder, file))
            for file in os.listdir(nodule_folder):
                os.remove(os.path.join(nodule_folder, file))
            
            # Move the Files Within Extracted Image/Nodule Folders to the Main Image/Nodule Folders
            with self.printer: print(f'Moving Files...')
            for file in os.listdir(extract_images):
                os.rename(os.path.join(extract_images, file), os.path.join(image_folder, file))
            for file in os.listdir(extract_nodules):
                os.rename(os.path.join(extract_nodules, file), os.path.join(nodule_folder, file))
            
            # Files are now in the main image/nodule folders.
            # However, the files we need are within subdirectory "luna25_images" and "luna25_nodule_blocks", in the main image/nodule folders.
            # We will move the files from the subdirectories to the main directories.
            with self.printer: print(f'Moving Items in Subdirectories to Main Data Directory...')
            for file in os.listdir(os.path.join(image_folder, "luna25_images")):
                os.rename(os.path.join(image_folder, "luna25_images", file), os.path.join(image_folder, file))
            for file in os.listdir(os.path.join(nodule_folder, "luna25_nodule_blocks")):
                os.rename(os.path.join(nodule_folder, "luna25_nodule_blocks", file), os.path.join(nodule_folder, file))
            
            # Remove the Subdirectories
            with self.printer: print(f'Removing Subdirectories...')
            os.rmdir(os.path.join(image_folder, "luna25_images"))
            os.rmdir(os.path.join(nodule_folder, "luna25_nodule_blocks"))

        # Process images folder
        with self.printer: print(f'Extracting images from {os.path.join(data_folder, self.naming_convention["images"])} to {extract_images}')
        unpack_split_zip(images, 
                        os.path.join(data_folder, self.naming_convention["images"]), 
                        extract_images)
        
        # Process nodules folder
        with self.printer: print(f'Extracting nodules from {os.path.join(data_folder, self.naming_convention["nodules"])} to {extract_nodules}')
        unpack_split_zip(nodules, 
                        os.path.join(data_folder, self.naming_convention["nodules"]), 
                        extract_nodules)

        clean_up(images, nodules, extract_images, extract_nodules)

class Arguments:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description='Download LUNA25 Dataset')
        self.parser.add_argument('--extract_only', type=bool, default=False, help='Extract The Files Only (Requires Downloaded Files)')
        self.args = self.parser.parse_args()

if __name__ == "__main__":
    opts = Arguments().args
    
    # Initialize
    dl = Download_LUNA25()
    
    # Check if we are extracting only
    if not opts.extract_only:
        dl.download()
    else:
        print("-- Extracting Files Only --\n\n")
    
    # Extract
    dl.extract("data", "extracted")