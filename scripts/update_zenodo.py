# Purpose:
#   This file updates and adds new uploads to an existing Zenodo deposition providing a valid Access Token and Conceptrec ID
#
# Configs:
#   ACCESS_TOKEN = your Zenodo application personal access token
#   conceptrecid = Zenodo deposition id you want to update.
#   folder_path = "folder path to the files"
#   >> create configs.json with contents formatted below
#    {
#        "conceptrecid":"",
#        "ACCESS_TOKEN":"",
#        "folder_path":""
#    }
# 
# Output:
#   Adding new files/Update changes to your Zenodo deposition
#
# How to excute:
#   ```
#   python zenodo_updatefiles.py
#   ```
#   Run the above line after correctly configured python environment and setting the configs
#
# Author: Nick Liu

import requests
import json
import os
import hashlib
import sys

from top_fibers_pkg.utils import get_logger


REPO_ROOT = "/home/data/apps/topfibers/repo"
LOG_DIR = "./logs"
LOG_FNAME = "upload_zenodo_files.log"
SUCCESS_FNAME = "success.log"
##################################
ACCESS_TOKEN = ''
folder_path = ''
conceptrecid = ''
# read in configs
with open("scripts/configs.json", 'r') as file:
        configs      = json.load(file)
        conceptrecid = configs["conceptrecid"]
        ACCESS_TOKEN = configs["ACCESS_TOKEN"]
        folder_path  = configs["folder_path"]
##################################

# get checksum
def calculate_md5(file_path):
    with open(file_path, 'rb') as file:
        md5_hash = hashlib.md5()
        while chunk := file.read(4096):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


# remove draft
def rm_draft(ACCESS_TOKEN, new_deposition_id):
    r = requests.delete(f'https://zenodo.org/api/deposit/depositions/{new_deposition_id}',
        params={'access_token': ACCESS_TOKEN})
    if r.status_code == 204:
        logger.info(f"Draft discarded")
    else:
        logger.info(f"Failed to discard draft, error code: {r.status_code}")


if __name__ == "__main__":
    if not (os.getcwd() == REPO_ROOT):
        sys.exit(
            "ALL SCRIPTS MUST BE RUN FROM THE REPO ROOT!!\n"
            f"\tCurrent directory: {os.getcwd()}\n"
            f"\tRepo root        : {REPO_ROOT}\n"
        )

    script_name = os.path.basename(__file__)
    logger = get_logger(LOG_DIR, LOG_FNAME, script_name=script_name, also_print=True)
    logger.info("-" * 50)
    logger.info(f"Begin script: {__file__}")

    # Get list of depositions
    r = requests.get('https://zenodo.org/api/deposit/depositions',
                    params={'access_token': ACCESS_TOKEN})
    if r.status_code == 200:
        logger.info(f"Retrieved all deposition info")
    else:
        logger.info(f"Failed to retrieve the list of depositions, error code: {r.status_code}")
        try:
            logger.info(f"trace: {r.json()}")
        except:
            logger.info("no trace available")
        finally:
            exit()
    all_depo = r.json()

    # find the corresponding depo
    deposition_id = -1
    for depo in all_depo:
        if depo["conceptrecid"] == conceptrecid:
            deposition_id = depo['links']['latest'].split('/')[-1]
            break
    if deposition_id == -1: logger.info(f"Failed to match the given conceptrecid: {conceptrecid}"); exit()

    # create a new version from the previous version as draft
    url_new_version = f"https://zenodo.org/api/deposit/depositions/{deposition_id}/actions/newversion"
    response = requests.post(url_new_version, params={'access_token': ACCESS_TOKEN})
    json_response = response.json()

    if response.status_code != 201:
        logger.info(f"Failed to create draft. Status code: {response.status_code}")
        logger.info(json_response)
    else:
        new_deposition_id = json_response['links']['latest_draft'].split("/")[-1]
        logger.info("Draft created.")
        logger.info(f"New deposition ID: {new_deposition_id}")

        # Retrieve list of local files
        file_names = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f)) and not f.startswith('.')]
        remote_files = {x['filename']:(x['checksum'],x['id']) for x in json_response["files"]}
        to_be_updated = []
        # iterate through local files and either update or upload
        for filename in file_names:
            file_path = f"{folder_path}/{filename}"
            if filename in remote_files:
                checksum, file_id = remote_files[filename]
                if calculate_md5(file_path)!=checksum:
                    logger.info(filename, calculate_md5(file_path),remote_files[filename])
                    
                    # replace/update existing file
                    response = requests.delete(f'https://zenodo.org/api/deposit/depositions/{new_deposition_id}/files/{file_id}',
                                                params={'access_token': ACCESS_TOKEN})
                    if response.status_code == 204:
                        logger.info(f"Old File deleted. {filename}")
                    else:
                        logger.info(f"Failed to Remove old {filename}. Status code: {response.status_code}")
                        logger.info(json_response)
                        rm_draft(ACCESS_TOKEN, new_deposition_id)

                    url = f'https://zenodo.org/api/deposit/depositions/{new_deposition_id}/files?access_token={ACCESS_TOKEN}'
                    data = {'name': filename}
                    files = {'file': open(file_path, 'rb')}
                    response = requests.post(url, data=data, files=files)
                    if response.status_code == 201:
                        logger.info(f"File added to the new draft. {filename}")
                        to_be_updated += [filename]
                    else:
                        logger.info(f"Failed to add {filename} to the new draft. Status code: {response.status_code}")
                        logger.info(response.json())
                        rm_draft(ACCESS_TOKEN, new_deposition_id)
            else:
                logger.info(f"new file :{filename}")
                # upload new file
                url = f'https://zenodo.org/api/deposit/depositions/{new_deposition_id}/files?access_token={ACCESS_TOKEN}'
                data = {'name': filename}
                files = {'file': open(file_path, 'rb')}
                response = requests.post(url, data=data, files=files)
                if response.status_code == 201:
                    logger.info(f"File added to the new draft. {filename}")
                    to_be_updated += [filename]
                else:
                    logger.info(f"Failed to add {filename} to the new draft. Status code: {response.status_code}")
                    logger.info(response.json())
                    rm_draft(ACCESS_TOKEN, new_deposition_id)

        if len(to_be_updated)<1:
            logger.info("No updates detected")
            rm_draft(ACCESS_TOKEN, new_deposition_id)
        else:
            # publish draft
            url_publish = f"https://zenodo.org/api/deposit/depositions/{new_deposition_id}/actions/publish"
            response = requests.post(url_publish, params={'access_token': ACCESS_TOKEN})

            if response.status_code == 202:
                logger.info(f"New version published. ID: {new_deposition_id}")
            else:
                logger.info(f"Failed to publish the new version. Status code: {response.status_code}")
                logger.info(response.json())
                rm_draft(ACCESS_TOKEN, new_deposition_id)
