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

##################################
ACCESS_TOKEN = ''
folder_path = ''
conceptrecid = ''
# read in configs
with open("configs.json", 'r') as file:
        configs      = json.load(file)
        conceptrecid = configs["conceptrecid"]
        ACCESS_TOKEN = configs["ACCESS_TOKEN"]
        folder_path  = configs["folder_path"]
##################################

def calculate_md5(file_path):
    with open(file_path, 'rb') as file:
        md5_hash = hashlib.md5()
        while chunk := file.read(4096):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


# Get list of depositions
print("#"*20)
r = requests.get('https://zenodo.org/api/deposit/depositions',
                  params={'access_token': ACCESS_TOKEN})
if r.status_code == 200:
    print(f"Successfully retrieved all depositions")
else:
    print(f"Failed to retrieve the list of depositions, error code: {r.status_code}")
    try:
        print(f"trace: {r.json()}")
    except:
        print("no trace available")
    finally:
        exit()
all_depo = r.json()

# find the corresponding depo
deposition_id = -1
for depo in all_depo:
    if depo["conceptrecid"] == conceptrecid:
        deposition_id = depo['links']['latest'].split('/')[-1]
        break
if deposition_id == -1: print(f"Failed to match the given conceptrecid: {conceptrecid}"); exit()

# create a new version from the previous version as draft
url_new_version = f"https://zenodo.org/api/deposit/depositions/{deposition_id}/actions/newversion"
response = requests.post(url_new_version, params={'access_token': ACCESS_TOKEN})
json_response = response.json()

if response.status_code != 201:
    print(f"Failed to create a new version. Status code: {response.status_code}")
    print(json_response)
else:
    new_deposition_id = json_response['links']['latest_draft'].split("/")[-1]
    print("New version created successfully.")
    print(f"New deposition ID: {new_deposition_id}")

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
                print(filename, calculate_md5(file_path),remote_files[filename])
                
                # replace/update existing file
                response = requests.delete(f'https://zenodo.org/api/deposit/depositions/{new_deposition_id}/files/{file_id}',
                                            params={'access_token': ACCESS_TOKEN})
                if response.status_code == 204:
                    print(f"Old File deleted successfully. {filename}")
                else:
                    print(f"Failed to Remove old {filename}. Status code: {response.status_code}")
                    print(json_response)
                    ####################
                    #     rm Draft     #
                    ####################
                    r = requests.delete(f'https://zenodo.org/api/deposit/depositions/{new_deposition_id}',
                            params={'access_token': ACCESS_TOKEN})
                    if r.status_code == 204:
                        print(f"Draft discarded")
                    else:
                        print(f"Failed to discard draft, see below for details : error code {r.status_code}")
                    #####################

                url = f'https://zenodo.org/api/deposit/depositions/{new_deposition_id}/files?access_token={ACCESS_TOKEN}'
                data = {'name': filename}
                files = {'file': open(file_path, 'rb')}
                response = requests.post(url, data=data, files=files)
                if response.status_code == 201:
                    print(f"File added to the new version successfully. {filename}")
                    to_be_updated += [filename]
                else:
                    print(f"Failed to add {filename} to the new version. Status code: {response.status_code}")
                    print(response.json())
                    ####################
                    #     rm Draft     #
                    ####################
                    r = requests.delete(f'https://zenodo.org/api/deposit/depositions/{new_deposition_id}',
                            params={'access_token': ACCESS_TOKEN})
                    if r.status_code == 204:
                        print(f"Draft discarded")
                    else:
                        print(f"Failed to discard draft, see below for details : error code {r.status_code}")
                    #####################
        else:
            print(f"new file :{filename}")
            # upload new file
            url = f'https://zenodo.org/api/deposit/depositions/{new_deposition_id}/files?access_token={ACCESS_TOKEN}'
            data = {'name': filename}
            files = {'file': open(file_path, 'rb')}
            response = requests.post(url, data=data, files=files)
            if response.status_code == 201:
                print(f"File added to the new version successfully. {filename}")
                to_be_updated += [filename]
            else:
                print(f"Failed to add {filename} to the new version. Status code: {response.status_code}")
                print(response.json())
                ####################
                #     rm Draft     #
                ####################
                r = requests.delete(f'https://zenodo.org/api/deposit/depositions/{new_deposition_id}',
                        params={'access_token': ACCESS_TOKEN})
                if r.status_code == 204:
                    print(f"Draft discarded")
                else:
                    print(f"Failed to discard draft, see below for details : error code {r.status_code}")
                #####################

    if len(to_be_updated)<1:
        ####################
        #     rm Draft     #
        ####################
        r = requests.delete(f'https://zenodo.org/api/deposit/depositions/{new_deposition_id}',
                  params={'access_token': ACCESS_TOKEN})
        if r.status_code == 204:
            print(f"No file needs to be updated, draft discarded")
        else:
            print(f"Failed to discard draft, error code : {r.status_code}")
        #####################
    else:
        # publish draft
        url_publish = f"https://zenodo.org/api/deposit/depositions/{new_deposition_id}/actions/publish"
        response = requests.post(url_publish, params={'access_token': ACCESS_TOKEN})

        if response.status_code == 202:
            print(f"New version published successfully. ID: {new_deposition_id}")
        else:
            print(f"Failed to publish the new version. Status code: {response.status_code}")
            print(response.json())
            ####################
            #     rm Draft     #
            ####################
            r = requests.delete(f'https://zenodo.org/api/deposit/depositions/{new_deposition_id}',
                    params={'access_token': ACCESS_TOKEN})
            if r.status_code == 204:
                print(f"Draft discarded")
            else:
                print(f"Failed to discard draft, error code: {r.status_code}")
            #####################
