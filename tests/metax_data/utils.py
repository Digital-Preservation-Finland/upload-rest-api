from datetime import datetime

TEMPLATE_DATASET = {
    "id": None,
    "created": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"), # a default value
    "title": None, # non nullable
    "description": None,
    "modified": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"), # a default value
    "fileset": 
        {
            "csc_project": None,
            "total_files_size": 0 # what's the minimum files size
        },
    "preservation": {
            "state": -1,
            "description": None,
            "reason_description": None,
            "dataset_version": {
                "id": None,
                "persistent_identifier": None,
                "preservation_state": -1
            },
            "contract":None
    },
    "access_rights": None,
    "version": None, #has a default value?
    "language": [], #default is an empty list
    "persistent_identifier": None,
    "issued": None,
    "actors": [], # default is an empty list
    "keyword": [], # default is an empty list
    "theme": [], #default is an empty list
    "spatial": [], #default is an empty list
    "field_of_science": [], # default is an empty list 
    "provenance": [], # default is an empty list
    "metadata_owner": None,
    "data_catalog": None
    }
