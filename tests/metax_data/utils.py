from datetime import datetime
import copy

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

def update_nested_dict(original, update):
    """Update nested dictionary.

    The keys of update dictionary are appended to
    original dictionary. If original already contains the key, it will be
    overwritten. If key value is dictionary, the original value is updated with
    the value from update dictionary.

    :param original: Original dictionary
    :param update: Dictionary that contains only key/value pairs to be updated
    :returns: Updated dictionary
    """
    updated_dict = copy.deepcopy(original)

    if original is None:
        return update

    for key in update:
        if key in original and isinstance(update[key], dict):
            updated_dict[key] = update_nested_dict(original[key], update[key])
        else:
            updated_dict[key] = update[key]

    return updated_dict


