import copy

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


