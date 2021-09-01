import hashlib
from typing import Dict
import json
from copy import deepcopy,copy

def check_hash_in_map(map:Dict[str,str]) -> bool:
    """
    Checks that contents of a map match its hash
    """
    verified = False
    if( "hash" not in map):
        raise NotImplementedError("Hash not include in map.")
    temp = deepcopy(map)
    hash = copy(temp["hash"])
    del temp["hash"]
    new_hash = hashlib.md5(json.dumps(temp).encode()).hexdigest()
    verified = hash == new_hash
    return verified