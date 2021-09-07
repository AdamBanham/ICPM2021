import hashlib


def get_hash_checksum(filepath:str)->str:
    hash = hashlib.md5()
    bcontent = open(filepath,"rb").read()
    hash.update(bcontent)
    return hash.hexdigest()