from urlparse import urlparse
from pyLibrary.structs.dicts import Struct


def URL(value):

    output = urlparse(value)

    return Struct(
        protocol=output.scheme,
        host=output.netloc,
        port=output.port,
        path=output.path,
        query=output.query,
        fragmen=output.fragment
    )
