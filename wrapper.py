#!/usr/bin/env python
'''
Python ProvStore API wrapper

Example usage:

    api = Api()  # uses settings provided in this file
    # or
    api = Api(api_location="https://provenance.ecs.soton.ac.uk/store/api/v0/",
              api_username="provstoreusername",
              api_key="yourapikey")  # uses parameter settings

    # Writing a document
    prov_bundle = [some ProvBundle object]
    document_id = api.submit_document(prov_bundle, identifier="identifier for document"[, public=True])

    # Reading a document as a prov.model.ProvBundle
    prov_bundle = api.get_document(document_id)

    # Get information about a document
    prov_bundle = api.get_document_meta(document_id)

    # Reading a document as provn
    prov_bundle = api.get_document(document_id, format='provn')  # returns a string of the document in provn

    # Adding a bundle to a document
    api.add_bundle(document_id, prov_bundle, "identifier for bundle")

    # Deleting a document
    api.delete_document(document_id)
'''

import urllib2
import json
from prov.model import ProvBundle


API_LOCATION = "https://provenance.ecs.soton.ac.uk/store/api/v0/"


class ApiError(Exception):
    pass


class ApiUnauthorizedError(ApiError):
    pass


class ApiBadRequestError(ApiError):
    pass


class ApiNotFoundError(ApiError):
    pass


class ApiCannotConvertToTheRequestedFormat(ApiError):
    pass


class Api(object):

    def __init__(self, api_location=None, api_username=None, api_key=None):
        self.api_location = api_location or API_LOCATION
        self.api_username = api_username
        self.api_key = api_key

    def _authorization_header(self):
        return "ApiKey %s:%s" % (self.api_username, self.api_key)

    def request(self, path, data=None, method=None, raw=False):
        headers = {
                   'Accept': 'application/json',
                   'Content-type': 'application/json'
        }

        if self.api_location and self.api_key:
            headers['Authorization'] = self._authorization_header()

        if data:
            request = urllib2.Request(self.api_location + path, json.dumps(data, cls=RequestDataSerializer), headers)
        else:
            request = urllib2.Request(self.api_location + path, None, headers)

        if method:
            request.get_method = lambda: method

        try:
            response = urllib2.urlopen(request)
        except urllib2.HTTPError as e:
            response_code = e.code
            if response_code == 401:
                raise ApiUnauthorizedError()
            elif response_code == 400:
                raise ApiBadRequestError()
            elif response_code == 404:
                raise ApiNotFoundError()
            elif response_code == 422:
                raise ApiNotFoundError()
            else:
                raise

        response_string = response.read()
        if response_string or raw:
            if raw:
                return response_string
            else:
                return json.loads(response_string)
        else:
            return True

    def submit_document(self, prov_document, identifier, public=False):
        """Returns the ID of the newly inserted document"""

        data = {
                 'content': prov_document,
                 'public': public,
                 'rec_id': identifier
               }

        response = self.request("documents/", data)
        return response['id']

    def get_document(self, doc_id, format=None, flattened=False, view=None):
        """Returns a ProvBundle object of the document with the ID provided or raises ApiNotFoundError"""

        extension = format if format is not None else 'json'
        view = "/views/%s" % view if view in ['data', 'process', 'responsibility'] else ""
        url = "documents/%d%s%s.%s" % (doc_id, "/flattened" if flattened else "", view, extension)
        response = self.request(url, raw=True)

        if format is None:
            # Try to decode it as a ProvBundle
            prov_document = ProvBundle()
            prov_document._decode_JSON_container(json.loads(response))
            return prov_document
        else:
            # return the raw response
            return response

    def get_document_meta(self, doc_id):
        """Returns a JSON object containing information about the document or raises ApiNotFoundError"""

        response = self.request("documents/" + str(doc_id) + "/")
        return response

    def add_bundle(self, doc_id, prov_document, identifier):
        """Adds a ProvBundle object to the document with the ID provided or raises ApiNotFoundError"""

        data = {
                 'content': prov_document,
                 'rec_id': identifier
               }

        self.request("documents/" + str(doc_id) + "/bundles/", data)
        return True

    def delete_document(self, doc_id):
        """Removes a ProvBundle object with the provided ID from the ProvStore"""

        self.request("documents/" + str(doc_id) + "/", method="DELETE")
        return True


# Allows JSON serialization of a prov document as an element of another JSON object
class RequestDataSerializer(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ProvBundle):
            return o._encode_JSON_container()
        else:
            return json.JSONEncoder.default(self, o)


if __name__ == '__main__':
    api = Api()
    print api.get_document(55)
    print api.get_document(55, raw=True)
    print api.get_document(55, format='provn')
    print api.get_document_meta(55)

    # Basic tests
    # from provserver.testdata.examples import bundles1, w3c_publication_2
    # new_id = api.submit_document(bundles1(), "identifier")
    # print new_id
    # print api.get_document(new_id)
    # print api.add_bundle(new_id, w3c_publication_2(), "added")
    # print api.delete_document(new_id)
