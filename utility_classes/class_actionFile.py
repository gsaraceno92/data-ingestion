
import json
import requests

class dataRequests(object):

    __slots__ = ["data", "url"]

    def postRequest(self, data, url):
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(data)
        result = ''
        result = requests.post(url, data=data, headers=headers)  
        return result

    def getRequest(self, url):
        result = ''
        # above we obtain the response from get request
        result = requests.get(url)
        # then we could use result.json/result.text to obtain the data
        # or result.status_code to receive the code
        return result     

