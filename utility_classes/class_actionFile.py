
import json
import requests

class dataRequests(object):

    __slots__ = ["data", "url"]

    def __init__(self, data,url= "http://ubook.local/ibs/book"):
        self.data = data
        self.url = url

    def getUrlReq(self):
        return self.url

    def postRequest(self):
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(self.data)
        result = ''
        result = requests.post(self.url, data=data, headers=headers)  
        return result

    def getRequest(self):
        result = ''
        # above we obtain the response from get request
        result = requests.get(self.url)
        # then we could use result.json/result.text to obtain the data
        # or result.status_code to receive the code
        return result     

