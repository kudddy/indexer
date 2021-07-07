import requests
import json


class ClientFastText:
    def __init__(self, url):
        self.url = url

        self.size_vector = 60

    def get_most_similar(self, word: str, topn:int=30):
        result = requests.post(
            url=f'{self.url}/getnearesttoken/{word}/{topn}')
        if result.status_code == 200:
            result = result.json()
        else:
            return None
        return result['PAYLOAD']['result']

    def get_vector(self, line, is_tokenize=True):
        print(f'{self.url}/get_vector')
        return requests.post(headers={'Content-Type': 'application/json'},
                             url=f'{self.url}/get_vector',
                             data=json.dumps({"token": self._TOKEN, "data": line,
                                              "is_tokenize": is_tokenize})).json()['result']
