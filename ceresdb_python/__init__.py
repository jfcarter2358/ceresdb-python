import socket
import json
from typing import List, Dict
import time
import requests

ENCODING='utf-8'
__version__ = '1.1.0'

class Connection:
    def __init__(self, username: str, password: str, host: str, port: int) -> None:
        self.username = username
        self.password = password
        self.host = host
        self.port = port

    def query(self, query_string: str) -> List[Dict]:
        payload = {
            "query": query_string
        }

        try:
            req = requests.post(f'http://{self.host}:{self.port}/api/query', auth=(self.username, self.password), json=payload)
            req.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xxx
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            raise e
        except requests.exceptions.HTTPError as e:
            decoded_content = req.content.decode(ENCODING)
            error_data = json.loads(decoded_content)
            raise Exception(f"The server returned an error: {error_data['error']}")


        else:
            output_data = []
            decoded_content = req.content.decode(ENCODING)
            if decoded_content != 'null':
                output_data = json.loads(decoded_content)
            return output_data

    def timed_query(self, query_string: str) -> tuple[List[Dict], Dict]:
        payload = {
            "query": query_string
        }

        try:
            time_start = time.time()
            req = requests.post(f'http://{self.host}:{self.port}/api/query', auth=(self.username, self.password), json=payload)
            req.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xxx
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            raise e
        except requests.exceptions.HTTPError as e:
            raise e
        else:
            output_data = []
            decoded_content = req.content.decode(ENCODING)
            if decoded_content != 'null':
                output_data = json.loads(decoded_content)
                if type(output_data) == dict:
                    if 'error' in output_data.keys():
                        raise Exception(f"The server returned an error: {output_data['error']}")
            time_end = time.time()
            
            timing = str(time_end - time_start)

            return (output_data, timing)
