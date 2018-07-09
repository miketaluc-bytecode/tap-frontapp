import os
import hashlib
import json
import time
from datetime import datetime
from binascii import hexlify
from base64 import b64encode

import requests
import backoff
import singer
from singer import metrics

LOGGER = singer.get_logger()

class RateLimitException(Exception):
    pass

class MetricsRateLimitException(Exception):
    pass

class Client(object):
    BASE_URL = 'https://api2.frontapp.com/analytics'

    def __init__(self, config):
        self.token = config.get('token')
        # this should really be an array of metrics rather than just one.  just going to leave it one for now. 
        #self.metrics = config.get('metrics')
        self.metric = config.get('metric')
        self.session = requests.Session()

        self.calls_remaining = None
        self.limit_reset = None

# mike don't think i need this 
# though we could prob use the created date
#    def get_wsse_header(self):
        # commenting since we aren't passing an encrypted password
        # should we encrypt the token?  not for now.
        # nonce = hexlify(os.urandom(16)).decode('utf-8')
        # created = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+00:00')
        # sha1 = hashlib.sha1(str.encode(nonce + created + self.secret)).hexdigest()
        # password_digest = bytes.decode(b64encode(str.encode(sha1)))

        #return ('UsernameToken Username="{}", ' +
        #        'PasswordDigest="{}", Nonce="{}", Created="{}"').format(
        #        'Created="{}"').format(
        #            self.username,
        #            password_digest,
        #            nonce,
        #            created)

    def url(self, path):
        return self.BASE_URL + path

    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def request(self, method, path, **kwargs):
        if self.calls_remaining is not None and self.calls_remaining == 0:
            wait = self.limit_reset - int(time.monotonic())
            if wait > 0 and wait <= 300:
                time.sleep(wait)

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        if self.token:
            kwargs['headers']['Authorization: Bearer '] = self.token
#        kwargs['headers']['X-WSSE'] = self.get_wsse_header()

        kwargs['headers']['Content-Type'] = 'application/json'

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
# mike should prob change the name of metrics
            with metrics.http_request_timer(endpoint) as timer:
                response = requests.request(method, self.url(path), **kwargs)
                timer.tags[metrics.Tag.http_status_code] = response.status_code

# here we need to look at the response and ask if Progress = 100.  if not, wait one second then make the call again


        else:
            response = requests.request(method, self.url(path), **kwargs)

        self.calls_remaining = int(response.headers['X-Ratelimit-Remaining'])
        self.limit_reset = int(response.headers['X-Ratelimit-Reset'])

        if response.status_code in [429, 503]:
            raise RateLimitException()
        if response.status_code == 423:
            raise MetricsRateLimitException()
        try:
            response.raise_for_status()
        except:
            LOGGER.error('{} - {}'.format(response.status_code, response.text))
            raise
        return response.json()['data']

    def get(self, path, **kwargs):
        return self.request('get', path, **kwargs)

    def post(self, path, data, **kwargs):
        kwargs['data'] = json.dumps(data)
        return self.request('post', path, **kwargs)
