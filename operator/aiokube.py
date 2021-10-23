
import os
import asyncio
import aiohttp
import yaml
import ssl
import base64
import tempfile
from urllib.parse import urlencode
import json
import traceback

class Kube:
    def __init__(self, kubeconfig, settings={}):
        """
        init kube instance by kubeconfig
        
        default
        settings:
           request_timeout: 3

        """
        kube_info = yaml.safe_load(kubeconfig)
        
        clusters = kube_info.get('clusters')
        users = kube_info.get('users')
        if clusters is None or users is None:
            raise ValueError("clusters or users not found in kubeconfig")
        cluster = clusters[0]['cluster']
        user = users[0]['user']

        self.apiserver = cluster['server']
        self.headers = {}
        self.ssl = None
        self.session = None
        self.connector = None
        self.settings = {
           "request_timeout": 3,
           "receive_timeout": 300,     
        }
        self.settings.update(settings)

        if user.get("token"):
            self.headers["Authorization"] = f"Bearer {user['token']}"
        else:
            
            if user.get('client-certificate-data') is not None:
                client_certificate_file = self.generate_temp_file(
                    base64.b64decode(user["client-certificate-data"]).decode('utf-8')
                ).name
            elif user.get('client-certificate') is not None:
                client_certificate_file = user["client-certificate"]
            else:
                raise ValueError("client-certificate-data or client-certificate is required")

            if user.get('client-key-data') is not None:
                client_key_file = self.generate_temp_file(
                    base64.b64decode(user["client-key-data"]).decode('utf-8')
                ).name
            elif user.get('client-key') is not None:
                client_key_file = user["client-key"]
            else:
                raise ValueError("client-key-data or client-key is required")

           
            if user.get('certificate-authority-data') is not None:
                certificate_authority_file = self.generate_temp_file(
                    base64.b64decode(user["certificate-authority-data"]).decode('utf-8')
                ).name
            elif user.get('certificate-authority') is not None:
                certificate_authority_file = user["certificate-authority"]
            else:
                certificate_authority_file = None
 
            self.ssl = self._init_ssl(
                client_certificate_file=client_certificate_file,
                client_key_file=client_key_file,
                cafile=certificate_authority_file
            )

            if user.get('client-certificate-data') is not None:
                os.remove(client_certificate_file)
            if user.get('certificate-authority-data') is not None:
                os.remove(certificate_authority_file)
            if user.get('client-key-data') is not None:
                os.remove(client_key_file)


    @staticmethod
    def _init_ssl(client_certificate_file, client_key_file, cafile=None):
        """
        generate sslcontext by cert info
        """

        sslcontext = ssl.create_default_context(cafile=cafile)
        if cafile is None:
            sslcontext.check_hostname = False
            sslcontext.verify_mode = ssl.CERT_NONE

        sslcontext.load_cert_chain(certfile=client_certificate_file, keyfile=client_key_file)

        return sslcontext


    def get_session(self):
        if self.session is None:
            self.connector = aiohttp.TCPConnector(loop=asyncio.get_event_loop(), limit=1000)
            self.session = aiohttp.ClientSession(connector=self.connector)
        return self.session

    def get_setting(self, key, overwrite_value=None):
        if overwrite_value is None:
            return self.settings.get(key)
        else:
            return overwrite_value

    def get_url(self, uri):
        return f"{self.apiserver}{uri}"

    async def get_resource_info(self, apiVersion, kind):
        if apiVersion in ["v1"]:
            uri = "/api/v1"
        else:
            uri = f"/apis/{apiVersion}"
        for resource in (await self.get(uri)).get("resources"):
            if resource.get("kind") == kind:
                return resource

    @staticmethod
    def generate_temp_file(raw):
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        with open(temp_file.name, 'w') as f:
            f.write(raw)
        return temp_file

    async def get(self, uri, params=None, timeout=None):
        async with self.get_session().get(
            url=self.get_url(uri), 
            params=params, 
            headers=self.headers, 
            ssl=self.ssl, 
            timeout=self.get_setting("request_timeout", timeout)
        ) as resp:
            return await resp.json()


    def watch(self, uri, params=None, receive_timeout=None):
        return Watcher(self, uri, params, receive_timeout)


class Watcher:
    def __init__(self, kube, uri, params, receive_timeout):
        self.kube = kube
        self.uri = uri
        self.params = params if params is not None else {}
        self.receive_timeout = self.kube.get_setting("receive_timeout", receive_timeout)
        self.last_resource_version = 0
        self.ws_connect = None
        self.ws_instance = None

    def save_checkpoint(self, resource_version):
        if resource_version is None:
            return
        if int(resource_version) > self.last_resource_version:
            self.last_resource_version = int(resource_version)

    async def close_ws_connect(self):
        await self.ws_connect.__aexit__(None, None, None)
        self.ws_connect = None

    async def __aenter__(self):
        await self.reconnect()
        return OperationReceiver(self.ws_instance, self)

    async def __aexit__(self, exc_type, exc, tb):
        await self.ws_connect.__aexit__(exc_type, exc, tb)

    async def reconnect(self):
        if self.ws_connect is not None:
            await self.close_ws_connect()

        req_params = {"watch": 1, "allowWatchBookmarks": "true"}
        req_params.update(self.params)
        if self.last_resource_version > 0:
            req_params["resourceVersion"] = self.last_resource_version

        url = self.kube.get_url(self.uri) + "?" + urlencode(req_params)
        self.ws_connect = self.kube.get_session().ws_connect(
            url=url,
            headers=self.kube.headers,
            ssl=self.kube.ssl,
            origin=self.kube.apiserver,
            receive_timeout=self.receive_timeout
        )
        self.ws_instance = await self.ws_connect.__aenter__()
        return self.ws_instance

class OperationReceiver:
    def __init__(self, ws_instance, watcher):
        self.ws_instance = ws_instance
        self.watcher = watcher
        self.queue = asyncio.Queue()
        self.worker = asyncio.create_task(self.worker())
        self.enable = True

    def __del__(self):
        self.enable = False
        self.worker.cancel()

    async def worker(self):
        while self.enable:
            try:
                msg = await self.ws_instance.receive()
                if msg.type is aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if data.get('type') == "ERROR":
                        if "too old resource version" in data['object'].get("message", ""):
                            # {
                            #     "type":"ERROR",
                            #     "object":{
                            #         "kind":"Status",
                            #         "apiVersion":"v1",
                            #         "metadata":{},
                            #         "status":"Failure",
                            #         "message":"too old resource version: 1 (322)",
                            #         "reason":"Expired",
                            #         "code":410
                            #      }
                            # }
                            recommend_version = data['object']['message'].split("(")[1].split(")")[0]
                            self.watcher.save_checkpoint(recommend_version)
                            self.ws_instance = await self.watcher.reconnect()
                        else:
                            print(data)
                    else:
                        self.watcher.save_checkpoint(data.get('object',{}).get("metadata", {}).get("resourceVersion"))
                        print("put %s" % data)
                        if data.get('type') not in ["BOOKMARK", None]:
                            self.queue.put_nowait(data)
                        continue

                elif msg.type in [aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE]:
                    self.ws_instance = await self.watcher.reconnect()
                else:
                    print(msg)
            except asyncio.TimeoutError:
                self.ws_instance = await self.watcher.reconnect()
            except:
                print(traceback.format_exc())
                 
            await asyncio.sleep(5)

    async def receive(self):
        return await self.queue.get()



