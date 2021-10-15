
import asyncio
import aiohttp
import yaml
import ssl

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

