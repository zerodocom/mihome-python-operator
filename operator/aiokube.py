
import asyncio
import aiohttp

class Kube:
    def __init__(self, kubeconfig):
        """
        init kube instance by kubeconfig
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

        if user.get("token"):
            self.headers["Authorization"] = f"Bearer {user['token']}"
        else:
            self.ssl = self._init_ssl(
                client_certificate_data=user.get('client-certificate-data'),
                client_key_data=user.get('client-key-data'),
                certificate_authority_data=cluster.get('certificate-authority-data')
            )


    @staticmethod
    def _init_ssl(client_certificate_data, client_key_data, certificate_authority_data=None):
        """
        generate sslcontext by cert info
        """
        certificate_authority = None
        if certificate_authority_data is not None:
            certificate_authority = tempfile.NamedTemporaryFile(delete=False)
            with open(certificate_authority.name, 'w') as f:
                f.write(base64.b64decode(certificate_authority_data).decode('utf-8'))
            cafile = certificate_authority.name
        else:
            cafile = None

        if client_certificate_data is None:
            raise ValueError("client_certificate_data is required")
        client_certificate = tempfile.NamedTemporaryFile(delete=False)
        with open(client_certificate.name, 'w') as f:
            f.write(base64.b64decode(client_certificate_data).decode('utf-8'))

        if client_key_data is None:
            raise ValueError("client_key_data is required")
        client_key = tempfile.NamedTemporaryFile(delete=False)
        with open(client_key.name, 'w') as f:
            f.write(base64.b64decode(client_key_data).decode('utf-8'))

        sslcontext = ssl.create_default_context(cafile=cafile)
        if certificate_authority_data is None:
            sslcontext.check_hostname = False
            sslcontext.verify_mode = ssl.CERT_NONE

        sslcontext.load_cert_chain(certfile=client_certificate.name, keyfile=client_key.name)

        if certificate_authority:
            os.remove(certificate_authority.name)
        os.remove(client_certificate.name)
        os.remove(client_key.name)

        return sslcontext


    async def get_session(self):
        if self.session is None:
            self.connector = TCPConnector(loop=asyncio.get_event_loop(), limit=1000)
            self.session = aiohttp.ClientSession(connector=self.connector)
        return self.session

