from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from base64 import b64encode, b64decode

from curl_cffi.requests import AsyncSession
from time import time
from json import dumps

from modules.helpers.utils import request_proxy_format


class Browser:
    BACKPACK_API = "https://api.backpack.exchange/api/v1"

    def __init__(
            self,
            api_key: str,
            api_secret: str,
            proxy: str,
            account_name: str
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.private_key = Ed25519PrivateKey.from_private_bytes(b64decode(api_secret))
        self.account_name = account_name

        self.proxy = proxy
        self.req_proxy = request_proxy_format(self.proxy)

        self.session = self.get_new_session()

    def get_new_session(self):
        session = AsyncSession(
            impersonate="chrome131",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1.1 Safari/605.1.1",
                "Origin": "https://backpack.exchange",
                "Referer": "https://backpack.exchange/",
            }
        )
        if self.req_proxy:
            session.proxies.update(self.req_proxy)

        return session

    async def send_request(self, **kwargs):
        if kwargs.get("api_instruction") is not None:
            headers = kwargs.get("headers", {})
            headers.update(
                self.build_headers(
                    kwargs["api_instruction"],
                    {**kwargs.get("params", {}), **kwargs.get("json", {})},
                )
            )
            kwargs["headers"] = headers
            del kwargs["api_instruction"]

        if kwargs.get("session"):
            session = kwargs["session"]
            del kwargs["session"]
        else:
            session = self.session

        return await session.request(**kwargs)

    def build_headers(self, method: str, params: dict):
        timestamp = str(int(time() * 1e3))
        window = "10000"

        body = {
            key: dumps(value) if type(value) == bool else value
            for key, value in sorted(params.items())
        }
        body.update({
            "timestamp": timestamp,
            "window": window,
        })
        instruction = f"instruction={method}&" if method else ""
        str_body = instruction + "&".join(f"{key}={value}" for key, value in body.items())
        signature = self.private_key.sign(str_body.encode())
        encoded_signature = b64encode(signature).decode()

        res = {
            "X-Timestamp": timestamp,
            "X-Window": window,
            "X-API-Key": self.api_key,
            "X-Signature": encoded_signature,
        }

        return res
