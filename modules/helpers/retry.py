import time
import asyncio
from settings import RETRY
from modules.helpers.logger import error


def retry(module_str: str, retries=RETRY):
    def decorator(f):
        def newfn(*args, **kwargs):
            attempt = 0
            while attempt < retries:
                try:
                    return f(*args, **kwargs)

                except Exception as e:
                    attempt += 1
                    if attempt == retries:
                        raise Exception(f'{module_str} | {e}')
                    error(f'[-] [{attempt}/{retries}] {module_str} | {e}', True)
                    time.sleep(30)
        return newfn
    return decorator


def async_retry(module_str: str, retries=RETRY):
    def decorator(f):
        async def newfn(*args, **kwargs):
            attempt = 0
            while attempt < retries:
                try:
                    return await f(*args, **kwargs)

                except Exception as e:
                    attempt += 1
                    if attempt == retries:
                        raise Exception(f'{module_str} | {e}')

                    account_name = None
                    if args and len(args) > 0:
                        self = args[0]
                        if hasattr(self, 'account_id'):
                            account_name = self.account_id

                    if account_name:
                        await error(f'{account_name} | [{attempt}/{retries}] {module_str} | {e}', True)
                    else:
                        await error(f'[-] [{attempt}/{retries}] {module_str} | {e}', True)
                    await asyncio.sleep(30)
        return newfn
    return decorator
