import hmac
import base64
import aiohttp
import asyncio

from modules.helpers.logger import info, success, error, warning
from settings import OKX_KEY, OKX_PASSWORD, OKX_SECRET


async def okx_withdraw(
        address, amount, retry=0
):
    def okx_data(api_key, secret_key, passphras, request_path="/api/v5/account/balance?ccy=ETH", body='',
                 meth="GET"):

        try:
            import datetime
            def signature(
                    timestamp: str, method: str, request_path: str, secret_key: str, body: str = ""
            ) -> str:
                if not body:
                    body = ""

                message = timestamp + method.upper() + request_path + body
                mac = hmac.new(
                    bytes(secret_key, encoding="utf-8"),
                    bytes(message, encoding="utf-8"),
                    digestmod="sha256",
                )
                d = mac.digest()
                return base64.b64encode(d).decode("utf-8")

            dt_now = datetime.datetime.utcnow()
            ms = str(dt_now.microsecond).zfill(6)[:3]
            timestamp = f"{dt_now:%Y-%m-%dT%H:%M:%S}.{ms}Z"

            headers = {
                "Content-Type": "application/json",
                "OK-ACCESS-KEY": api_key,
                "OK-ACCESS-SIGN": signature(timestamp, meth, request_path, secret_key, body),
                "OK-ACCESS-TIMESTAMP": timestamp,
                "OK-ACCESS-PASSPHRASE": passphras,
                'x-simulated-trading': '0'
            }
        except Exception as ex:
            error(str(ex))
        return request_path, headers

    wallet = address
    SUB_ACC = True

    TOKEN_TO_WITHDRAW, CHAIN = 'USDC', 'Solana'

    api_key = OKX_KEY
    secret_key = OKX_SECRET
    passphras = OKX_PASSWORD

    # take FEE for withdraw
    _, headers = okx_data(api_key, secret_key, passphras,
                          request_path=f"/api/v5/asset/currencies?ccy={TOKEN_TO_WITHDRAW}",
                          meth="GET")
    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://www.okx.cab/api/v5/asset/currencies?ccy={TOKEN_TO_WITHDRAW}",
                               headers=headers, timeout=10) as response:
            response_data = await response.json()

    try:
        for lst in response_data['data']:
            if lst['chain'] == f'{TOKEN_TO_WITHDRAW}-{CHAIN}':
                FEE = lst['minFee']
    except Exception as err:
        if 'data' in str(err):
            raise Exception(
                "Incorrectly configured the OKX API. Maybe the IP wasn't added. If that doesn't help, create new API key with all permissions"
            )

    try:
        while True:
            if SUB_ACC:
                _, headers = okx_data(api_key, secret_key, passphras,
                                      request_path=f"/api/v5/users/subaccount/list", meth="GET")
                async with aiohttp.ClientSession() as session:
                    async with session.get("https://www.okx.cab/api/v5/users/subaccount/list",
                                           headers=headers, timeout=10) as response:
                        list_sub = await response.json()

                for sub_data in list_sub['data']:
                    while True:
                        name_sub = sub_data['subAcct']

                        _, headers = okx_data(api_key, secret_key, passphras,
                                              request_path=f"/api/v5/asset/subaccount/balances?subAcct={name_sub}&ccy={TOKEN_TO_WITHDRAW}",
                                              meth="GET")
                        async with aiohttp.ClientSession() as session:
                            async with session.get(
                                    f"https://www.okx.cab/api/v5/asset/subaccount/balances?subAcct={name_sub}&ccy={TOKEN_TO_WITHDRAW}",
                                    headers=headers, timeout=10) as response:
                                sub_balance = await response.json()

                        if sub_balance.get('msg') == f'Sub-account {name_sub} doesn\'t exist':
                            await warning(f'[-] OKX | Error: {sub_balance["msg"]}')
                            continue
                        sub_balance = sub_balance['data'][0]['bal']

                        if float(sub_balance) > 0:
                            await info(f'[•] OKX | {name_sub} | {sub_balance} {TOKEN_TO_WITHDRAW}')

                            body = {"ccy": f"{TOKEN_TO_WITHDRAW}", "amt": str(sub_balance), "from": 6, "to": 6,
                                    "type": "2",
                                    "subAcct": name_sub}
                            _, headers = okx_data(api_key, secret_key, passphras,
                                                  request_path=f"/api/v5/asset/transfer", body=str(body),
                                                  meth="POST")
                            async with aiohttp.ClientSession() as session:
                                async with session.post("https://www.okx.cab/api/v5/asset/transfer",
                                                        data=str(body), headers=headers, timeout=10) as response:
                                    await response.json()
                        break

            try:
                _, headers = okx_data(api_key, secret_key, passphras,
                                      request_path=f"/api/v5/account/balance?ccy={TOKEN_TO_WITHDRAW}")
                async with aiohttp.ClientSession() as session:
                    async with session.get(f'https://www.okx.cab/api/v5/account/balance?ccy={TOKEN_TO_WITHDRAW}',
                                           headers=headers, timeout=10) as response:
                        balance = await response.json()

                balance = balance["data"][0]["details"][0]["cashBal"]

                if balance != 0:
                    body = {"ccy": f"{TOKEN_TO_WITHDRAW}", "amt": float(balance), "from": 18, "to": 6, "type": "0",
                            "subAcct": "", "clientId": "", "loanTrans": "", "omitPosRisk": ""}
                    _, headers = okx_data(api_key, secret_key, passphras, request_path=f"/api/v5/asset/transfer",
                                          body=str(body), meth="POST")
                    async with aiohttp.ClientSession() as session:
                        async with session.post("https://www.okx.cab/api/v5/asset/transfer",
                                                data=str(body), headers=headers, timeout=10) as response:
                            await response.json()
            except Exception as ex:
                pass

            # CHECK MAIN BALANCE
            _, headers = okx_data(api_key, secret_key, passphras,
                                  request_path=f"/api/v5/asset/balances?ccy={TOKEN_TO_WITHDRAW}", meth="GET")
            async with aiohttp.ClientSession() as session:
                async with session.get(f'https://www.okx.cab/api/v5/asset/balances?ccy={TOKEN_TO_WITHDRAW}',
                                       headers=headers, timeout=10) as response:
                    main_balance = await response.json()

            main_balance = float(main_balance["data"][0]['availBal'])
            await info(f'[•] OKX | Total balance: {main_balance} {TOKEN_TO_WITHDRAW}')

            if amount > main_balance:
                await warning(f'[•] OKX | Not enough balance ({main_balance} < {amount}), waiting 10 secs...')
                await asyncio.sleep(10)
                continue

            break

        body = {"ccy": TOKEN_TO_WITHDRAW, "amt": amount, "fee": FEE, "dest": "4",
                "chain": f"{TOKEN_TO_WITHDRAW}-{CHAIN}",
                "toAddr": wallet}
        _, headers = okx_data(api_key, secret_key, passphras, request_path=f"/api/v5/asset/withdrawal",
                              meth="POST", body=str(body))
        async with aiohttp.ClientSession() as session:
            async with session.post("https://www.okx.cab/api/v5/asset/withdrawal",
                                    data=str(body), headers=headers, timeout=10) as response:
                result = await response.json()

        if result['code'] == '0':
            await success(f"[+] OKX | Success withdraw {amount} {TOKEN_TO_WITHDRAW} in {CHAIN} to {wallet}")
            return True
        else:
            err = result['msg']
            if retry < 3:
                await error(
                    f"[-] OKX | Withdraw {amount} {TOKEN_TO_WITHDRAW} {CHAIN} to {wallet} is unsuccessful. {err}")
                await asyncio.sleep(10)
                return await okx_withdraw(address, amount=amount, retry=retry + 1)
            else:
                raise ValueError(f'OKX withdraw error: {err}')

    except Exception as err:
        await error(f"[-] OKX | Withdraw {amount} {TOKEN_TO_WITHDRAW} {CHAIN} to {wallet} is unsuccessful. {err}")
        if retry < 3:
            await asyncio.sleep(10)
            if 'Insufficient balance' in str(err):
                return await okx_withdraw(address, amount=amount, retry=retry)
            return await okx_withdraw(address, amount=amount, retry=retry + 1)
        else:
            raise ValueError(f'OKX withdraw error: {err}')
