import asyncio

from modules.core.browser import Browser
from modules.helpers.retry import async_retry
from modules.helpers.logger import debug
from time import time
from datetime import datetime


class Backpack(Browser):
    def __init__(self, account_id: str, api_key: str, api_secret: str, proxy: str, backpack_deposit_address: str | None):
        super().__init__(api_key, api_secret, proxy, account_id)
        self.account_id = account_id
        self.backpack_deposit_address = backpack_deposit_address
        
    @async_retry("Get Deposit Address")
    async def get_deposit_address(self):
        response = await self.send_request(
            method="GET",
            url=f"https://api.backpack.exchange/wapi/v1/capital/deposit/address",
            params={"blockchain": "Solana"},
            api_instruction="depositAddressQuery",

        )
        data = response.json()
        if not data.get("address"):
            raise Exception(f"Unexpected response: {data}")
        return data["address"]

    @async_retry("Get Account Statistics")
    async def get_account_statistics(self, last_reset_timestamp: int):
        offset = 0
        fills = []
        month_timestamp = int(time() - 60 * 60 * 24 * 30)

        while True:
            response = await self.send_request(
                method="GET",
                url="https://api.backpack.exchange/wapi/v1/history/fills",
                params={
                    "limit": 1000,
                    "offset": offset,
                },
                api_instruction="fillHistoryQueryAll"
            )
            if response.status_code != 200:
                raise Exception(f"Unexpected response <{response.status_code}> offset: {offset}: {response.text}")
            current_fills = response.json()
            fills.extend(current_fills)
            if len(current_fills) == 1000:
                offset += 1000
            else:
                break
        
        positions = {}
        for fill in fills:
            symbol = fill["symbol"]
            quantity = fill["quantity"]
            side = fill["side"]
            volume = float(fill["price"]) * float(quantity)
            timestamp = datetime.fromisoformat(fill["timestamp"]).timestamp()
            
            key = f"{symbol}_{quantity}"
            if key not in positions:
                positions[key] = []
            positions[key].append({
                "side": side,
                "volume": volume,
                "timestamp": timestamp
            })

        week_pnl = 0
        month_pnl = 0

        for position_fills in positions.values():
            i = 0
            while i < len(position_fills) - 1:
                open_fill = position_fills[i]
                close_fill = position_fills[i + 1]
                
                if (
                        open_fill["side"] == "Bid" and close_fill["side"] == "Ask" or
                        open_fill["side"] == "Ask" and close_fill["side"] == "Bid"
                ):
                    pnl = close_fill["volume"] - open_fill["volume"] if open_fill["side"] == "Bid" \
                        else open_fill["volume"] - close_fill["volume"]
                    
                    if close_fill["timestamp"] >= last_reset_timestamp:
                        week_pnl += pnl
                    if close_fill["timestamp"] >= month_timestamp:
                        month_pnl += pnl
                
                i += 2

        week_volume = round(sum([
            float(fill["price"]) * float(fill["quantity"])
            for fill in fills
            if datetime.fromisoformat(fill["timestamp"]).timestamp() >= last_reset_timestamp
        ]), 2)

        month_volume = round(sum([
            float(fill["price"]) * float(fill["quantity"])
            for fill in fills
            if datetime.fromisoformat(fill["timestamp"]).timestamp() >= month_timestamp
        ]), 2)

        week_days = len(set([
            datetime.fromisoformat(fill["timestamp"]).strftime('%Y-%m-%d')
            for fill in fills
            if datetime.fromisoformat(fill["timestamp"]).timestamp() >= last_reset_timestamp
        ]))

        month_days = len(set([
            datetime.fromisoformat(fill["timestamp"]).strftime('%Y-%m-%d')
            for fill in fills
            if datetime.fromisoformat(fill["timestamp"]).timestamp() >= month_timestamp
        ]))

        week_orders = len(set([
            fill["orderId"]
            for fill in fills
            if datetime.fromisoformat(fill["timestamp"]).timestamp() >= last_reset_timestamp
        ]))

        month_orders = len(set([
            fill["orderId"]
            for fill in fills
            if datetime.fromisoformat(fill["timestamp"]).timestamp() >= month_timestamp
        ]))

        liquidations = await self.get_liquidations()

        week_liquidations = len([
            fill
            for fill in liquidations
            if datetime.fromisoformat(fill["timestamp"]).timestamp() >= last_reset_timestamp
        ])

        month_liquidations = len([
            fill
            for fill in liquidations
            if datetime.fromisoformat(fill["timestamp"]).timestamp() >= month_timestamp
        ])

        return {
            "pnl": {
                "week": round(week_pnl, 6),
                "month": round(month_pnl, 6)
            },
            "volume": {
                "week": week_volume,
                "month": month_volume
            },
            "active_days": {
                "week": week_days,
                "month": month_days
            },
            "orders": {
                "week": week_orders,
                "month": month_orders
            },
            "liquidations": {
                "week": week_liquidations,
                "month": month_liquidations
            }
        }

    @async_retry("Get Prices")
    async def get_prices(self, futures_only=False):
        response = await self.send_request(
            method="GET",
            url=f"{self.BACKPACK_API}/tickers",
        )
        prices = {
            ticker["symbol"].replace("_USDC", "").replace("_PERP", ""): float(ticker["lastPrice"])
            for ticker in response.json()
            if not futures_only or ticker["symbol"].endswith("_PERP")
        }
        prices["USDC"] = 1
        return prices

    @async_retry("Get Balances")
    async def get_balances(self, net_equity=False, balances_and_equity=False):
        response = await self.send_request(
            method="GET",
            url=f"{self.BACKPACK_API}/capital/collateral",
            api_instruction="collateralQuery"
        )
        usdc_equity = {
            'USDC': float(response.json()["netEquityAvailable"]),
        }
        if net_equity:
            return usdc_equity

        if balances_and_equity:
            balances = {
                balance["symbol"]: float(balance["availableQuantity"])
                for balance in response.json()["collateral"]
            }
        else:
            balances = {
                balance["symbol"]: float(balance["totalQuantity"])
                for balance in response.json()["collateral"]
            }

        response = await self.send_request(
            method="GET",
            url=f"{self.BACKPACK_API}/capital",
            api_instruction="balanceQuery"
        )

        for token_name in response.json():
            if balances.get(token_name) is None:
                balances[token_name] = float(response.json()[token_name]["available"])

        if balances_and_equity:
            return usdc_equity, balances

        return balances

    @async_retry("Change Leverage")
    async def change_leverage(self, leverage: int) -> bool:
        response = await self.send_request(
            method="PATCH",
            url=f"{self.BACKPACK_API}/account",
            json={"leverageLimit": str(leverage)},
            api_instruction="accountUpdate"
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed: <{response.status_code}> {response.text}")
            
        account_info = await self.get_account_info()
        if account_info.get("leverageLimit") != str(leverage):
            raise Exception(f"Leverage change verification failed: {response.text}")
            
        await debug(f"Backpack | Changed leverage to {leverage} for {self.account_id}")
        return account_info
    
    @async_retry("Get Account Info")
    async def get_account_info(self):
        response = await self.send_request(
            method="GET",
            url=f"{self.BACKPACK_API}/account",
            api_instruction="accountQuery",
        )
        if response.status_code != 200:
            raise Exception(f"Failed: <{response.status_code}> {response.text}")
        return response.json()

    @async_retry("Get Token Decimals")
    async def get_token_decimals(self) -> dict:
        response = await self.send_request(
            method="GET",
            url=f"{self.BACKPACK_API}/markets",
            api_instruction="marketsQuery"
        )
        
        futures_decimals = {}
        
        for market in response.json():
            if not market["symbol"].endswith("_PERP"):
                continue
                
            min_quantity = market["filters"]["quantity"]["minQuantity"]
            min_price = market["filters"]["price"]["minPrice"]
            min_tick_size = market["filters"]["price"].get("tickSize", "0")
            
            quantity_decimals = len(min_quantity.split('.')[1]) if '.' in min_quantity else 0
            price_decimals = len(min_price.split('.')[1]) if '.' in min_price else 0
            tick_size_decimals = len(min_tick_size.split('.')[1]) if '.' in min_tick_size else 0
            
            token_name = market["baseSymbol"]
            futures_decimals[token_name] = {
                "amount": quantity_decimals,
                "price": price_decimals,
                "tick_size": tick_size_decimals
            }
            
        return futures_decimals
    
    @async_retry("Create Order")
    async def create_order(self, payload: dict):
        response = await self.send_request(
            method="POST",
            url=f"{self.BACKPACK_API}/order",
            json=payload,
            api_instruction="orderExecute",
        )
        return response.json()
    
    @async_retry("Get Futures Positions")
    async def get_futures_positions(self, attempt=0):
        try:
            response = await self.send_request(
                method="GET",
                url=f"{self.BACKPACK_API}/position",
                api_instruction="positionQuery",
            )
            if response.status_code != 200:
                raise Exception(f"Unexpected response <{response.status_code}>: {response.text}")
        except Exception as e:
            if 'Failed to perform, curl: (16)' in str(e) and attempt < 3:
                await asyncio.sleep(30)
                return await self.get_futures_positions(attempt=attempt+1)
            else:
                raise e
        return response.json()
    
    @async_retry("Withdraw")
    async def withdraw(self, address: str, amount: float, symbol: str = 'USDC', blockchain='Solana'):
        response = await self.send_request(
            method="POST",
            url=f"https://api.backpack.exchange/wapi/v1/capital/withdrawals",
            json={"address": address, "quantity": amount, "symbol": symbol, "blockchain": blockchain},
            api_instruction="withdraw",
        )
        if response.status_code != 200:
            if response.json().get('message'):
                raise Exception(f"Unexpected response <{response.status_code}>: {response.json()['message']}")
            raise Exception(f"Unexpected response <{response.status_code}>: {response.json()}")
        if response.json()['status'] not in ('pending', 'confirmed', 'success'):
            raise Exception(f"Failed to withdraw: {response.json()}")
        return response.json()
    
    @async_retry("Get Max Order Size")
    async def get_max_order_size(self, symbol: str, side: str):
        response = await self.send_request(
            method="GET",
            url=f"{self.BACKPACK_API}/account/limits/order",
            params={"symbol": symbol, "side": side},
            api_instruction="maxOrderQuantity",
        )
        if not response.json().get("maxOrderQuantity"):
            raise Exception(f"Unexpected response for {symbol} {side}: {response.json()}")
        return float(response.json()["maxOrderQuantity"])
    
    @async_retry("Get Liquidations")
    async def get_liquidations(self):
        offset = 0
        fills = []

        while True:
            response = await self.send_request(
                method="GET",
                url="https://api.backpack.exchange/wapi/v1/history/fills",
                params={
                    "limit": 1000,
                    "offset": offset,
                    "fillType": "AllLiquidation",
                    "marketType": "PERP"
                },
                api_instruction="fillHistoryQueryAll"
            )
            current_fills = response.json()
            fills.extend(current_fills)
            if len(current_fills) == 1000:
                offset += 1000
            else:
                break

        return fills

    @async_retry("Get Transferable Amount")
    async def get_transferable_amount(self, symbol: str):
        response = await self.send_request(
            method="GET",
            url=f"{self.BACKPACK_API}/account/limits/withdrawal",
            params={"symbol": symbol, "autoLendRedeem": True},
            api_instruction="maxWithdrawalQuantity",
        )
        if not response.json().get("maxWithdrawalQuantity"):
            raise Exception(f"Unexpected response for {symbol}: {response.json()}")
        return float(response.json()["maxWithdrawalQuantity"])

    @async_retry("Get Borrow Amount")
    async def get_borrow_amount(self):
        response = await self.send_request(
            method="GET",
            url=f"{self.BACKPACK_API}/borrowLend/positions",
            api_instruction="borrowLendPositionQuery",
        )
        if response.status_code != 200:
            raise Exception(f"Unexpected response <{response.status_code}>: {response.text}")
        for item in response.json():
            if item['symbol'] == 'USDC':
                return float(item['netExposureQuantity'])
        return 0
