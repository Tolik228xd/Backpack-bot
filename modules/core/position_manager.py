import asyncio
import random
from typing import List
from modules.core.backpack import Backpack
from modules.helpers.logger import success, error, info, warning, debug
from settings import POSITION_SETTINGS, RETRY, ORDERS_TIMEOUT
from modules.data.constants import TOKEN_LEVERAGE
from modules.helpers.utils import round_to_decimals, calculate_short_positions
from time import time


class PositionManager:
    def __init__(self, futures_decimals: dict = {}):
        self.futures_decimals = futures_decimals

    def get_token_leverage(self, token: str) -> int:
        return TOKEN_LEVERAGE.get(token, TOKEN_LEVERAGE["default"])

    async def create_future_order(
            self,
            account: Backpack,
            token: str,
            side: str,
            usdc_amount: float = 0,
            token_amount: float = 0,
            leverage: int = 0,
            retry: int = 0,
            log_error=True,
    ):
        if leverage:
            account_info = await account.get_account_info()
            if account_info.get("leverageLimit") != str(leverage):
                await account.change_leverage(leverage)
                await asyncio.sleep(random.uniform(2.5, 7.5))

        normalized_side = "LONG" if side == "Bid" else "SHORT"
        payload = {"orderType": "Market"}

        trading_pair = f"{token}_USDC_PERP"
        max_order_size, token_prices = await asyncio.gather(
            account.get_max_order_size(trading_pair, side),
            account.get_prices(futures_only=True)
        )
        max_usdc_amount = max_order_size * token_prices[token]

        if usdc_amount:
            if usdc_amount > max_usdc_amount:
                await warning(f"Backpack | Requested amount {usdc_amount:.2f} USDC exceeds max {max_usdc_amount:.2f} USDC for {token} on {account.account_id}")
                usdc_amount = max_usdc_amount

            rounded_amount = round_to_decimals(usdc_amount, self.futures_decimals[token]["tick_size"])
            payload.update({
                "quoteQuantity": f"{rounded_amount:.8f}",
                "side": side,
                "symbol": trading_pair,
                "reduceOnly": False
            })

        elif token_amount:
            if token_amount > max_order_size:
                await warning(f"Backpack | Requested amount {token_amount:.8f} {token} exceeds max {max_order_size:.8f} {token} on {account.account_id}")
                token_amount = max_order_size

            rounded_amount = round_to_decimals(token_amount, self.futures_decimals[token]["amount"])
            payload.update({
                "quantity": f"{rounded_amount:.8f}",
                "side": side,
                "symbol": trading_pair,
                "reduceOnly": False
            })

        else:
            raise Exception("One of usdc_amount or token_amount must be specified")

        order_resp = await account.create_order(payload)

        if order_resp.get("status") == "Filled":
            executed_amount = float(order_resp['executedQuantity'])
            executed_usdc = float(order_resp['executedQuoteQuantity'])
            order_price = round(executed_usdc / executed_amount, self.futures_decimals[token]["price"])

            leverage_str = '' if not leverage else f' with {leverage}x'
            await success(f"Backpack | Created {normalized_side} order for {account.account_id}{leverage_str}: {executed_amount:.5f} {token} @ {order_price} USDC")
            return True
        else:
            error_msg = order_resp.get("message", str(order_resp))
            if log_error:
                await error(f"Backpack | Order creation failed on {account.account_id} for {trading_pair}, amount {rounded_amount:.8f}: {error_msg}")

            if retry < RETRY:
                return await self.create_future_order(
                    account=account,
                    token=token,
                    side=side,
                    usdc_amount=usdc_amount,
                    token_amount=token_amount,
                    leverage=leverage,
                    retry=retry + 1,
                    log_error=log_error
                )
            raise Exception(f"Order creation failed: {error_msg}")

    async def open_positions(self, long_account: Backpack, short_accounts: List[Backpack], token: str, leverage: int = None, short_sizes: List[float] = None, long_size: float = None, main_direction: str = "long") -> bool:
        try:
            if not leverage and not short_sizes and not long_size:
                leverage = random.randint(*POSITION_SETTINGS['leverage'])
                long_size = round(random.uniform(*POSITION_SETTINGS['total_positions_size']), 2) / 2

                short_sizes = calculate_short_positions(
                    total_size=long_size,
                    num_accounts=len(short_accounts),
                )

            total_position_size = long_size * 2

            main_account = long_account
            hedge_accounts = short_accounts
            main_size = long_size
            hedge_sizes = short_sizes

            if main_direction == "long":
                main_side = "Bid"
                hedge_side = "Ask"
                main_emoji = "🟢"
                hedge_emoji = "🔴"
                main_text = "Long"
                hedge_text = "Short"
            else:
                main_side = "Ask"
                hedge_side = "Bid"
                main_emoji = "🔴"
                hedge_emoji = "🟢"
                main_text = "Short"
                hedge_text = "Long"

            hedge_accounts_info = "\n".join([
                f"{hedge_emoji} {hedge_text} {size:.2f} USDC for {acc.account_id}"
                for acc, size in zip(hedge_accounts, short_sizes)
            ])

            await info(f"""
📊 Opening new positions:
🎯 Token: {token}
💰 Total Size: {total_position_size:.2f} USDC
📈 Leverage: {leverage}x

{main_emoji} {main_text} {main_size:.2f} USDC for {main_account.account_id}

{hedge_accounts_info}
""")

            await self.create_future_order(
                account=main_account,
                token=token,
                side=main_side,
                usdc_amount=main_size,
                leverage=leverage
            )

            for account, size in zip(hedge_accounts, hedge_sizes):
                hedge_delay = round(random.uniform(*ORDERS_TIMEOUT), 2)
                await info(f"Backpack | Sleeping {hedge_delay} seconds before next {hedge_text.lower()}...", telegram=False)
                await asyncio.sleep(hedge_delay)

                await self.create_future_order(
                    account=account,
                    token=token,
                    side=hedge_side,
                    usdc_amount=size,
                    leverage=leverage
                )

            await success(f"Backpack | Successfully opened positions for {token}: {main_text.lower()} {main_size:.5f} USDC, {len(hedge_accounts)} {hedge_text.lower()}s")
            return True

        except Exception as e:
            await error(f"Backpack | Error opening positions: {e}")
            await self.close_all_positions([long_account] + short_accounts)
            return False

    async def monitor_positions(self, long_account: Backpack, short_accounts: List[Backpack], token: str):
        try:
            trading_pair = f"{token}_USDC_PERP"
            start_time = time()
            max_position_time = round(random.uniform(*POSITION_SETTINGS['max_position_time']), 2)
            pnl_limit = round(random.uniform(*POSITION_SETTINGS['max_pnl']), 2)
            account_leverage = int((await long_account.get_account_info())['leverageLimit'])

            await info(f"Backpack | Monitoring long position with PnL limit of {pnl_limit} and max position time of {max_position_time} seconds")

            while True:
                elapsed_time = time() - start_time

                positions = await long_account.get_futures_positions()
                long_position = next((pos for pos in positions if pos.get("symbol") == trading_pair), None)

                if not long_position:
                    await warning(f"Backpack | Long position not found for {trading_pair}")
                    break

                unrealized_pnl = float(long_position["pnlUnrealized"])
                realized_pnl = float(long_position["pnlRealized"])
                total_pnl = unrealized_pnl + realized_pnl
                net_exposure = abs(float(long_position["netExposureNotional"]))

                position_size = net_exposure / account_leverage
                pnl_percent = total_pnl / position_size * 100

                if sum(POSITION_SETTINGS['max_position_time']) > 0 and elapsed_time >= max_position_time:
                    await info(f"Backpack | Max hold time reached for {trading_pair}: {elapsed_time:.2f} seconds. PnL: {pnl_percent:.2f}%")
                    break

                if sum(POSITION_SETTINGS['max_pnl']) > 0 and abs(pnl_percent) >= pnl_limit:
                    await info(f"Backpack | PnL limit reached for {trading_pair}: {pnl_percent:.2f}%. Hold time: {elapsed_time:.2f} seconds")
                    break

                await asyncio.sleep(10)

            await self.close_all_positions([long_account] + short_accounts)

        except Exception as e:
            await error(f"Backpack | Error monitoring positions: {e}")
            await self.close_all_positions([long_account] + short_accounts, token=token)

    async def close_positions(self, account: Backpack, token: str = None):
        trading_pair = f"{token}_USDC_PERP" if token else None
        try:
            positions = await account.get_futures_positions()
            if not positions:
                await info(f"Backpack | No positions found on {account.account_id}")
                return 1

            if trading_pair:
                positions = [pos for pos in positions if pos.get("symbol") == trading_pair]

            for position in positions:
                symbol = position["symbol"]
                token = position["symbol"].replace('_USDC_PERP', '')
                    
                side = "Bid" if float(position["netQuantity"]) < 0 else "Ask"
                position_amount = round_to_decimals(
                    abs(float(position["netExposureQuantity"])),
                    self.futures_decimals[token]["amount"]
                )
                
                if position_amount == 0:
                    await warning(f"Backpack | Zero position amount for {symbol} on {account.account_id}")
                    continue

                await self.create_future_order(
                    account=account,
                    token=token,
                    side=side,
                    token_amount=position_amount,
                )

            return True

        except Exception as e:
            await error(f"Backpack | Error closing positions: {e}")
            return False

    async def close_all_positions(self, accounts: List[Backpack], token: str = None, log=True):
        try:
            random.shuffle(accounts)
            for i, account in enumerate(accounts):
                additional_str = f'{token} ' if token else ''
                if log:
                    await debug(f'Backpack | Closing all {additional_str}positions on {account.account_id}')
                res = await self.close_positions(account, token)

                if isinstance(res, bool) and i < len(accounts) - 1:
                    position_delay = round(random.uniform(*ORDERS_TIMEOUT), 2)
                    await info(f"Backpack | Sleeping {position_delay} seconds before closing next position...", telegram=False)
                    await asyncio.sleep(position_delay)

            await success("Backpack | Closed all positions on all accounts")
            return True

        except Exception as e:
            await error(f"Backpack | Error closing all positions: {e}")
            return False
