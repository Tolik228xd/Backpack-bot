import sys
import json
import asyncio
from typing import List
import random

from modules.core.backpack import Backpack
from modules.core.position_manager import PositionManager
from modules.core.delta_neutral_liquidation import DeltaNeutralLiquidation
from modules.core.default_liquidations import DefaultLiquidation
from modules.core.backpack_utils import BackpackUtils
from modules.helpers.logger import info, error
from modules.helpers.utils import get_account_limits
from settings import POSITION_SETTINGS, POSITIONS_TIMEOUT, ORDERS_TIMEOUT


class TradingManager(BackpackUtils):
    ACCOUNTS_PATH = "accounts.json"

    def __init__(self):
        self.accounts = self._load_accounts()
        self.account_limits = get_account_limits([acc.api_key for acc in self.accounts])
        self.position_manager = PositionManager()
        if not self.accounts:
            sys.exit('No accounts to process')

        self.futures_decimals = {}

    def _load_accounts(self, load_sub_accounts: bool = False) -> List[Backpack] | List[List[Backpack]]:
        try:
            with open(self.ACCOUNTS_PATH, "r") as f:
                accounts_data = json.load(f)

            if not load_sub_accounts:
                backpack_accounts = []
                for acc_id, acc_data in accounts_data.items():
                    backpack = Backpack(
                        account_id=acc_id,
                        api_key=acc_data["backpack_api"],
                        api_secret=acc_data["backpack_secret"],
                        proxy=acc_data["proxy"] if acc_data["proxy"] and acc_data["proxy"] != 'ip:port:login:pass' else None,
                        backpack_deposit_address=acc_data.get("backpack_deposit_address")
                    )
                    backpack_accounts.append(backpack)
                return backpack_accounts
            else:
                backpack_account_pairs = []
                for acc_id, acc_data in accounts_data.items():
                    main_account = Backpack(
                        account_id=acc_id,
                        api_key=acc_data["backpack_api"],
                        api_secret=acc_data["backpack_secret"],
                        proxy=acc_data["proxy"] if acc_data["proxy"] and acc_data["proxy"] != 'ip:port:login:pass' else None,
                        backpack_deposit_address=acc_data.get("backpack_deposit_address")
                    )
                    
                    sub_account = Backpack(
                        account_id=f"{acc_id}_sub",
                        api_key=acc_data["backpack_sub-account_api"],
                        api_secret=acc_data["backpack_sub-account_secret"],
                        proxy=acc_data["proxy"] if acc_data["proxy"] and acc_data["proxy"] != 'ip:port:login:pass' else None,
                        backpack_deposit_address=None
                    )
                    
                    backpack_account_pairs.append([main_account, sub_account])
                return backpack_account_pairs

        except Exception as e:
            raise Exception(f"Error loading accounts: {e}")

    async def start_trading(self):
        self.futures_decimals = await self.accounts[0].get_token_decimals()
        self.position_manager.futures_decimals = self.futures_decimals
        try:
            while True:
                await info("Starting new trading cycle...")

                accounts_data = await self.parse_accounts_data(self.accounts)
                available_accounts = self._filter_available_accounts(accounts_data)

                num_accounts = random.randint(*POSITION_SETTINGS['accounts_in_pair'])

                if not available_accounts:
                    await error("Backpack | No accounts available for trading. All accounts have reached their limits or no accounts with enough USDC balance.")
                    sys.exit()
                elif len(available_accounts) < num_accounts:
                    available_ids = [account.account_id for account in available_accounts]
                    await error(
                        f"Backpack | Not enough accounts available for trading. [{len(available_accounts)}/{num_accounts}] Available only: " + ', '.join(
                            available_ids))
                    sys.exit()

                selected_accounts = self._select_random_accounts(available_accounts, num_accounts)

                if not selected_accounts:
                    await error("Backpack | Failed to select accounts for trading.")
                    sys.exit()

                long_account = selected_accounts[0]
                short_accounts = selected_accounts[1:]

                token = random.choice(POSITION_SETTINGS["tokens"])

                position_opened = await self.position_manager.open_positions(long_account, short_accounts, token)

                if not position_opened:
                    await error("Backpack | Failed to open positions.")
                    await asyncio.sleep(60)
                    continue

                await self.position_manager.monitor_positions(long_account, short_accounts, token)

                sleep_time = round(random.uniform(*POSITIONS_TIMEOUT), 2)
                await info(f"Backpack | Sleeping {sleep_time} seconds before next trading cycle...", telegram=False)
                await asyncio.sleep(sleep_time)

        except Exception as e:
            await error(f"Error in trading cycle: {e}")
            await self.close_all_positions()

    def _filter_available_accounts(self, accounts_data) -> List[Backpack]:
        available_accounts = []
        minimum_usdc_balance = POSITION_SETTINGS["total_positions_size"][1] / POSITION_SETTINGS["leverage"][0] / 2 * 1.1

        for account_data in accounts_data:
            if not account_data:
                continue

            account_id = account_data["account_id"]
            backpack = next((acc for acc in self.accounts if acc.account_id == account_id), None)

            limits = self.account_limits[backpack.api_key]
            volume_limit = limits["volume_limit"]
            pnl_limit = limits["pnl_limit"]

            if (
                    (
                        volume_limit != 0 and
                        account_data["statistics"]["volume"]['week'] >= volume_limit
                    ) or
                    (
                        pnl_limit != 0 and
                        account_data["statistics"]["pnl"]['week'] >= pnl_limit
                    )
            ):
                continue

            if account_data["balances"]["usdc"] < minimum_usdc_balance:
                continue

            available_accounts.append(backpack)

        return available_accounts

    def _select_random_accounts(self, available_accounts: List[Backpack], num_accounts: int) -> List[Backpack]:
        if len(available_accounts) < num_accounts:
            return []
        random.seed()
        return random.sample(available_accounts, num_accounts)

    async def close_all_positions(self):
        self.futures_decimals = await self.accounts[0].get_token_decimals()
        self.position_manager.futures_decimals = self.futures_decimals
        await self.position_manager.close_all_positions(self.accounts)
    
    async def run_delta_neutral_liquidations(self):
        self.accounts = self._load_accounts(load_sub_accounts=True)
        delta_neutral_liquidation = DeltaNeutralLiquidation(self.accounts, self.position_manager, self.account_limits)
        await delta_neutral_liquidation.start_liquidation_trading()

    async def run_default_liquidations(self):
        self.accounts = self._load_accounts(load_sub_accounts=True)
        default_liquidation = DefaultLiquidation(self.accounts, self.position_manager, self.account_limits)
        await default_liquidation.start_liquidation_trading()

    async def withdraw_all_balances(self):
        self.accounts = self._load_accounts(load_sub_accounts=True)
        await self.get_all_deposit_addresses(self.accounts)
        random.shuffle(self.accounts)
        await info("Backpack | Starting withdrawal of all USDC balances...")
        
        for account_pair in self.accounts:
            await info(f"Backpack | Processing account {account_pair[0].account_id}...")
            try:
                res = await self.withdraw_excess_usdc(account_pair, mode_run=True)
            except Exception as e:
                await error(f"Backpack | Failed to process {account_pair[0].account_id}: {e}")
                continue
            if res is True and account_pair != self.accounts[-1]:
                position_delay = round(random.uniform(*ORDERS_TIMEOUT), 2)
                await info(f"Backpack | Sleeping {position_delay} seconds before transferring on the next accounts...", telegram=False)
                await asyncio.sleep(position_delay)
        
        await info("Backpack | Finished processing all accounts")
