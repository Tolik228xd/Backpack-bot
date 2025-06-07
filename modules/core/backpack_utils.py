import random
import asyncio

from modules.helpers.logger import success, debug, warning
from modules.helpers.utils import save_accounts_statistics, get_last_thursday_timestamp, round_to_decimals
from modules.core.backpack import Backpack
from modules.core.okx import okx_withdraw


class BackpackUtils:
    ACCOUNTS_PATH = "accounts.json"

    async def parse_accounts_data(self, accounts: list[Backpack], is_parse_mode=False, log=True, sub_accounts: list[Backpack] = None):
        if log:
            await debug('Parse Statistic | Parsing accounts data...')
        prices = await accounts[0].get_prices()
        last_reset_timestamp = get_last_thursday_timestamp()

        async def process_account(account: Backpack):
            try:
                account.backpack_deposit_address = await account.get_deposit_address()
                balances = await account.get_balances()

                if sub_accounts:
                    sub_accs = [sub for sub in sub_accounts if sub.account_id.startswith(account.account_id)]
                    if sub_accs:
                        sub_account = sub_accs[0]
                        sub_balances = await sub_account.get_balances()
                        for token in sub_balances:
                            if token in balances:
                                balances[token] += sub_balances[token]
                            else:
                                balances[token] = sub_balances[token]

                total_usd_balance = round(sum([
                    balances[token] * prices[token]
                    for token in balances
                    if token in prices
                ]), 2)
                usdc_balance = round(balances.get("USDC", 0), 2)

                statistics = await account.get_account_statistics(last_reset_timestamp)

                return {
                    "api_key": account.api_key,
                    "account_id": account.account_id,
                    "deposit_address": account.backpack_deposit_address,
                    "balances": {
                        "total_usd": total_usd_balance,
                        "usdc": usdc_balance
                    },
                    "statistics": statistics
                }
            except Exception as e:
                raise Exception(f"Parse Statistic | Error processing account {account.account_id}: {e}")

        tasks = [process_account(account) for account in accounts]
        results = await asyncio.gather(*tasks)

        await save_accounts_statistics(results, self.ACCOUNTS_PATH, is_parse_mode)

        if log:
            await success('Parse Statistic | All data was successfully parsed!')

        return results

    def _filter_available_accounts_for_liquidation(self, accounts_data, account_limits, accounts: list[list[Backpack]]) -> list[list[Backpack]]:
        available_accounts = []

        for account_data in accounts_data:
            if not account_data:
                continue

            account_id = account_data["account_id"]
            backpack = [acc for acc in accounts if acc[0].account_id == account_id][0]

            limits = account_limits[backpack[0].api_key]
            volume_limit = limits["volume_limit"]
            liquidation_limit = limits["liquidation_limit"]

            if (
                    (
                            volume_limit != 0 and
                            account_data["statistics"]["volume"]['week'] >= volume_limit
                    ) or
                    (
                            liquidation_limit != 0 and
                            account_data["statistics"]["liquidations"]['week'] >= liquidation_limit
                    )
            ):
                continue

            available_accounts.append(backpack)

        return available_accounts

    async def check_and_adjust_balance(self, account: list[Backpack], required_margin: float) -> bool:
        try:
            required_margin *= 1.05

            main_account, sub_account = account
            net_equity, balances = await main_account.get_balances(balances_and_equity=True)
            net_equity = net_equity.get('USDC')

            if net_equity > required_margin and balances.get('USDC'):
                if net_equity - balances['USDC'] > required_margin:
                    excess = round_to_decimals(balances['USDC'] - random.uniform(0.001, 0.01), 5)
                else:
                    excess = round_to_decimals(balances['USDC'] - required_margin, 5)
                excess = min(round_to_decimals(await main_account.get_transferable_amount('USDC') * 0.95, 3), excess)
                if excess > 0.005:
                    await main_account.withdraw(
                        sub_account.backpack_deposit_address,
                        excess
                    )
                    await success(f"Backpack | Successfully withdrew {excess} USDC from {main_account.account_id} to {sub_account.account_id}")
                    await asyncio.sleep(3)
                    return True

            elif net_equity < required_margin:
                while net_equity < required_margin:
                    sub_balance = (await sub_account.get_balances()).get("USDC", 0)
                    
                    if sub_balance <= 0.0001:
                        break
                        
                    deficit = (required_margin - net_equity) * 1.01
                    transfer_amount = round_to_decimals(min(deficit, sub_balance), 5)

                    if transfer_amount > 0.005:
                        await sub_account.withdraw(
                            main_account.backpack_deposit_address,
                            transfer_amount
                        )
                        await success(f"Backpack | Successfully withdrew {transfer_amount} USDC from {sub_account.account_id} to {main_account.account_id}")
                        await asyncio.sleep(3)
                        net_equity = (await main_account.get_balances(net_equity=True)).get("USDC", 0)
                    else:
                        break

                if net_equity < round_to_decimals(required_margin, 2):
                    await debug(f"Backpack | Main account {main_account.account_id} has insufficient balance: {net_equity:.5f}/{required_margin:.5f} USDC")
                    await okx_withdraw(
                        main_account.backpack_deposit_address,
                        max((required_margin - net_equity) * 1.1, random.uniform(1.05, 2)),
                    )
                    for _ in range(60):
                        await asyncio.sleep(5)
                        new_balance = (await main_account.get_balances(net_equity=True)).get("USDC", 0)

                        if new_balance > net_equity:
                            await success(
                                f"Backpack | USDC deposit received on {main_account.account_id}, new balance: ${new_balance:.5f} USDC"
                            )
                            return await self.check_and_adjust_balance(account, required_margin)

            return True
        except Exception as e:
            raise Exception(f"Balance adjustment failed for {account[0].account_id}: {e}")

    async def withdraw_excess_usdc(self, account: list[Backpack], mode_run=False, k=None):
        transfer_amount = None
        try:
            main_account, sub_account = account
            if not k:
                if mode_run:
                    k = 1
                else:
                    k = 0.99
            transfer_amount = round_to_decimals(await main_account.get_transferable_amount('USDC') * k, 6)

            if transfer_amount > 0:
                await main_account.withdraw(
                    sub_account.backpack_deposit_address,
                    transfer_amount
                )
                await success(f"Backpack | Successfully withdrew all free balance ({transfer_amount}) USDC from {main_account.account_id} to {sub_account.account_id}")
                await asyncio.sleep(3)

            else:
                if mode_run:
                    await warning("Backpack | No available balance for transfer")
                return 1

            return True
        except Exception as e:
            if transfer_amount > 0 and k > 0.99:
                await warning(f"Backpack | Failed to withdraw free balance ({transfer_amount} USDC) on sub for {account[0].account_id}: {e}. Reducing amount for a bit")
                return self.withdraw_excess_usdc(account, mode_run, k=k-0.00005)
            else:
                await warning(f"Backpack | Failed to withdraw free balance ({transfer_amount} USDC) on sub for {account[0].account_id}: {e}")
            return False

    async def get_position_size(self, account: Backpack, token: str) -> float:
        positions = await account.get_futures_positions()
        for pos in positions:
            if pos["symbol"] == f"{token}_USDC_PERP":
                return abs(float(pos.get("netExposureNotional", 0)))
        return 0

    async def monitor_position_changes(
        self,
        account: Backpack,
        token: str,
    ) -> tuple[bool, float]:
        current_size = await self.get_position_size(account, token)
        is_liquidated = current_size == 0
        return is_liquidated, current_size

    async def get_all_deposit_addresses(self, account_pairs: list[list[Backpack]]):
        for pair in account_pairs:
            for account in pair:
                if not account.backpack_deposit_address:
                    account.backpack_deposit_address = await account.get_deposit_address()

    async def close_borrow(self, account: list[Backpack]):
        try:
            main_account, sub_account = account
            borrow_amount = await main_account.get_borrow_amount() + 1
            if borrow_amount > 0:
                await sub_account.withdraw(
                    main_account.backpack_deposit_address,
                    borrow_amount
                )
                await success(f"Backpack | Successfully withdrew {borrow_amount} USDC from {sub_account.account_id} to {main_account.account_id} and covered all borrows")
                await asyncio.sleep(3)
            else:
                await warning(f"Backpack | No borrows found on {main_account.account_id}")
            return True
        except Exception as e:
            await warning(f"Backpack | Failed to cover borrows on {account[0].account_id}: {e}")
            return False
