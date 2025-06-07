import asyncio
import random
import time
from typing import TypedDict
from dataclasses import dataclass

from modules.core.backpack import Backpack
from modules.core.position_manager import PositionManager
from modules.core.backpack_utils import BackpackUtils
from modules.helpers.logger import error, info, warning
from modules.helpers.utils import calculate_short_positions
from settings import DELTA_NEUTRAL_SETTINGS


class InitialStates(TypedDict):
    main: float
    hedge: dict[str, float]
    hedge_sizes: list[float]


@dataclass
class PairData:
    main_account: list[Backpack]
    hedge_accounts: list[list[Backpack]]
    token: str
    initial_states: InitialStates
    log_prefix: str
    main_direction: str


class DeltaNeutralLiquidation(BackpackUtils):
    LEVERAGE = 50
    PAIR_STATE_ACTIVE = "active"
    PAIR_STATE_PARTIAL_LIQUIDATION = "partial_liquidation"
    PAIR_STATE_CLOSED = "closed"

    def __init__(self, accounts: list[list[Backpack]], position_manager: PositionManager, account_limits):
        self.accounts = accounts
        self.position_manager = position_manager
        self.account_limits = account_limits
        self.futures_decimals = {}
        self.accounts_lock = asyncio.Lock()

    async def _select_accounts(self, accounts_needed: int) -> tuple[list[Backpack] | None, list[list[Backpack]]]:
        async with self.accounts_lock:
            unique_accounts = []
            for acc in self.accounts:
                if not [i for i in unique_accounts if i[0].account_id == acc[0].account_id]:
                    unique_accounts.append(acc)
            self.accounts = unique_accounts

            if len(self.accounts) < accounts_needed:
                return None, []

            accounts_data = await self.parse_accounts_data(
                [acc[0] for acc in self.accounts],
                log=False,
                sub_accounts=[acc[1] for acc in self.accounts]
            )
            available_accounts = self._filter_available_accounts_for_liquidation(
                accounts_data,
                self.account_limits,
                self.accounts
            )

            if len(available_accounts) < accounts_needed:
                return None, []

            long_account = random.choice(available_accounts)
            available_accounts.remove(long_account)
            short_accounts = random.sample(available_accounts, accounts_needed - 1)

            for account in [long_account] + short_accounts:
                self.accounts.remove(account)

            return long_account, short_accounts

    async def _handle_partial_liquidation(
        self,
        pair_data: PairData,
        partial_info: dict
    ) -> bool:
        if time.time() - partial_info["start_time"] > DELTA_NEUTRAL_SETTINGS['partial_liquidation_timeout'] * 60:
            await warning(f"{pair_data.log_prefix} | Closing pair due to partial liquidation timeout on {partial_info['account_id']}")
            await self.position_manager.close_all_positions(
                [pair_data.main_account[0]] + [acc[0] for acc in pair_data.hedge_accounts],
                log=False,
            )
            return True

        is_main = partial_info["account_id"] == pair_data.main_account[0].account_id
        account = pair_data.main_account if is_main else next(
            acc for acc in pair_data.hedge_accounts
            if acc[0].account_id == partial_info["account_id"]
        )

        current_size = await self.get_position_size(account[0], pair_data.token)
        if current_size == 0:
            if is_main:
                return await self.handle_main_liquidation(pair_data)
            else:
                return await self.handle_hedge_liquidation(pair_data, account)
        return False

    async def handle_main_liquidation(self, pair_data: PairData):
        try:
            await info(f"{pair_data.log_prefix} | Main {pair_data.main_direction} position liquidated on {pair_data.main_account[0].account_id}, closing all hedges")
            await self.position_manager.close_all_positions(
                [pair_data.main_account[0]] + [acc[0] for acc in pair_data.hedge_accounts],
                log=False
            )
            async with self.accounts_lock:
                self.accounts.append(pair_data.main_account)
                for hedge in pair_data.hedge_accounts:
                    self.accounts.append(hedge)
            return True
        except Exception as e:
            raise Exception(f"Error handling main position liquidation: {e}")

    async def handle_hedge_liquidation(self, pair_data: PairData, liquidated_account: list[Backpack]) -> bool:
        try:
            await info(f"{pair_data.log_prefix} | Hedge position liquidated on {liquidated_account[0].account_id}, adjusting position")
            liquidated_size = next(
                size for acc, size in zip(pair_data.hedge_accounts, pair_data.initial_states['hedge_sizes'])
                if acc[0].account_id == liquidated_account[0].account_id
            )

            side = "Ask" if pair_data.main_direction == "long" else "Bid"
            await self.position_manager.create_future_order(
                account=pair_data.main_account[0],
                token=pair_data.token,
                side=side,
                usdc_amount=liquidated_size,
                leverage=self.LEVERAGE
            )
            
            pair_data.initial_states["main"] = await self.get_position_size(
                pair_data.main_account[0],
                pair_data.token
            )
            pair_data.hedge_accounts.remove(liquidated_account)
            pair_data.initial_states['hedge_sizes'].remove(liquidated_size)
            
            async with self.accounts_lock:
                self.accounts.append(liquidated_account)
                
            if not pair_data.hedge_accounts:
                await info(f"{pair_data.log_prefix} | All hedge positions liquidated, closing pair...")
                await self.position_manager.close_all_positions(
                    [pair_data.main_account[0]],
                    log=False,
                )
                async with self.accounts_lock:
                    self.accounts.append(pair_data.main_account)
                return True
            return False
        except Exception as e:
            raise Exception(f"Error handling hedge liquidation: {e}")

    async def run_single_pair(self, log_prefix: str) -> bool:
        selected_accounts = []
        try:
            accounts_in_pair = random.randint(*DELTA_NEUTRAL_SETTINGS['accounts_in_pair'])
            main_account, hedge_accounts = await self._select_accounts(accounts_in_pair)
            if not main_account:
                await warning(f'{log_prefix} | Not enough accounts for launching a thread')
                return False

            selected_accounts = [main_account] + hedge_accounts
            token = random.choice(DELTA_NEUTRAL_SETTINGS['tokens'])
            main_size = random.uniform(*DELTA_NEUTRAL_SETTINGS['long_size']) * self.LEVERAGE
            hedge_sizes = calculate_short_positions(
                total_size=main_size,
                num_accounts=len(hedge_accounts),
                variation=random.uniform(*DELTA_NEUTRAL_SETTINGS['size_variation'])
            )

            main_direction = random.choice(DELTA_NEUTRAL_SETTINGS['main_direction'])
            
            for account, size in zip(selected_accounts, [main_size] + hedge_sizes):
                if not await self.check_and_adjust_balance(account, size / self.LEVERAGE):
                    raise Exception(f"{log_prefix} | Failed to adjust balance for {account[0].account_id}")

            if not await self.position_manager.open_positions(
                main_account[0],
                [acc[0] for acc in hedge_accounts],
                token,
                leverage=self.LEVERAGE,
                short_sizes=hedge_sizes,
                long_size=main_size,
                main_direction=main_direction
            ):
                raise Exception(f"{log_prefix} | Failed to open positions")

            initial_states = {
                "main": await self.get_position_size(main_account[0], token),
                "hedge": {acc[0].account_id: await self.get_position_size(acc[0], token) for acc in hedge_accounts},
                "hedge_sizes": hedge_sizes,
            }

            pair_data = PairData(
                main_account=main_account,
                hedge_accounts=hedge_accounts,
                token=token,
                initial_states=initial_states,
                log_prefix=log_prefix,
                main_direction=main_direction
            )

            await info(f"{log_prefix} | Monitoring liquidations for pair with {main_account[0].account_id} (main {main_direction})")
            state = self.PAIR_STATE_ACTIVE
            partial_liquidation = None

            while state != self.PAIR_STATE_CLOSED:
                await asyncio.sleep(8)

                if state == self.PAIR_STATE_PARTIAL_LIQUIDATION:
                    if await self._handle_partial_liquidation(pair_data, partial_liquidation):
                        break
                    continue

                is_liquidated, current_size = await self.monitor_position_changes(
                    pair_data.main_account[0],
                    pair_data.token
                )

                if is_liquidated:
                    await self.handle_main_liquidation(pair_data)
                    break
                elif current_size < pair_data.initial_states["main"] * 0.99:
                    state = self.PAIR_STATE_PARTIAL_LIQUIDATION
                    partial_liquidation = {
                        "account_id": pair_data.main_account[0].account_id,
                        "start_time": time.time(),
                        "initial_size": pair_data.initial_states["main"]
                    }
                    continue

                for hedge_account in pair_data.hedge_accounts[:]:
                    is_liquidated, current_size = await self.monitor_position_changes(
                        hedge_account[0],
                        pair_data.token
                    )
                    if is_liquidated:
                        if await self.handle_hedge_liquidation(pair_data, hedge_account):
                            state = self.PAIR_STATE_CLOSED
                            break
                    elif current_size < pair_data.initial_states["hedge"][hedge_account[0].account_id] * 0.99:
                        state = self.PAIR_STATE_PARTIAL_LIQUIDATION
                        partial_liquidation = {
                            "account_id": hedge_account[0].account_id,
                            "start_time": time.time(),
                            "initial_size": pair_data.initial_states["hedge"][hedge_account[0].account_id]
                        }
                        break
            return True
        except Exception as e:
            await error(f"{log_prefix} | Error in pair trading: {e}")
            if selected_accounts:
                async with self.accounts_lock:
                    for acc in selected_accounts:
                        self.accounts.append(acc)
            if 'pair_data' in locals():
                await self.position_manager.close_all_positions(
                    [pair_data.main_account[0]] + [acc[0] for acc in pair_data.hedge_accounts],
                    log=False
                )
            return False

    async def start_liquidation_trading(self):
        try:
            self.futures_decimals = await self.accounts[0][0].get_token_decimals()
            self.position_manager.futures_decimals = self.futures_decimals
            await self.get_all_deposit_addresses(self.accounts)

            while True:
                num_parallel_pairs = random.randint(*DELTA_NEUTRAL_SETTINGS.get('parallel_pairs', [1, 1]))
                await info(f"Backpack | Starting {num_parallel_pairs} parallel delta neutral pairs")
                tasks = []
                for i in range(num_parallel_pairs):
                    log_prefix = f"Thread-{i+1}"
                    task = asyncio.create_task(self.run_single_pair(log_prefix))
                    tasks.append(task)
                results = await asyncio.gather(*tasks, return_exceptions=True)
                if not any(results) or not self.accounts:
                    await info("No more accounts available for trading")
                    break
                await asyncio.sleep(60)
        except Exception as e:
            await error(f"Error in liquidation trading: {e}")
            await self.position_manager.close_all_positions(
                [acc[0] for acc in self.accounts]
            )

