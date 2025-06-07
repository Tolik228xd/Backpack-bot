import asyncio
import random
import time
from typing import TypedDict
from dataclasses import dataclass
from modules.core.backpack import Backpack
from modules.core.position_manager import PositionManager
from modules.core.backpack_utils import BackpackUtils
from modules.helpers.logger import error, info, warning, debug
from settings import DEFAULT_LIQUIDATION_SETTINGS, ORDERS_TIMEOUT, RETRY


class AccountState(TypedDict):
    direction: str
    tokens: list[str]
    last_reinvest_pnl: dict[str, float]


@dataclass
class AccountData:
    account: list[Backpack]
    state: AccountState
    log_prefix: str


class DefaultLiquidation(BackpackUtils):
    LEVERAGE = 50
    CACHE_LIFETIME = 600

    def __init__(self, accounts: list[list[Backpack]], position_manager: PositionManager, account_limits):
        self.accounts = accounts
        self.position_manager = position_manager
        self.account_limits = account_limits
        self.futures_decimals = {}
        self.accounts_lock = asyncio.Lock()
        self.active_accounts: dict[str, AccountData] = {}
        self._cached_parse_data = None
        self._cache_timestamp = 0

    async def _get_available_accounts(self) -> list[list[Backpack]]:
        current_time = time.time()
        
        need_update = False
        
        if not self._cached_parse_data or current_time - self._cache_timestamp >= self.CACHE_LIFETIME:
            need_update = True
        else:
            cached_account_ids = {data["account_id"] for data in self._cached_parse_data}
            current_account_ids = {acc[0].account_id for acc in self.accounts}
            
            if not current_account_ids.issubset(cached_account_ids):
                need_update = True
        
        if need_update:
            self._cached_parse_data = await self.parse_accounts_data(
                [acc[0] for acc in self.accounts],
                log=False,
                sub_accounts=[acc[1] for acc in self.accounts]
            )
            self._cache_timestamp = current_time
        
        current_accounts_data = [
            data for data in self._cached_parse_data 
            if any(acc[0].account_id == data["account_id"] for acc in self.accounts)
        ]
        
        available_accounts = self._filter_available_accounts_for_liquidation(
            current_accounts_data,
            self.account_limits,
            self.accounts
        )
        
        return available_accounts

    async def _select_account(self) -> list[Backpack] | None:
        async with self.accounts_lock:
            if not self.accounts:
                await warning("Backpack | No more accounts available for trading")
                return None

            available_accounts = await self._get_available_accounts()
            if not available_accounts:
                await warning("Backpack | No more accounts available for trading")
                return None

            selected_account = random.choice(available_accounts)
            self.accounts.remove(selected_account)
            return selected_account

    async def _try_open_position(
        self,
        account_data: AccountData,
        available_tokens: list[str] = None,
        size: float = None,
    ) -> tuple[bool, str | None]:
        if not available_tokens:
            available_tokens = [t for t in DEFAULT_LIQUIDATION_SETTINGS["tokens"]
                                if t not in account_data.state["tokens"]]

        retries = RETRY

        while retries > 0 and available_tokens:
            token = random.choice(available_tokens)
            available_tokens.remove(token)

            try:
                if size is None:
                    size = random.uniform(*DEFAULT_LIQUIDATION_SETTINGS["position_size"]) * self.LEVERAGE

                if not await self.check_and_adjust_balance(account_data.account, size / self.LEVERAGE):
                    await warning(f"{account_data.log_prefix} | Failed to adjust balance for {token}")
                    continue

                await self.position_manager.create_future_order(
                    account=account_data.account[0],
                    token=token,
                    side="Bid" if account_data.state["direction"] == "long" else "Ask",
                    usdc_amount=size,
                    leverage=self.LEVERAGE,
                    log_error=False
                )

                account_data.state["tokens"].append(token)
                return True, token

            except Exception as e:
                await warning(f"{account_data.log_prefix} | Failed to open position for {token}: {e}")

                if 'Account is currently being liquidated' in str(e):
                    await self.close_borrow(account_data.account)
                    available_tokens = [t for t in DEFAULT_LIQUIDATION_SETTINGS["tokens"]
                                        if t not in account_data.state["tokens"]]
                    retries -= 1
                    continue
                
                if not available_tokens and retries > 1:
                    available_tokens = [t for t in DEFAULT_LIQUIDATION_SETTINGS["tokens"]
                                        if t not in account_data.state["tokens"]]
                    retries -= 1
                    await info(f"{account_data.log_prefix} | Retrying with remaining tokens, attempts left: {retries}")

        return False, None

    async def _initialize_positions(self, account_data: AccountData) -> bool:
        try:
            num_positions = random.randint(*DEFAULT_LIQUIDATION_SETTINGS["position_number"])
            direction = random.choice(["long", "short"])
            account_data.state["direction"] = direction

            await info(f"{account_data.log_prefix} | Initializing {num_positions} {direction} positions")
            positions_opened = 0

            while positions_opened < num_positions:
                success, token = await self._try_open_position(account_data)

                if success:
                    positions_opened += 1
                    if positions_opened < num_positions:
                        delay = round(random.uniform(*ORDERS_TIMEOUT), 2)
                        await info(f"{account_data.log_prefix} | Sleeping {delay}s before next position...")
                        await asyncio.sleep(delay)
                else:
                    break

            if not account_data.state["tokens"]:
                raise Exception("Failed to open any positions")

            await info(f"{account_data.log_prefix} | Successfully opened {len(account_data.state['tokens'])} positions")
            return True

        except Exception as e:
            await error(f"{account_data.log_prefix} | Failed to initialize positions: {e}")
            await self.position_manager.close_all_positions([account_data.account[0]], log=False)
            return False

    async def _check_account_limits(self, account: list[Backpack]) -> bool:
        accounts_data = await self.parse_accounts_data(
            accounts=[account[0]],
            log=False,
            sub_accounts=[account[1]]
        )
        if not self._filter_available_accounts_for_liquidation(
                accounts_data,
                self.account_limits,
                [account]
        ):
            return False
        return True

    async def _handle_liquidation(self, account_data: AccountData, liquidated_token: str, log=True) -> bool:
        try:
            if log:
                await debug(f"{account_data.log_prefix} | Handling liquidation on {liquidated_token}")

            account_data.state["tokens"].remove(liquidated_token)
            
            if not await self._check_account_limits(account_data.account):
                await warning(f"{account_data.log_prefix} | Account limits exceeded or not enough funds, opening new position skipped...")
                return len(account_data.state["tokens"]) > 0

            available_tokens = [t for t in DEFAULT_LIQUIDATION_SETTINGS["tokens"]
                                if t not in account_data.state["tokens"]]
            if not available_tokens:
                await warning(f"{account_data.log_prefix} | No more tokens available for trading")
                return bool(account_data.state["tokens"])

            success, _ = await self._try_open_position(account_data)
            return success or len(account_data.state["tokens"]) > 0

        except Exception as e:
            await error(f"{account_data.log_prefix} | Failed to handle liquidation: {e}")
            return False

    async def reinvest_profit(self, account_data: AccountData, token: str,
                              reinvest_size: float, current_pnl: float) -> bool:
        try:
            await debug(f"{account_data.log_prefix} | Reinvesting {reinvest_size:.4f} USDC on {token}")

            await self.position_manager.create_future_order(
                account=account_data.account[0],
                token=token,
                side="Bid" if account_data.state["direction"] == "long" else "Ask",
                usdc_amount=reinvest_size * self.position_manager.get_token_leverage(token),
                leverage=self.LEVERAGE,
                log_error=False
            )

            account_data.state["last_reinvest_pnl"][token] = current_pnl
            return True

        except Exception as e:
            await warning(f"{account_data.log_prefix} | Failed to reinvest profit: {e}. Trying to withdraw excess USDC...")
            withdraw_success = await self.withdraw_excess_usdc(account_data.account)
            if withdraw_success:
                account_data.state["last_reinvest_pnl"][token] = current_pnl
            return withdraw_success

    async def manage_account(self, account: list[Backpack], log_prefix: str) -> None:
        try:
            account_data = AccountData(
                account=account,
                state={
                    "direction": "",
                    "tokens": [],
                    "last_reinvest_pnl": {}
                },
                log_prefix=log_prefix
            )

            if not await self._initialize_positions(account_data):
                raise Exception("Failed to initialize positions")

            self.active_accounts[account[0].account_id] = account_data
            await info(f"{log_prefix} | Started monitoring {len(account_data.state['tokens'])} positions")

            while account_data.state["tokens"]:
                positions = await account[0].get_futures_positions()
                current_tokens = {pos["symbol"].split("_")[0]: pos for pos in positions}

                for token in account_data.state["tokens"]:
                    if token not in current_tokens:
                        if not await self._handle_liquidation(account_data, token):
                            return

                for token, position in current_tokens.items():
                    if token not in account_data.state["tokens"]:
                        continue

                    total_pnl = float(position["pnlUnrealized"]) + float(position["pnlRealized"])
                    position_value = abs(float(position["netExposureNotional"]))
                    leverage = self.position_manager.get_token_leverage(token)
                    current_pnl = total_pnl / position_value * 100 * leverage

                    reopen_threshold = DEFAULT_LIQUIDATION_SETTINGS["reopen_pnl_threshold"]
                    if reopen_threshold != 0 and current_pnl >= reopen_threshold:
                        await info(f"{log_prefix} | Position {token} reached reopen threshold ({current_pnl:.2f}%), reopening...")
                        
                        await self.position_manager.close_positions(account[0], token=token)
                        
                        if not await self._handle_liquidation(account_data, token, log=False):
                            return
                        continue

                    reinvest_threshold = DEFAULT_LIQUIDATION_SETTINGS["reinvest_pnl_threshold"]
                    last_pnl = account_data.state["last_reinvest_pnl"].get(token, 0)
                    if (
                            reinvest_threshold != 0 and
                            current_pnl >= reinvest_threshold and
                            current_pnl >= last_pnl + reinvest_threshold
                    ):
                        new_profit_percent = current_pnl - last_pnl
                        new_profit_usdc = (new_profit_percent / (100 * leverage)) * position_value
                        await self.reinvest_profit(account_data, token, new_profit_usdc, current_pnl)

                await asyncio.sleep(5)

        except Exception as e:
            await error(f"{log_prefix} | Account management error: {e}")
        finally:
            if account[0].account_id in self.active_accounts:
                del self.active_accounts[account[0].account_id]
            await self.position_manager.close_all_positions([account[0]])
            async with self.accounts_lock:
                if account not in self.accounts:
                    self.accounts.append(account)

    async def _close_all_active_positions(self):
        for account_data in self.active_accounts.values():
            await self.position_manager.close_all_positions([account_data.account[0]], log=False)

    async def start_liquidation_trading(self):
        active_tasks: set[asyncio.Task] = set()
        
        try:
            self.futures_decimals = await self.accounts[0][0].get_token_decimals()
            self.position_manager.futures_decimals = self.futures_decimals
            await self.get_all_deposit_addresses(self.accounts)
            
            num_parallel = random.randint(*DEFAULT_LIQUIDATION_SETTINGS["number_of_parallel_accounts"])
            await info(f"Backpack | Starting {num_parallel} parallel accounts")
            
            async def start_new_task() -> bool:
                account = await self._select_account()
                if not account:
                    return False

                task = asyncio.create_task(self.manage_account(account, account[0].account_id))
                task.add_done_callback(active_tasks.discard)
                active_tasks.add(task)
                
                await asyncio.sleep(random.uniform(*DEFAULT_LIQUIDATION_SETTINGS["account_delay"]))
                return True

            for _ in range(num_parallel):
                if not await start_new_task():
                    break

            while active_tasks:
                while len(active_tasks) < num_parallel:
                    if not await start_new_task():
                        break

                try:
                    done, pending = await asyncio.wait(
                        active_tasks,
                        return_when=asyncio.FIRST_COMPLETED,
                        timeout=60
                    )
                    
                    if not done and not pending:
                        if not await self._select_account():
                            await warning("Backpack | No more accounts available for trading")
                            break
                    
                    for task in done:
                        try:
                            await task
                        except Exception as e:
                            await error(f"Backpack | Task error: {e}")
                    
                except asyncio.CancelledError:
                    await info("Liquidation trading cancelled, closing positions...")
                    break

        except Exception as e:
            raise Exception(f"Critical error in liquidation trading: {e}")
        finally:
            for task in active_tasks:
                task.cancel()
            
            if active_tasks:
                try:
                    await asyncio.wait_for(asyncio.wait(active_tasks), timeout=30.0)
                except asyncio.TimeoutError:
                    await warning("Shutdown timeout after 30s")
                
            await self._close_all_active_positions()
