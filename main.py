import asyncio

from modules.helpers.utils import choose_mode
from modules.core.trading_manager import TradingManager


async def run_mode(mode: str):
    manager = TradingManager()
    if mode == "futures_trading":
        await manager.start_trading()
    elif mode == "close_positions":
        await manager.close_all_positions()
    elif mode == "parse_accounts_data":
        await manager.parse_accounts_data(manager.accounts, True)
    elif mode == "delta_neutral_liquidations":
        await manager.run_delta_neutral_liquidations()
    elif mode == "default_liquidations":
        await manager.run_default_liquidations()
    elif mode == "withdraw_all_balances":
        await manager.withdraw_all_balances()


if __name__ == '__main__':
    selected_mode = choose_mode()
    if selected_mode:
        asyncio.run(run_mode(selected_mode))
