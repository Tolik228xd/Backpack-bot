from questionary import select, Choice
from modules.data.constants import QUESTIONARY_STYLE
from sys import exit
import os
import json
import random
from datetime import datetime, timedelta, timezone
from time import time
from settings import ACCOUNT_TARGET_METRICS, POSITION_SETTINGS
from decimal import Decimal
import csv


def request_proxy_format(proxy):
    if not proxy: return None
    try:
        proxy_list = proxy.split(':')
        return {
            'http': f'http://{proxy_list[2]}:{proxy_list[3]}@{proxy_list[0]}:{proxy_list[1]}',
            'https': f'http://{proxy_list[2]}:{proxy_list[3]}@{proxy_list[0]}:{proxy_list[1]}',
        }
    except Exception:
        return None


def choose_mode() -> str:
    action = select(
        "Select an action to perform:",
        choices=[
            Choice(f"📈 Run Futures Trading", 'futures_trading'),
            Choice(f"📊️ Run Delta Neutral Liquidations", 'delta_neutral_liquidations'),
            Choice(f"🎰 Run Default Liquidations", 'default_liquidations'),
            Choice(f"🧨 Close All Positions", 'close_positions'),
            Choice(f"📔 Parse Accounts Data", "parse_accounts_data"),
            Choice(f"💸 Withdraw All Balances on Main Accounts", "withdraw_all_balances"),
            Choice(f"❌ Exit", 'exit'),
        ],
        style=QUESTIONARY_STYLE,
        pointer='>>>'
    ).ask()

    if not action or action == 'exit':
        exit()
    return action


def get_last_thursday_timestamp() -> int:
    now = datetime.now(timezone.utc)
    days_since_thursday = (now.weekday() - 3) % 7
    
    if days_since_thursday == 0 and now.hour == 0 and now.minute == 0 and now.second == 0:
        days_since_thursday = 7
    
    last_thursday = now - timedelta(
        days=days_since_thursday,
        hours=now.hour,
        minutes=now.minute,
        seconds=now.second,
    )
    
    return int(last_thursday.timestamp())


def get_account_limits(api_keys: list) -> dict:
    os.makedirs("database", exist_ok=True)
    limits_file = "database/account_limits.json"

    current_limits = {
        "timestamp": int(time()),
        "accounts": {}
    }

    needs_update = False
    
    if os.path.exists(limits_file):
        with open(limits_file, "r") as f:
            saved_limits = json.load(f)
            if saved_limits.get("timestamp", 0) >= get_last_thursday_timestamp():
                current_limits = saved_limits
            else:
                needs_update = True
                current_limits["accounts"] = saved_limits.get("accounts", {})

    for api_key in api_keys:
        if api_key not in current_limits["accounts"] or needs_update:
            current_limits["accounts"][api_key] = {
                "volume_limit": round(random.uniform(*ACCOUNT_TARGET_METRICS['volume']), 2),
                "pnl_limit": round(random.uniform(*ACCOUNT_TARGET_METRICS['pnl']), 2),
                "liquidation_limit": round(random.randint(*ACCOUNT_TARGET_METRICS['liquidations_count']), 2)
            }
        elif "liquidation_limit" not in current_limits["accounts"][api_key]:
            current_limits["accounts"][api_key]["liquidation_limit"] = round(random.uniform(*ACCOUNT_TARGET_METRICS['liquidations_count']), 2)
            needs_update = True

    if needs_update or not os.path.exists(limits_file):
        current_limits["timestamp"] = int(time())
        with open(limits_file, "w") as f:
            json.dump(current_limits, f, indent=4)

    return {
        api_key: current_limits["accounts"][api_key]
        for api_key in api_keys
    }


async def save_accounts_statistics(accounts_data, accounts_file_path: str, is_parse_mode: bool = False):
    os.makedirs("database", exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    if accounts_file_path:
        with open(accounts_file_path, "r") as f:
            accounts_json = json.load(f)

        modified = False
        for account in accounts_data:
            if account:
                acc_id = account["account_id"]
                accounts_json[acc_id]["backpack_deposit_address"] = account["deposit_address"]
                modified = True

        if modified:
            with open(accounts_file_path, "w") as f:
                json.dump(accounts_json, f, indent=4)

    if is_parse_mode:
        csv_file = f"database/account_stats_{timestamp}.csv"
        with open(csv_file, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                "api_key",
                "id",
                "balance_usdc",
                "balance_total",
                "pnl_week",
                "pnl_month",
                "volume_week",
                "volume_month",
                "liquidations_week",
                "liquidations_month"
            ])
            
            for account in accounts_data:
                if account:
                    writer.writerow([
                        account.get("api_key", ""),
                        account.get("account_id", ""),
                        account.get("balances", {}).get("usdc", 0),
                        account.get("balances", {}).get("total_usd", 0),
                        account.get("statistics", {}).get("pnl", {}).get("week", 0),
                        account.get("statistics", {}).get("pnl", {}).get("month", 0),
                        account.get("statistics", {}).get("volume", {}).get("week", 0),
                        account.get("statistics", {}).get("volume", {}).get("month", 0),
                        account.get("statistics", {}).get("liquidations", {}).get("week", 0),
                        account.get("statistics", {}).get("liquidations", {}).get("month", 0)
                    ])
    else:
        json_file = "database/account_stats.json"
        current_data = {}
        
        if os.path.exists(json_file):
            with open(json_file, "r") as f:
                current_data = json.load(f)
        
        current_data.update({
            "last_update": timestamp,
            "accounts": [acc for acc in accounts_data if acc is not None]
        })
        
        with open(json_file, "w") as f:
            json.dump(current_data, f, indent=4)


def calculate_short_positions(total_size: float, num_accounts: int, variation: float = None) -> list:
    try:
        if not variation:
            variation = random.uniform(*POSITION_SETTINGS['size_variation'])

        base_size = total_size / num_accounts

        variations = [random.uniform(-variation, variation) for _ in range(num_accounts)]

        sizes = [base_size * (1 + v) for v in variations]

        total_diff = total_size - sum(sizes)
        adjustment = total_diff / num_accounts
        sizes = [round(size + adjustment, 8) for size in sizes]

        return sizes
    except Exception as e:
        raise Exception(f'Failed to calculate short sizes: {e}')


def round_to_decimals(amount: float, decimals: int) -> float:
    return int(Decimal(str(amount)) * Decimal(10 ** decimals)) / 10 ** decimals
