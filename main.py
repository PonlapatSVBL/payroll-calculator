import asyncio
import aiohttp
import aiomysql
import base64
import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, cast

from dotenv import load_dotenv

load_dotenv()

# ========================
# Logging
# ========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"payroll_calc_{datetime.now().strftime('%Y%m%d')}.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ========================
# Configuration
# ========================
MYSQL_HOST     = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

SALARY_MONTH = os.getenv("SALARY_MONTH", "2026-05")
API_URL      = "https://hms-php-core-new-payroll-calculation.azurewebsites.net/api-web.php"

# Concurrency: N channels run at the same time, each processes M people concurrently
# Total concurrent API calls = MAX_CHANNELS_CONCURRENT * MAX_PEOPLE_PER_CHANNEL
MAX_CHANNELS_CONCURRENT = int(os.getenv("MAX_CHANNELS_CONCURRENT", "10"))
MAX_PEOPLE_PER_CHANNEL  = int(os.getenv("MAX_PEOPLE_PER_CHANNEL", "10"))


# ========================
# DB Queries
# ========================
async def fetch_databases(pool: aiomysql.Pool) -> List[str]:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT `code`
                FROM hms_api.sys_list_of_value
                WHERE list_type = 'inst_dbn'
                AND code != 'hms_inst32'
                ORDER BY list_type
                """
            )
            rows = cast(List[tuple[str, ...]], await cur.fetchall())  # type: ignore[misc]
            dbs: List[str] = [row[0] for row in rows]
            for extra in ("hms_focus", "hms_hr"):
                if extra not in dbs:
                    dbs.append(extra)
            dbs.insert(0, "hms_inst32")
            return dbs


async def fetch_slips(pool: aiomysql.Pool, db: str) -> List[Dict[str, Any]]:
    # db name comes from our own DB — backtick-quote it for safety
    sql = f"""
        SELECT
            _rp.master_salary_month,
            _sl.master_salary_slip_id,
            _sl.employee_id,
            _sl.instance_server_id,
            _sl.instance_server_channel_id
        FROM `{db}`.payroll_master_salary_report _rp
        INNER JOIN `{db}`.payroll_master_salary_slip _sl
            ON _rp.master_salary_report_id = _sl.master_salary_report_id
        WHERE _rp.master_salary_month = %s
    """
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (SALARY_MONTH,))
            return await cur.fetchall()


# ========================
# API
# ========================
def _b64(value: Any) -> str:
    return base64.b64encode(str(value).encode()).decode()


def build_payload(slip: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "_compgrp": "hrs",
        "_comp": "calculation_normal",
        "_action": "calculate_month",
        "identify_user_id": "",
        "instance_server_id": _b64(slip["instance_server_id"]),
        "instance_server_channel_id": _b64(slip["instance_server_channel_id"]),
        "employee_id": _b64(slip["employee_id"]),
        "year_month": slip["master_salary_month"],
        "calculate_to": "NOW",
        "user_name": "",
        "user_psw": "",
        "language_code": "TH",
    }


async def process_slip(
    slip: Dict[str, Any],
    session: aiohttp.ClientSession,
    people_sem: asyncio.Semaphore,
) -> None:
    slip_id = str(slip["master_salary_slip_id"])

    async with people_sem:
        try:
            payload = build_payload(slip)
            async with session.post(
                API_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                body = await resp.text()
                log.info(
                    f"[OK] slip={slip_id} emp={slip['employee_id']} "
                    f"ch={slip['instance_server_channel_id']} status={resp.status}"
                )
                log.debug(f"Response body: {body}")
        except Exception as exc:
            log.error(f"[ERR] slip={slip_id} emp={slip['employee_id']} error={exc}")


# ========================
# Channel Worker
# ========================
async def process_channel(
    channel_id: str,
    slips: List[Dict[str, Any]],
    session: aiohttp.ClientSession,
) -> None:
    log.info(f"Channel {channel_id}: {len(slips)} slip(s) pending...")
    people_sem = asyncio.Semaphore(MAX_PEOPLE_PER_CHANNEL)
    tasks = [process_slip(s, session, people_sem) for s in slips]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        log.warning(f"Channel {channel_id}: {len(errors)} task(s) raised exceptions.")
    log.info(f"Channel {channel_id}: done.")


# ========================
# Main
# ========================
async def main() -> None:
    log.info(
        f"MONTH={SALARY_MONTH} | "
        f"MAX_CHANNELS={MAX_CHANNELS_CONCURRENT} | "
        f"MAX_PEOPLE_PER_CHANNEL={MAX_PEOPLE_PER_CHANNEL} | "
        f"TOTAL_CONCURRENCY={MAX_CHANNELS_CONCURRENT * MAX_PEOPLE_PER_CHANNEL}"
    )

    pool = await aiomysql.create_pool(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        charset="utf8mb4",
        autocommit=True,
    )

    all_slips: List[Dict[str, Any]] = []
    async with pool:
        databases = await fetch_databases(pool)
        log.info(f"Found {len(databases)} database(s): {databases}")

        for db in databases:
            try:
                slips = await fetch_slips(pool, db)
                log.info(f"  DB '{db}': {len(slips)} slip(s).")
                all_slips.extend(slips)
            except Exception as exc:
                log.error(f"  DB '{db}': failed — {exc}")

    log.info(f"Total slips across all DBs: {len(all_slips)}")

    by_channel: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for slip in all_slips:
        by_channel[slip["instance_server_channel_id"]].append(slip)

    log.info(f"Total channels: {len(by_channel)}")

    channel_sem = asyncio.Semaphore(MAX_CHANNELS_CONCURRENT)
    total_conn = MAX_CHANNELS_CONCURRENT * MAX_PEOPLE_PER_CHANNEL
    connector = aiohttp.TCPConnector(limit=total_conn)

    async with aiohttp.ClientSession(connector=connector) as session:

        async def run_channel(ch_id: str, ch_slips: List[Dict[str, Any]]) -> None:
            async with channel_sem:
                await process_channel(ch_id, ch_slips, session)

        channel_tasks = [run_channel(ch_id, ch_slips) for ch_id, ch_slips in by_channel.items()]
        await asyncio.gather(*channel_tasks, return_exceptions=True)

    log.info("All done.")


if __name__ == "__main__":
    asyncio.run(main())
