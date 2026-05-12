import asyncio
import aiomysql
import base64
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

MYSQL_HOST     = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

SALARY_MONTH = os.getenv("SALARY_MONTH", "2026-05")
OUTPUT_FILE  = Path(os.getenv("OUTPUT_FILE", "slips.json"))


def _b64(value: Any) -> str:
    return base64.b64encode(str(value).encode()).decode()


async def fetch_databases(pool: aiomysql.Pool) -> List[str]:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT `code`
                FROM hms_api.sys_list_of_value
                WHERE list_type = 'inst_dbn'
                ORDER BY list_type
                """
            )
            rows = await cur.fetchall()
            return [row[0] for row in rows]


async def fetch_slips(pool: aiomysql.Pool, db: str) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT
            _rp.master_salary_month,
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


async def main() -> None:
    pool = await aiomysql.create_pool(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        charset="utf8mb4",
        autocommit=True,
    )

    slips: List[Dict[str, Any]] = []
    async with pool:
        databases = await fetch_databases(pool)
        log.info(f"Found {len(databases)} database(s): {databases}")

        for db in databases:
            try:
                rows = await fetch_slips(pool, db)
                log.info(f"  DB '{db}': {len(rows)} slip(s).")
                slips.extend(rows)
            except Exception as exc:
                log.error(f"  DB '{db}': failed — {exc}")

    output = [
        {
            "instance_server_id": _b64(s["instance_server_id"]),
            "instance_server_channel_id": _b64(s["instance_server_channel_id"]),
            "employee_id": _b64(s["employee_id"]),
            "year_month": s["master_salary_month"],
        }
        for s in slips
    ]

    OUTPUT_FILE.write_text(
        json.dumps(output, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info(f"Generated {len(output)} slip(s) → {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
