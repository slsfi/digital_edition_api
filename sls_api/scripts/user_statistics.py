"""
Generate user registration and activity statistics from the security DB.

This script is intended to run as a standalone reporting utility, for
example from Jenkins. It reads `sls_api/configs/security.yml` by default,
connects directly to the configured `user_database`, and writes the report
to stdout.

Default output format is pretty-printed JSON. TSV output is also available
with `--format tsv` for easier spreadsheet-style post-processing.

The script processes only a limited set of non-sensitive user profile and
activity fields: `created_timestamp`, `last_login_timestamp`,
`email_verified`, `cms_user`, `country`, and `intended_usage`. It does not
read or output passwords, email addresses, names, project assignments, or
token-related fields.

All statistics exclude CMS users (`cms_user = true`).

Reported statistics:
- Registrations in the last 12 calendar months, bucketed per month and
  split by current email verification status.
- Registrations in the previous full calendar year whose accounts are
  verified at the time the script runs.
- Total currently registered, verified, non-CMS users.
- Login recency counts for currently verified users over the last
  30/90/180/365 days, including percentages of all currently verified
  users.
- Country totals for currently verified users. Values must be ISO 3166-1
  alpha-2 country codes; missing or invalid values are grouped into the
  `missing_or_invalid` bucket. Recognized country codes are also decoded to
  English country names using a built-in mapping for Europe, North America,
  and Eastern Asia.
- Intended usage totals for currently verified users. Valid values are
  `personal`, `educational`, and `scholarly`. Multiple values may be
  stored as a `;`-separated string such as `personal;scholarly`. Missing or
  invalid values are grouped into the `missing_or_invalid` bucket.

Examples:
    python sls_api/scripts/user_statistics.py
    python sls_api/scripts/user_statistics.py --format tsv
"""

import argparse
import datetime
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from sqlalchemy import MetaData, Table, create_engine, select


ALLOWED_USAGE_VALUES = {"personal", "educational", "scholarly"}
COUNTRY_CODE_PATTERN = re.compile(r"^[A-Z]{2}$")
DEFAULT_LOGIN_RECENCY_DAYS = (30, 90, 180, 365)
DEFAULT_REGISTRATION_MONTHS = 12
MISSING_OR_INVALID_BUCKET = "missing_or_invalid"
MISSING_OR_INVALID_LABEL = "Missing or invalid"
COUNTRY_NAME_MAP = {
    "AD": "Andorra",
    "AL": "Albania",
    "AT": "Austria",
    "BA": "Bosnia and Herzegovina",
    "BE": "Belgium",
    "BG": "Bulgaria",
    "BY": "Belarus",
    "CA": "Canada",
    "CH": "Switzerland",
    "CN": "China",
    "CY": "Cyprus",
    "CZ": "Czechia",
    "DE": "Germany",
    "DK": "Denmark",
    "EE": "Estonia",
    "ES": "Spain",
    "FI": "Finland",
    "FO": "Faroe Islands",
    "FR": "France",
    "GB": "United Kingdom",
    "GE": "Georgia",
    "GL": "Greenland",
    "GR": "Greece",
    "HK": "Hong Kong",
    "HR": "Croatia",
    "HU": "Hungary",
    "IE": "Ireland",
    "IS": "Iceland",
    "IT": "Italy",
    "JP": "Japan",
    "KR": "South Korea",
    "LI": "Liechtenstein",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "LV": "Latvia",
    "MC": "Monaco",
    "MD": "Moldova",
    "ME": "Montenegro",
    "MK": "North Macedonia",
    "MO": "Macao",
    "MT": "Malta",
    "MX": "Mexico",
    "NL": "Netherlands",
    "NO": "Norway",
    "PL": "Poland",
    "PT": "Portugal",
    "RO": "Romania",
    "RU": "Russia",
    "RS": "Serbia",
    "SE": "Sweden",
    "SI": "Slovenia",
    "SK": "Slovakia",
    "SM": "San Marino",
    "TR": "Turkey",
    "TW": "Taiwan",
    "UA": "Ukraine",
    "US": "United States",
    "VA": "Vatican City",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate user registration and activity statistics.",
        epilog=(
            "Outputs pretty JSON by default. Use --format tsv for a flat "
            "tab-separated report suitable for spreadsheets."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("json", "tsv"),
        default="json",
        help="Output format written to stdout (default: json).",
    )
    return parser.parse_args()


def default_security_config_path() -> Path:
    return Path(__file__).resolve().parents[1] / "configs" / "security.yml"


def load_security_config(config_path: str) -> dict[str, Any]:
    yaml = YAML(typ="safe")
    with open(config_path, encoding="utf-8") as config_file:
        config = yaml.load(config_file) or {}

    for key, value in config.items():
        if isinstance(value, str):
            config[key] = os.path.expandvars(value)

    return config


def load_users_table(database_url: str) -> tuple[Any, Table]:
    engine = create_engine(database_url, pool_pre_ping=True)
    metadata = MetaData()
    users = Table("users", metadata, autoload_with=engine)
    return engine, users


def month_start(value: datetime.datetime) -> datetime.datetime:
    return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def shift_months(value: datetime.datetime, months: int) -> datetime.datetime:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    return value.replace(year=year, month=month)


def month_labels(now: datetime.datetime, months: int) -> list[str]:
    start = month_start(now)
    return [
        shift_months(start, offset).strftime("%Y-%m")
        for offset in range(-(months - 1), 1)
    ]


def current_time_like(value: datetime.datetime | None) -> datetime.datetime:
    """
    Return "now" in a form compatible with the given datetime value.

    PostgreSQL may return timezone-aware datetimes for timestamp-with-time-zone
    columns. Matching the timezone-awareness of the database value avoids
    naive/aware subtraction errors in recency calculations.
    """
    if isinstance(value, datetime.datetime) and value.tzinfo is not None and value.utcoffset() is not None:
        return datetime.datetime.now(datetime.timezone.utc).astimezone(value.tzinfo)
    return datetime.datetime.now()


def normalize_country(raw_country: Any) -> str:
    if not isinstance(raw_country, str):
        return MISSING_OR_INVALID_BUCKET

    country = raw_country.strip().upper()
    if COUNTRY_CODE_PATTERN.match(country):
        return country
    return MISSING_OR_INVALID_BUCKET


def country_label(country_code: str) -> str:
    if country_code == MISSING_OR_INVALID_BUCKET:
        return MISSING_OR_INVALID_LABEL
    return COUNTRY_NAME_MAP.get(country_code, country_code)


def normalize_usage_values(raw_usage: Any) -> list[str]:
    if not isinstance(raw_usage, str):
        return [MISSING_OR_INVALID_BUCKET]

    tokens = [token.strip().lower() for token in raw_usage.split(";")]
    tokens = [token for token in tokens if token]
    if not tokens:
        return [MISSING_OR_INVALID_BUCKET]

    unique_tokens = sorted(set(tokens))
    if any(token not in ALLOWED_USAGE_VALUES for token in unique_tokens):
        return [MISSING_OR_INVALID_BUCKET]

    return unique_tokens


def aggregate_statistics(rows: list[Any]) -> dict[str, Any]:
    """
    Aggregate database rows into the structured report emitted by the CLI.

    Registration buckets are based on the current month and previous
    11 calendar months. Login recency counts and all categorical totals
    are calculated only for non-CMS users. Login recency counts and all
    categorical totals are further restricted to users whose current
    `email_verified` value is true.
    """
    now = datetime.datetime.now()
    registration_month_keys = month_labels(now, DEFAULT_REGISTRATION_MONTHS)
    registration_month_set = set(registration_month_keys)
    previous_calendar_year = now.year - 1
    previous_calendar_year_verified_now = 0
    registration_stats = {
        month: {
            "month": month,
            "registered_total": 0,
            "verified_now": 0,
            "unverified_now": 0,
        }
        for month in registration_month_keys
    }

    verified_users_total = 0
    country_counter: Counter[str] = Counter()
    usage_counter: Counter[str] = Counter()
    login_recency_counts = {days: 0 for days in DEFAULT_LOGIN_RECENCY_DAYS}

    for row in rows:
        if bool(row.cms_user):
            continue

        created_timestamp = row.created_timestamp
        last_login_timestamp = row.last_login_timestamp
        email_verified = bool(row.email_verified)

        if isinstance(created_timestamp, datetime.datetime):
            created_month = created_timestamp.strftime("%Y-%m")
            if created_month in registration_month_set:
                registration_stats[created_month]["registered_total"] += 1
                if email_verified:
                    registration_stats[created_month]["verified_now"] += 1
                else:
                    registration_stats[created_month]["unverified_now"] += 1
            if created_timestamp.year == previous_calendar_year and email_verified:
                previous_calendar_year_verified_now += 1

        if not email_verified:
            continue

        verified_users_total += 1
        country_counter[normalize_country(row.country)] += 1

        for usage_value in normalize_usage_values(row.intended_usage):
            usage_counter[usage_value] += 1

        if isinstance(last_login_timestamp, datetime.datetime):
            age_in_days = (current_time_like(last_login_timestamp) - last_login_timestamp).total_seconds() / 86400
            for days in DEFAULT_LOGIN_RECENCY_DAYS:
                if age_in_days <= days:
                    login_recency_counts[days] += 1

    login_recency_verified = []
    for days in DEFAULT_LOGIN_RECENCY_DAYS:
        count = login_recency_counts[days]
        percent = round((count / verified_users_total) * 100, 2) if verified_users_total else 0.0
        login_recency_verified.append(
            {
                "days": days,
                "count": count,
                "percent_of_verified_users": percent,
            }
        )

    country_totals_verified = [
        {
            "country_code": country,
            "country_name": country_label(country),
            "count": count,
        }
        for country, count in sorted(country_counter.items(), key=lambda item: (-item[1], item[0]))
    ]
    intended_usage_totals_verified = [
        {"usage": usage, "count": count}
        for usage, count in sorted(usage_counter.items(), key=lambda item: (-item[1], item[0]))
    ]

    return {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "window": {
            "registration_months": DEFAULT_REGISTRATION_MONTHS,
            "registration_month_labels": registration_month_keys,
            "previous_calendar_year": previous_calendar_year,
            "login_recency_days": list(DEFAULT_LOGIN_RECENCY_DAYS),
        },
        "registered_verified_users_total": verified_users_total,
        "previous_calendar_year_registrations_verified_now": {
            "year": previous_calendar_year,
            "count": previous_calendar_year_verified_now,
        },
        "registrations_by_month": [
            registration_stats[month] for month in registration_month_keys
        ],
        "login_recency_verified": login_recency_verified,
        "country_totals_verified": country_totals_verified,
        "intended_usage_totals_verified": intended_usage_totals_verified,
    }


def to_tsv(report: dict[str, Any]) -> str:
    """
    Flatten the structured report into a single tab-separated table.
    """
    lines = ["report\tbucket\tcategory\tcount\tpercent"]
    lines.append(
        "\t".join(
            [
                "summary",
                "",
                "registered_verified_users_total",
                str(report["registered_verified_users_total"]),
                "",
            ]
        )
    )
    lines.append(
        "\t".join(
            [
                "summary",
                str(report["previous_calendar_year_registrations_verified_now"]["year"]),
                "previous_calendar_year_registrations_verified_now",
                str(report["previous_calendar_year_registrations_verified_now"]["count"]),
                "",
            ]
        )
    )

    for item in report["registrations_by_month"]:
        lines.append(
            "\t".join(
                [
                    "registrations_by_month",
                    item["month"],
                    "registered_total",
                    str(item["registered_total"]),
                    "",
                ]
            )
        )
        lines.append(
            "\t".join(
                [
                    "registrations_by_month",
                    item["month"],
                    "verified_now",
                    str(item["verified_now"]),
                    "",
                ]
            )
        )
        lines.append(
            "\t".join(
                [
                    "registrations_by_month",
                    item["month"],
                    "unverified_now",
                    str(item["unverified_now"]),
                    "",
                ]
            )
        )

    for item in report["login_recency_verified"]:
        lines.append(
            "\t".join(
                [
                    "login_recency_verified",
                    f"{item['days']}d",
                    "active_verified",
                    str(item["count"]),
                    f"{item['percent_of_verified_users']:.2f}",
                ]
            )
        )

    for item in report["country_totals_verified"]:
        lines.append(
            "\t".join(
                [
                    "country_totals_verified",
                    item["country_code"],
                    item["country_name"],
                    str(item["count"]),
                    "",
                ]
            )
        )

    for item in report["intended_usage_totals_verified"]:
        lines.append(
            "\t".join(
                [
                    "intended_usage_totals_verified",
                    "",
                    item["usage"],
                    str(item["count"]),
                    "",
                ]
            )
        )

    return "\n".join(lines)


def print_report(report: dict[str, Any], output_format: str) -> None:
    if output_format == "json":
        print(json.dumps(report, indent=2, ensure_ascii=True))
        return

    print(to_tsv(report))


def main() -> int:
    """
    Load configuration, query the users table, and print the report.
    """
    args = parse_args()
    config_path = str(default_security_config_path())
    try:
        security_config = load_security_config(config_path)
    except FileNotFoundError:
        print(
            "Could not find the default security config file.",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Failed to load the default security config file.",
            file=sys.stderr,
        )
        return 1

    database_url = security_config.get("user_database")
    if not database_url:
        print("security.yml is missing the 'user_database' setting.", file=sys.stderr)
        return 1

    try:
        engine, users = load_users_table(database_url)
        statement = select(
            users.c.created_timestamp,
            users.c.last_login_timestamp,
            users.c.email_verified,
            users.c.cms_user,
            users.c.country,
            users.c.intended_usage,
        )

        with engine.connect() as connection:
            rows = connection.execute(statement).all()
    except Exception:
        print("Failed to query the users table.", file=sys.stderr)
        return 1

    report = aggregate_statistics(rows)
    print_report(report, args.format)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
