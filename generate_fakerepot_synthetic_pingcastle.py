#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate SYNTHETIC PingCastle XML reports that match the customer use-case:

- Weekly scans for years
- Keep all reports for last N days
- For older than N days: keep only ONE per month per domain (latest by Generation)

This generator produces multiple reports per domain within the same month in the "old" period,
so the maintenance script will produce DELETE_Extras > 0.

AUTHOR = Karim AZZOUZI
VENDOR = Netwrix Corporation
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import random
import re
import zipfile
import xml.etree.ElementTree as ET
from typing import Optional, List, Tuple

ISO_DT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)?$"
)

def parse_iso_datetime(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not ISO_DT_RE.match(s):
        return None
    try:
        d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d
    except Exception:
        return None

def format_like(original: str, new_dt: dt.datetime) -> str:
    original = (original or "").strip()
    frac = "." in original.split("T")[-1]
    if new_dt.tzinfo is None:
        new_dt = new_dt.replace(tzinfo=dt.timezone.utc)
    if frac:
        return new_dt.isoformat(timespec="microseconds")
    return new_dt.isoformat(timespec="seconds")

def ensure_synthetic_marker(root: ET.Element) -> None:
    root.insert(0, ET.Comment("SYNTHETIC_TEST_DATASET - generated for lab/testing only"))
    if root.find("./Synthetic") is None:
        el = ET.Element("Synthetic")
        el.text = "true"
        root.insert(1, el)

def set_text_if_exists(root: ET.Element, path: str, value: str) -> None:
    el = root.find(path)
    if el is not None:
        el.text = value

def safe_domain(i: int) -> str:
    return f"fake-{i:03d}.corp.example.invalid"

def safe_netbios(i: int) -> str:
    return f"FAKE{i:03d}"

def domain_to_dn(domain_fqdn: str) -> str:
    parts = domain_fqdn.split(".")
    return ",".join([f"DC={p}" for p in parts])

def replace_dn_suffix(dn: str, old_dc_suffix_any: str, new_dc_suffix: str) -> str:
    if not dn or "DC=" not in dn:
        return dn
    if old_dc_suffix_any and old_dc_suffix_any in dn:
        return dn.replace(old_dc_suffix_any, new_dc_suffix)
    m = re.search(r"(,DC=[^,]+)+$", dn)
    if m:
        return dn[: m.start()] + new_dc_suffix
    return dn

def update_known_date_tags(root: ET.Element, base: dt.datetime) -> None:
    # We want GenerationDate to match PingCastle "Generation"
    candidates = [
        ("./GenerationDate", base),
        ("./SchemaLastChanged", base - dt.timedelta(days=30)),
        ("./ExchangeInstall", base - dt.timedelta(days=90)),
        ("./LastADBackup", base - dt.timedelta(days=2, hours=3)),
        ("./LAPSInstalled", base - dt.timedelta(days=1800)),
        ("./AdminLastLoginDate", base - dt.timedelta(days=800)),
        ("./KrbtgtLastChangeDate", base - dt.timedelta(days=2500)),
    ]
    for path, new_dt in candidates:
        el = root.find(path)
        if el is None or not el.text:
            continue
        old = el.text
        parsed = parse_iso_datetime(old)
        el.text = format_like(old, new_dt) if parsed else new_dt.isoformat()

    # Also some DC attributes/tags if present
    for dc in root.findall(".//HealthcheckDomainController"):
        old_attr = dc.attrib.get("AdminLocalLogin")
        if old_attr:
            parsed = parse_iso_datetime(old_attr)
            new_dt = base - dt.timedelta(days=1200)
            dc.attrib["AdminLocalLogin"] = format_like(old_attr, new_dt) if parsed else new_dt.isoformat()

def month_range(start: dt.date, end: dt.date) -> List[Tuple[int,int]]:
    out = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        out.append((y, m))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return out

def gen_weekly_dates_in_month(rng: random.Random, year: int, month: int, per_month: int) -> List[dt.datetime]:
    # pick 4-5 "weekly-like" days within the month
    # Spread them (e.g. 3, 10, 17, 24, 28) with slight jitter.
    base_days = [3, 10, 17, 24, 28][:per_month]
    out = []
    for d in base_days:
        day = min(d + rng.randint(-1, 1), 28)
        hour = rng.randint(0, 23)
        minute = rng.randint(0, 59)
        second = rng.randint(0, 59)
        out.append(dt.datetime(year, month, day, hour, minute, second, tzinfo=dt.timezone.utc))
    out.sort()
    return out

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--template", required=True, help="Path to template XML (exported PingCastle report)")
    ap.add_argument("--outdir", default="out_reports_usecase", help="Output folder")
    ap.add_argument("--domains", type=int, default=3, help="Number of domains to generate")
    ap.add_argument("--reports-per-domain", type=int, default=120, help="Total reports per domain")
    ap.add_argument("--retention-days", type=int, default=365, help="Cutoff days (to match maintenance script)")
    ap.add_argument("--old-years", type=int, default=6, help="How many years of OLD data (weekly-like)")
    ap.add_argument("--old-per-month", type=int, default=4, help="How many reports per month in old period (4-5 typical)")
    ap.add_argument("--recent-reports", type=int, default=20, help="How many recent reports (kept all)")
    ap.add_argument("--seed", type=int, default=1337, help="Random seed")
    args = ap.parse_args()

    rng = random.Random(args.seed)

    with open(args.template, "rb") as f:
        data = f.read()

    tpl_root = ET.fromstring(data)
    old_domain = (tpl_root.findtext("./DomainFQDN") or "").strip()
    old_dc_suffix = ""
    if old_domain:
        old_dc_suffix = "," + domain_to_dn(old_domain)

    os.makedirs(args.outdir, exist_ok=True)

    now = dt.datetime.now(dt.timezone.utc)
    cutoff = now - dt.timedelta(days=args.retention_days)

    # Old period: from (cutoff - old_years) to (cutoff - 1 day)
    old_start = (cutoff - dt.timedelta(days=365 * args.old_years)).date()
    old_end = (cutoff - dt.timedelta(days=1)).date()

    months = month_range(old_start.replace(day=1), old_end.replace(day=1))

    generated_files: List[str] = []

    for di in range(1, args.domains + 1):
        dom = safe_domain(di)
        nb = safe_netbios(di)
        new_dc_tail = "," + domain_to_dn(dom)

        # Build OLD dates (weekly-like in each month) until we reach the target count
        old_dates: List[dt.datetime] = []
        for (y, m) in months:
            old_dates.extend(gen_weekly_dates_in_month(rng, y, m, args.old_per_month))
            if len(old_dates) >= max(0, args.reports_per_domain - args.recent_reports):
                break

        old_dates = old_dates[: max(0, args.reports_per_domain - args.recent_reports)]

        # Build RECENT dates (within last retention-days/inside cutoff) to ensure "keep recent all"
        recent_dates: List[dt.datetime] = []
        if args.recent_reports > 0:
            recent_start = cutoff + dt.timedelta(days=1)
            recent_end = now
            delta = int((recent_end - recent_start).total_seconds())
            for _ in range(args.recent_reports):
                pick = rng.randint(0, max(delta, 1))
                recent_dates.append(recent_start + dt.timedelta(seconds=pick))
            recent_dates.sort()

        all_dates = old_dates + recent_dates
        # If user asked for more than we produced, pad with more recent dates
        while len(all_dates) < args.reports_per_domain:
            all_dates.append(now - dt.timedelta(days=rng.randint(0, args.retention_days - 1)))
        all_dates = all_dates[: args.reports_per_domain]
        all_dates.sort()

        # Emit files
        for idx, gen_dt in enumerate(all_dates, 1):
            root = ET.fromstring(data)
            ensure_synthetic_marker(root)

            set_text_if_exists(root, "./DomainFQDN", dom)
            set_text_if_exists(root, "./ForestFQDN", dom)
            set_text_if_exists(root, "./NetBIOSName", nb)

            update_known_date_tags(root, gen_dt)

            # DN suffix rewrite
            for el in root.iter():
                if el.text and "DC=" in el.text and ("DistinguishedName" in el.tag or "CN=" in el.text):
                    el.text = replace_dn_suffix(el.text, old_dc_suffix, new_dc_tail)

            stamp = gen_dt.strftime("%Y%m%d-%H%M%S")
            out_name = f"PingCastleReport_{dom.replace('.', '_')}_{stamp}_{idx:03d}.xml"
            out_path = os.path.join(args.outdir, out_name)

            xml_bytes = ET.tostring(root, encoding="utf-8", xml_declaration=True)
            with open(out_path, "wb") as f:
                f.write(xml_bytes)

            generated_files.append(out_path)

    zip_path = os.path.abspath(os.path.join(args.outdir, f"PingCastle_Synthetic_USECASE_{args.domains}domains.zip"))
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in generated_files:
            z.write(p, arcname=os.path.basename(p))

    print(f"[OK] Generated: {len(generated_files)} XML files")
    print(f"[OK] ZIP: {zip_path}")
    print(f"[INFO] cutoff (Generation) = {cutoff.isoformat()}")
    print("[INFO] Old data contains multiple reports per month per domain -> you should see DELETE_Extras > 0.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
