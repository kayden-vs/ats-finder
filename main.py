"""
main.py — CLI entrypoint for ats_finder.

Usage:
    python main.py --input companies.txt --output companies_found.yaml \\
                   --csv ats_report.csv --workers 10

Run `python main.py --help` for full argument list.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

import httpx
from tqdm import tqdm

from detector import probe_all, set_semaphore, _ALL_PROBERS
from normalizer import generate_slugs
from output import checkpoint, print_summary, write_csv, write_yaml

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, stream=sys.stderr)
    # Silence noisy third-party loggers unless verbose
    if not verbose:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)


logger = logging.getLogger("ats_finder")

# ---------------------------------------------------------------------------
# CLI arguments
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ats_finder",
        description=(
            "Auto-detect which ATS a company uses and output a companies.yaml "
            "append-block plus a detailed CSV report."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--input", "-i",
        default="companies.txt",
        metavar="FILE",
        help="Path to newline-separated company names file.",
    )
    p.add_argument(
        "--output", "-o",
        default="companies_found.yaml",
        metavar="FILE",
        help="Path for YAML output (directly appendable to jobradar's companies.yaml).",
    )
    p.add_argument(
        "--csv", "-c",
        default="ats_report.csv",
        metavar="FILE",
        help="Path for CSV probe report.",
    )
    p.add_argument(
        "--workers", "-w",
        type=int,
        default=10,
        metavar="N",
        help="Max concurrent companies processed at once.",
    )
    p.add_argument(
        "--skip-ats",
        default="",
        metavar="ATS,...",
        help=(
            "Comma-separated list of ATS platforms to skip. "
            f"Available: {', '.join(_ALL_PROBERS.keys())}"
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be probed without making any requests.",
    )
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    return p


# ---------------------------------------------------------------------------
# Core async runner
# ---------------------------------------------------------------------------

CHECKPOINT_EVERY = 10  # write partial results after every N companies


async def run(
    companies: list[str],
    output_yaml: Path,
    output_csv: Path,
    workers: int,
    skip_ats: set[str],
    dry_run: bool,
) -> list[dict]:
    """Main async pipeline."""

    if dry_run:
        print(f"\nDry run — would probe {len(companies)} companies across "
              f"{len(_ALL_PROBERS) - len(skip_ats)} ATS platforms:\n")
        for company in companies:
            slugs = generate_slugs(company)
            print(f"  {company!r:30s} → slugs: {slugs[:3]}{'...' if len(slugs) > 3 else ''}")
        print(f"\nSkipping: {', '.join(skip_ats) if skip_ats else 'none'}")
        return []

    # Set global semaphore based on worker count
    sem = asyncio.Semaphore(workers)
    set_semaphore(sem)

    all_results: list[dict] = []
    # slug -> original company name mapping for YAML output
    slug_to_name: dict[str, str] = {}

    print(f"\nProbing {len(companies)} companies across "
          f"{len(_ALL_PROBERS) - len(skip_ats)} ATS platforms...\n")

    start_total = time.monotonic()

    async with httpx.AsyncClient() as client:
        # Process companies with bounded concurrency using a semaphore-guarded queue
        company_sem = asyncio.Semaphore(workers)

        async def process_company(company: str, pbar: tqdm) -> dict:
            async with company_sem:
                slugs = generate_slugs(company)
                logger.debug("Probing %r with slugs %s", company, slugs)
                result = await probe_all(company, slugs, client, skip_ats=skip_ats)
                result["company"] = company
                # Register slug → name mapping for YAML
                if result.get("slug"):
                    slug_to_name[result["slug"]] = company
                pbar.update(1)
                return result

        with tqdm(total=len(companies), unit="co", ncols=80,
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} companies | {elapsed}") as pbar:

            tasks = [process_company(c, pbar) for c in companies]

            # Process in chunks of CHECKPOINT_EVERY for incremental saves
            chunk_size = CHECKPOINT_EVERY
            first_checkpoint = True

            for i in range(0, len(tasks), chunk_size):
                chunk_tasks = tasks[i : i + chunk_size]
                chunk_results = await asyncio.gather(*chunk_tasks, return_exceptions=True)

                for r in chunk_results:
                    if isinstance(r, Exception):
                        logger.error("Company probe raised unexpectedly: %s", r)
                        # Manufacture an error entry
                        all_results.append({
                            "company": "unknown",
                            "ats": None,
                            "slug": None,
                            "jobs_found": 0,
                            "careers_url": "",
                            "probe_time_ms": 0,
                            "status": "error",
                        })
                    else:
                        all_results.append(r)

                # Checkpoint partial results
                checkpoint(
                    output_yaml,
                    output_csv,
                    all_results,
                    slug_to_name,
                    first_batch=first_checkpoint,
                )
                first_checkpoint = False
                logger.info(
                    "Checkpoint: %d/%d companies processed",
                    min(i + chunk_size, len(companies)),
                    len(companies),
                )

    elapsed = time.monotonic() - start_total
    logger.info("Total probe time: %.1fs", elapsed)

    return all_results


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    _configure_logging(args.verbose)

    # Read company list
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: input file not found: {input_path}", file=sys.stderr)
        return 1

    raw = input_path.read_text(encoding="utf-8").splitlines()
    companies = [line.strip() for line in raw if line.strip() and not line.startswith("#")]

    if not companies:
        print("Error: no companies found in input file.", file=sys.stderr)
        return 1

    # Parse skip-ats
    skip_ats: set[str] = set()
    if args.skip_ats:
        for s in args.skip_ats.split(","):
            s = s.strip().lower()
            if s and s not in _ALL_PROBERS:
                print(f"Warning: unknown ATS to skip: {s!r}", file=sys.stderr)
            elif s:
                skip_ats.add(s)

    output_yaml = Path(args.output)
    output_csv = Path(args.csv)

    # Run the async pipeline
    try:
        all_results = asyncio.run(
            run(
                companies=companies,
                output_yaml=output_yaml,
                output_csv=output_csv,
                workers=args.workers,
                skip_ats=skip_ats,
                dry_run=args.dry_run,
            )
        )
    except KeyboardInterrupt:
        print("\n\nInterrupted — partial results may have been saved.", file=sys.stderr)
        return 130

    if args.dry_run:
        return 0

    # Final write (in case last chunk < CHECKPOINT_EVERY)
    # Collect slug → name mapping from results
    slug_to_name: dict[str, str] = {
        r["slug"]: r["company"]
        for r in all_results
        if r.get("slug") and r.get("company")
    }
    write_yaml(output_yaml, all_results, slug_to_name)
    write_csv(output_csv, all_results, mode="w")

    print_summary(all_results)
    print(f"\nOutput written to: {output_yaml}, {output_csv}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
