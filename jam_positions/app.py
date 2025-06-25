#basic_code
# import pandas as pd
# df = pd.read_csv("jam_positions.csv")
# required = [
#     "AsofDate",  
#     "client",
#     "strategy",
#     "ticker",
#     "monthCode",
#     "yearCode",
#     "pstn"        
# ]

# df = df[required]
# group_cols = [
#     "AsofDate",
#     "client",
#     "strategy",
#     "ticker",
#     "monthCode",
#     "yearCode"
# ]
# df = (
#     df.groupby(group_cols, as_index=False , sort=False)["pstn"]
#       .sum()
# )
# df = df[df["strategy"] != "FARRMF"]
# negative_positions = df[df["pstn"] < 0]
# print(negative_positions.to_string(index=False))






#new

# from __future__ import annotations
# import json
# import sys
# from pathlib import Path
# from typing import Final
# import click
# import pandas as pd

# REQUIRED_COLS: Final[list[str]] = [
#     "AsofDate", "client", "strategy", "ticker",
#     "monthCode", "yearCode", "pstn",
# ]

# GROUP_COLS: Final[list[str]] = REQUIRED_COLS[:-1]   
# ALLOWED_STRATS: Final[set[str]] = {"OY", "OYFAR", "FAREQH", "CUSTOM1"}

# # Hard‑coded Jam‑process‑log row
# LOG: Final[dict[str, object]] = {
#     "process_name":   "LoadAPGIndex.pl",
#     "process_path":   "/apps/jam/etc/batchjobs/LoadAPGIndex.pl",
#     "run_by":         "jam",
#     "run_date":       "12/06/25 00:00",
#     "exit_code":      1,
#     "exit_msg":       "Job Started",
#     "log_file":       "/logs/jam/logs/LoadAPGIndex.pl.0612",
#     "output_file":    "",
#     "data":           "",
#     "business_day":   "12/06/2025",
#     "host_name":      "pd-vsl-app-01.core.corp",
#     "job_step":       0,
#     "start_time":     "12/06/25 17:14",
#     "end_time":       "12/06/25 17:14",
#     "records_input":  0,
#     "records_loaded": 0,
#     "records_error":  0,
# }

# def select_required(df: pd.DataFrame) -> pd.DataFrame:
#     return df[REQUIRED_COLS]

# def exclude_strategies(df: pd.DataFrame) -> pd.DataFrame:
#     df = df[df["strategy"] != "FARRMF"]          # <- new line

#     mask = (
#         df["strategy"].str.contains("SHORT", case=False, na=False)
#         | df["strategy"].isin(ALLOWED_STRATS)
#     )
#     print('----', df[~mask])
#     return df[~mask]

# def group_and_sum(df: pd.DataFrame) -> pd.DataFrame:
#     return (
#         df.groupby(GROUP_COLS, as_index=False, sort=False)["pstn"]
#           .sum()
#     )

# def keep_negatives(df: pd.DataFrame) -> pd.DataFrame:
#     return df[df["pstn"] < 0]

# @click.command()
# @click.argument("csv_path", type=click.Path(exists=True, dir_okay=False))
# def main(csv_path: str) -> None:
#     negatives = pd.DataFrame()     

#     try:
#         negatives = (
#             pd.read_csv(csv_path)
#               .pipe(select_required)
#               .pipe(exclude_strategies)
#               .pipe(group_and_sum)
#               .pipe(keep_negatives)
#         )

#     except Exception as exc:     
#         click.echo(f"ERROR: {exc}", err=True)
#         sys.exit(1)

#     finally:
#         if not negatives.empty:
#             print(json.dumps(LOG, indent=2))


# if __name__ == "__main__":
#     main()







#updated_command_line_input
# from __future__ import annotations
# import json, sys
# from pathlib import Path
# from typing import Final, Callable, Dict
# import click
# import pandas as pd

# REQUIRED_COLS: Final[list[str]] = [
#     "AsofDate", "client", "strategy", "ticker",
#     "monthCode", "yearCode", "pstn",
# ]
# GROUP_COLS: Final[list[str]] = REQUIRED_COLS[:-1]
# ALLOWED_STRATS: Final[set[str]] = {"OY", "OYFAR", "FAREQH", "CUSTOM1"}

# LOG: Final[dict[str, object]] = {
#     "process_name":   "LoadAPGIndex.pl",
#     "process_path":   "/apps/jam/etc/batchjobs/LoadAPGIndex.pl",
#     "run_by":         "jam",
#     "run_date":       "12/06/25 00:00",
#     "exit_code":      1,
#     "exit_msg":       "Job Started",
#     "log_file":       "/logs/jam/logs/LoadAPGIndex.pl.0612",
#     "output_file":    "",
#     "data":           "",
#     "business_day":   "12/06/2025",
#     "host_name":      "pd-vsl-app-01.core.corp",
#     "job_step":       0,
#     "start_time":     "12/06/25 17:14",
#     "end_time":       "12/06/25 17:14",
#     "records_input":  0,
#     "records_loaded": 0,
#     "records_error":  0,
# }

# def select_required(df: pd.DataFrame) -> pd.DataFrame:
#     return df[REQUIRED_COLS]

# def exclude_strategies(df: pd.DataFrame) -> pd.DataFrame:
#     df = df[df["strategy"] != "FARRMF"]
#     mask = (
#         df["strategy"].str.contains("SHORT", case=False, na=False)
#         | df["strategy"].isin(ALLOWED_STRATS)
#     )
#     return df[~mask]

# def group_and_sum(df: pd.DataFrame) -> pd.DataFrame:
#     return df.groupby(GROUP_COLS, as_index=False, sort=False)["pstn"].sum()

# def keep_negatives(df: pd.DataFrame) -> pd.DataFrame:
#     return df[df["pstn"] < 0]

# # ─────────────────────── CLI “script” wrappers ────────────────────── #
# @click.group()         
# def cli():
#     """Multi‑script driver.  Run `python app.py --help` to list jobs."""
#     pass

# @cli.command("neg_checker")
# @click.argument("csv_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
# def negative_position_checker(csv_path: Path):
#     try:
#         df = (
#             pd.read_csv(csv_path)
#               .pipe(select_required)
#               .pipe(exclude_strategies)
#               .pipe(group_and_sum)
#               .pipe(keep_negatives)
#         )
#     except Exception as exc:
#         click.echo(f"ERROR: {exc}", err=True)
#         sys.exit(1)

#     if not df.empty:
#         click.echo(df.to_string(index=False))
#         click.echo(json.dumps(LOG, indent=2))


# @cli.command("just_group")
# @click.argument("csv_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
# def just_group(csv_path: Path):
#     """
#     Load the CSV and print grouped sums (no filtering).
#     """
#     df = (
#         pd.read_csv(csv_path)
#           .pipe(select_required)
#           .pipe(group_and_sum)
#     )
#     click.echo(df.to_string(index=False))


# if __name__ == "__main__":
#     cli()






#env_input
from __future__ import annotations
import json, sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Final
import click
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

load_dotenv()
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
CSV_PATH = os.getenv("CSV_PATH")

REQUIRED_COLS: Final[list[str]] = [
    "AsofDate", "client", "strategy", "ticker",
    "monthCode", "yearCode", "pstn",
]
GROUP_COLS: Final[list[str]] = REQUIRED_COLS[:-1]
ALLOWED_STRATS: Final[set[str]] = {"OY", "OYFAR", "FAREQH", "CUSTOM1"}

LOG: Final[dict[str, object]] = {
    "process_name":   "LoadAPGIndex.pl",
    "process_path":   "/apps/jam/etc/batchjobs/LoadAPGIndex.pl",
    "run_by":         "jam",
    "run_date":       "12/06/25 00:00",
    "exit_code":      1,
    "exit_msg":       "Job Started",
    "log_file":       "/logs/jam/logs/LoadAPGIndex.pl.0612",
    "output_file":    "",
    "data":           "",
    "business_day":   "12/06/2025",
    "host_name":      "pd-vsl-app-01.core.corp",
    "job_step":       0,
    "start_time":     "12/06/25 17:14",
    "end_time":       "12/06/25 17:14",
    "records_input":  0,
    "records_loaded": 0,
    "records_error":  0,
}

# ─────────────────────── IO Classes ───────────────────────────── #
class IO(ABC):
    @abstractmethod
    def load_data(self) -> pd.DataFrame:
        pass

class CSVReader(IO):
    def __init__(self, path: Path):
        self.path = path

    def load_data(self) -> pd.DataFrame:
        return pd.read_csv(self.path)

class DatabaseReader(IO):
    def __init__(self):
        self.db_uri = os.getenv("DB_URI")

    def load_data(self) -> pd.DataFrame:
        if not self.db_uri:
            raise ValueError("DB_URI must be set in the .env file.")

        query = """
        SELECT
            AsofDate,
            client,
            strategy,
            ticker,
            monthCode,
            yearCode,
            pstn
        FROM your_table_name
        WHERE AsofDate >= '2025-06-01'  -- optional filter
        """

        try:
            engine = create_engine(self.db_uri)
            with engine.connect() as conn:
                df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            raise RuntimeError(f"Database query failed: {e}")

# ─────────────────────── Logic functions ───────────────────────── #
def select_required(df: pd.DataFrame) -> pd.DataFrame:
    return df[REQUIRED_COLS]

def exclude_strategies(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["strategy"] != "FARRMF"]
    mask = (
        df["strategy"].str.contains("SHORT", case=False, na=False)
        | df["strategy"].isin(ALLOWED_STRATS)
    )
    return df[~mask]

def group_and_sum(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(GROUP_COLS, as_index=False, sort=False)["pstn"].sum()

def keep_negatives(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["pstn"] < 0]

# ─────────────────────── CLI ───────────────────────────── #
@click.group()
def cli():
    """Multi‑script driver.  Run `python app.py --help` to list jobs."""
    pass

def get_io_source() -> IO:
    if ENVIRONMENT == "prod":
        return DatabaseReader()
    elif ENVIRONMENT == "dev":
        if not CSV_PATH:
            click.echo("CSV_PATH not set in .env", err=True)
            sys.exit(1)
        return CSVReader(Path(CSV_PATH))
    else:
        click.echo(f"Unknown ENVIRONMENT: {ENVIRONMENT}", err=True)
        sys.exit(1)

@cli.command("neg_checker")
def negative_position_checker():
    try:
        reader = get_io_source()
        df = (
            reader.load_data()
                  .pipe(select_required)
                  .pipe(exclude_strategies)
                  .pipe(group_and_sum)
                  .pipe(keep_negatives)
        )
    except Exception as exc:
        click.echo(f"ERROR: {exc}", err=True)
        sys.exit(1)

    if not df.empty:
        #click.echo(df.to_string(index=False))
        click.echo(json.dumps(LOG, indent=2))

@cli.command("just_group")
def just_group():
    reader = get_io_source()
    df = (
        reader.load_data()
              .pipe(select_required)
              .pipe(group_and_sum)
              .pipe(exclude_strategies)
              .pipe(keep_negatives)
    )
    click.echo(df.to_string(index=False))

if __name__ == "__main__":
    cli()

