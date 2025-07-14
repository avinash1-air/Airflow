import os
import sys
from click.testing import CliRunner

def test_dup_checker_without_duplicates():
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["CSV_PATH_DUP"] = "Book1.csv"

    if "app" in sys.modules:
        del sys.modules["app"]
    from app import cli

    runner = CliRunner()
    result = runner.invoke(cli, ["dup_req"])
    assert result.output == "True\n"
