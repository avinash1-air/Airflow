import os
import sys
from click.testing import CliRunner

def test_contracts_within_12_months():
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["CSV_PATH_CON"] = "TradeReqs.csv"

    if "app" in sys.modules:
        del sys.modules["app"]
    from app import cli

    runner = CliRunner()
    result = runner.invoke(cli, ["contract_req"])
    assert result.output == "True\n"
