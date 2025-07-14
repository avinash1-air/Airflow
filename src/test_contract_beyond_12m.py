import os
import sys
from click.testing import CliRunner

def test_contracts_exceeding_12_months():
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["CSV_PATH_CON"] = "TradeReqs_more.csv"

    if "app" in sys.modules:
        del sys.modules["app"]
    from app import cli

    runner = CliRunner()
    result = runner.invoke(cli, ["contract_req"])
    assert result.output == "False\n"
