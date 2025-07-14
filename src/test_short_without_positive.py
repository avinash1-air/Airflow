import os
import sys
from click.testing import CliRunner

def test_short_strats_without_positive_positions():
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["CSV_PATH"] = "jam_positions.csv"  

    if "app" in sys.modules:
        del sys.modules["app"]
    from app import cli

    runner = CliRunner()
    result = runner.invoke(cli, ["short_strats"])
    assert result.output == "True\n"
