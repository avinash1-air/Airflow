import os
import sys
from click.testing import CliRunner

os.environ["ENVIRONMENT"] = "dev"
os.environ["CSV_PATH"] = "test_data/jam_positions.csv" 

if "app" in sys.modules:
        del sys.modules["app"]

from app import cli
def test_neg_checker_with_negative_data():
    runner = CliRunner()
    result = runner.invoke(cli, ["neg_checker"])
    #print("-------->", result.output) 
    assert result.output == "False\n"