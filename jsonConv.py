import pandas as pd

from pathlib import Path

dataDir = Path("csv_data")

for file_path in dataDir.iterdir():
    if file_path.is_file():
        with file_path.open("r") as f:
            df = pd.read_csv(f)
            df.to_json("json_data/" + file_path.stem + ".json", orient="records", indent=2)
