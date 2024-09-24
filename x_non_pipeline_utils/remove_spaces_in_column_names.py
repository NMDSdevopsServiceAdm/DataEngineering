from pathlib import Path
import pandas as pd

# folder_path = "F:\ASC-WDS Copy Files\Research & Analysis Team Folders\Analysis Team\b. Data Sources\09. Tableau\Covid-19 - ASC stats monitor\z. NHS Capacity Tracker downloads\For data engineering ingestion\Added 2024_09_24"
folder = Path(
    "F:\ASC-WDS Copy Files\Research & Analysis Team Folders\Analysis Team\b. Data Sources\09. Tableau\Covid-19 - ASC stats monitor\z. NHS Capacity Tracker downloads\For data engineering ingestion\Added 2024_09_24"
)
for file in folder.glob("*.csv"):
    current_file = pd.read_csv(file)
    current_file.columns.str.replace(" ", "_")
    current_file.to_csv(file)
