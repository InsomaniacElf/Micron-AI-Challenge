from glob import glob
from typing import Literal

import pandas as pd

TRAIN_DIR = "./train"


def get_train_data_files(
    file_type: Literal["run", "incoming_run", "metrology"],
) -> list[str]:
    # if file_type == "run":
    #     return glob(f"{TRAIN_DIR}/run_data_*.parquet")
    # elif file_type == "incoming_run":
    #     return glob(f"{TRAIN_DIR}/incoming_run_data_*.parquet")
    # else:
    #     return glob(f"{TRAIN_DIR}/metrology_data*.parquet")

    return glob(f"{TRAIN_DIR}/{file_type}_data*.parquet")


def read_multi_parquet_files(file_paths: list[str]) -> pd.DataFrame:
    return pd.concat(
        [pd.read_parquet(f) for f in file_paths],
    ).reset_index(drop=True)


def clean_df_column_names(df: pd.DataFrame) -> pd.DataFrame:
    cols: list[str] = df.columns
    df.columns = [col.strip().lower().replace(" ", "_") for col in cols]

    return df
