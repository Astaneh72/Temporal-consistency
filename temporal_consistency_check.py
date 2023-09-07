import pandas as pd
import numpy as np
import sys, os

if int(sys.argv[2]) == 5:
    max_change_limit = 1
elif int(sys.argv[2]) == 10:
    max_change_limit = 2
elif int(sys.argv[2]) == 15:
    max_change_limit = 4


def read_input_file(input_file):
    df = pd.read_csv(f".\\{input_file}", header=[2])
    return df


def temporal_consistency_check(df):
    df["Timeseries Comment"] = df["Timeseries Comment"].fillna(value="")
    for index, row in df[1:].iterrows():
        temp_diff = abs(df.at[index, "Value"] - df.at[index - 1, "Value"])
        if temp_diff > max_change_limit:
            df.at[index, "Quality Code"] = 200
            if df.at[index, "Timeseries Comment"] != "":
                df.at[index, "Timeseries Comment"] = (
                    df.at[index, "Timeseries Comment"]
                    + "; temporal consistency check failed"
                )
            else:
                df.at[index, "Timeseries Comment"] = "temporal consistency check failed"
    df["Quality Code"] = np.where(df["Quality Code"] != 200, 160, 200)

    return df


def save_output_file(df, input_file):
    newfolder = os.path.join(".\\", f"{input_file[:-4]} checked")
    os.makedirs(newfolder, exist_ok=True)
    path = os.path.join(newfolder, f"{input_file[:-4]}_checked.csv")
    df.to_csv(path)


if __name__ == "__main__":
    data = read_input_file(sys.argv[1])
    checked_data = temporal_consistency_check(data)
    save_output_file(checked_data, sys.argv[1])
