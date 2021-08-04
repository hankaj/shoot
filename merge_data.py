import pandas as pd
import re
from os import listdir


def drop_title_columns(df):
    df.drop([1, 4, 7], inplace=True)


def reshape(df):
    return pd.DataFrame(df.values.reshape((1, 140)))


def drop_dummy_cols(df):
    df.drop(columns=[i for i in range(2, 20)], inplace=True)
    df.drop(columns=[i for i in range(20, 140, 2)], inplace=True)


def get_points_and_tens(result):
    n_points = int(re.search("^[0-9]*", result).group())
    inner_tens = int(re.search(" [0-9]*x", result).group()[1:-1])
    return n_points, inner_tens


def get_day_date_time(dt):
    day = dt[0]
    date = f"{dt[1]} {dt[2]} {dt[3]}"
    time = dt[4]
    return day, date, time


def rename_columns(df):
    df.columns = [i for i in range(1, 61)]


def add_columns(df, date, day, time, n_points, inner_tens):
    df["date"] = date
    df["day"] = day
    df["time"] = time
    df["n_points"] = n_points
    df["inner_tens"] = inner_tens


def refactor_data(data_dir):
    df = pd.read_csv(f"csv_sius_data/60/{data_dir}", names=list(range(20)))
    drop_title_columns(df)
    df = reshape(df)
    drop_dummy_cols(df)
    n_points, inner_tens = get_points_and_tens(df.loc[0, 0])
    day, date, time = get_day_date_time(df.loc[0, 1].split(" "))
    df.drop(columns=[0, 1], inplace=True)
    rename_columns(df)
    add_columns(df, date, day, time, n_points, inner_tens)
    return df


def export_data():
    data_dirs = listdir("csv_sius_data/60")
    if ".DS_Store" in data_dirs:
        data_dirs.remove(".DS_Store")
    dfs_to_concat = []
    for data_dir in data_dirs:
        dfs_to_concat.append(refactor_data(data_dir))
    df_final = pd.concat(dfs_to_concat)
    df_final.to_csv("export_dataframe.csv", index=False, header=True)


if __name__ == "__main__":
    export_data()
