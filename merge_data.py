import pandas as pd
import re
from os import listdir


def drop_title_columns(df, n_shots):
    if n_shots == 60:
        df.drop([1, 4, 7], inplace=True)
    else:
        df.drop([1, 4], inplace=True)


def reshape(df):
    return pd.DataFrame(df.values.reshape((1, -1)))


def drop_dummy_cols(df, n_shots):
    df.drop(columns=[i for i in range(2, 20)], inplace=True)
    df.drop(columns=[i for i in range(20, 2 * n_shots + 20, 2)], inplace=True)


def get_points_and_tens(result):
    n_points = int(re.search("^[0-9]*", result).group())
    inner_tens = int(re.search(" [0-9]*x", result).group()[1:-1])
    return n_points, inner_tens

def convert_date(dt):
    months = {
        "JAN" : "01",
        "FEB" : "02",
        "MAR" : "03",
        "APR" : "04",
        "MAY" : "05",
        "JUN" : "06",
        "JUL" : "07",
        "AUG" : "08",
        "SEP" : "09",
        "OCT" : "10",
        "NOV" : "11",
        "DEC" : "12"
    }
    month = months[dt[2]]
    return f"{dt[3]}-{month}-{int(dt[1]):02d}"

def get_day_date_time(dt):
    day = dt[0]
    date = convert_date(dt)
    time = dt[4]
    return day, date, time


def rename_columns(df, n_shots):
    df.columns = [i for i in range(1, n_shots + 1)]


def add_columns(df, date, day, time, n_points, inner_tens):
    df["date"] = date
    df["day"] = day
    df["time"] = time
    df["n_points"] = n_points
    df["inner_tens"] = inner_tens


def refactor_data(data_dir, n_shots):
    df = pd.read_csv(f"csv_sius_data/{n_shots}/{data_dir}", names=list(range(20)))
    drop_title_columns(df, n_shots)
    df = reshape(df)
    drop_dummy_cols(df, n_shots)
    n_points, inner_tens = get_points_and_tens(df.loc[0, 0])
    day, date, time = get_day_date_time(df.loc[0, 1].split(" "))
    df.drop(columns=[0, 1], inplace=True)
    rename_columns(df, n_shots)
    add_columns(df, date, day, time, n_points, inner_tens)
    return df


def get_dfs_to_concat(data_dirs, n_shots):
    if ".DS_Store" in data_dirs:
        data_dirs.remove(".DS_Store")
    dfs_to_concat = []
    for data_dir in data_dirs:
        dfs_to_concat.append(refactor_data(data_dir, n_shots))
    return dfs_to_concat


def export_data():
    data_dirs_60 = listdir("csv_sius_data/60")
    data_dirs_40 = listdir("csv_sius_data/40")
    dfs_to_concat = get_dfs_to_concat(data_dirs_60, 60) + get_dfs_to_concat(
        data_dirs_40, 40
    )
    df_final = pd.concat(dfs_to_concat)
    df_final.set_index('date', inplace=True)
    df_final.sort_index(inplace=True)
    print(df_final)
    df_final.to_csv("export_dataframe.csv", index=True, header=True)


if __name__ == "__main__":
    export_data()
