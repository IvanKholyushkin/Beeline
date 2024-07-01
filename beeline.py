import pandas as pd
import numpy as np

class Beeline:
    def __init__(self, path_kms, path_oper, delimiter=";"):
        self.path_kms = path_kms
        self.path_oper = path_oper
        self.delimiter = delimiter
        self.df_kms, self.df_oper = self.load_and_prepare_data()
        self.delta = 3
    
    def load_and_prepare_data(self):
        def preprocess_df(df):
            columns_mapping = {
                "to_char": "Дата звонка",
                "to_char.1": "Время звонка",
                "phoneb": "Принимающий номер",
                "to_char.2": "Длительность",
                "?column?": "Длительность округленная до минут",
            }
            df.rename(columns=columns_mapping, inplace=True)
            df.drop_duplicates(ignore_index=True, inplace=True)
            df["Время звонка"] = pd.to_timedelta(df["Время звонка"], errors='coerce').dt.total_seconds().astype('Int64')
            df["Длительность"] = pd.to_timedelta(df["Длительность"], errors='coerce').dt.total_seconds().astype('Int64')
            return df

        df_kms = pd.read_csv(self.path_kms, delimiter=self.delimiter)
        df_oper = pd.read_csv(self.path_oper, delimiter=self.delimiter)

        df_kms = preprocess_df(df_kms)
        df_oper = preprocess_df(df_oper)

        return df_kms, df_oper

    def find_within_delta(self):
        matched_calls = self.df_kms.merge(
            self.df_oper,
            on=["Дата звонка", "Принимающий номер"],
            how="inner",
            indicator=True,
        ).sort_values(by=["Дата звонка", "Время звонка_x", "Время звонка_y"])

        within_delta = matched_calls[
            (
                np.abs(matched_calls["Длительность_x"] - matched_calls["Длительность_y"])
                <= self.delta
            )
            & (
                np.abs(matched_calls["Время звонка_x"] - matched_calls["Время звонка_y"])
                <= self.delta
            )
        ]

        return within_delta

    def find_non_matched_calls(self, df_main, within_delta, numbers_not_in):
        non_matched_calls = df_main.merge(
            within_delta,
            on=["Дата звонка", "Время звонка", "Принимающий номер", "Длительность"],
            how="left",
            indicator=True,
        )
        non_matched_calls = non_matched_calls[
            non_matched_calls["_merge"] == "left_only"
        ][["Дата звонка", "Время звонка", "Принимающий номер", "Длительность"]]

        other_calls = non_matched_calls.merge(
            numbers_not_in,
            on=["Дата звонка", "Время звонка", "Принимающий номер", "Длительность"],
            how="left",
            indicator=True,
        )
        return other_calls[
            other_calls["_merge"] == "left_only"
        ][["Дата звонка", "Время звонка", "Принимающий номер", "Длительность"]]

    def analyze_data(self):
        within_delta = self.find_within_delta()

        numbers_not_in_oper = self.df_kms.merge(
            self.df_oper, on=["Дата звонка", "Принимающий номер"], how="left", indicator=True
        )
        numbers_not_in_oper = numbers_not_in_oper[numbers_not_in_oper["_merge"] == "left_only"][
            ["Дата звонка", "Время звонка_x", "Принимающий номер", "Длительность_x"]
            ].rename(
            columns={"Время звонка_x": "Время звонка", "Длительность_x": "Длительность"}
        )

        numbers_not_in_kms = self.df_oper.merge(
            self.df_kms, on=["Дата звонка", "Принимающий номер"], how="left", indicator=True
        )
        numbers_not_in_kms = numbers_not_in_kms[numbers_not_in_kms["_merge"] == "left_only"][
            ["Дата звонка", "Время звонка_x", "Принимающий номер", "Длительность_x"]
            ].rename(
            columns={"Время звонка_x": "Время звонка", "Длительность_x": "Длительность"}
        )

        within_delta_kms = within_delta[
            ["Дата звонка", "Время звонка_x", "Принимающий номер", "Длительность_x"]
        ].rename(columns={"Время звонка_x": "Время звонка", "Длительность_x": "Длительность"})

        other_calls_kms = self.find_non_matched_calls(self.df_kms, within_delta_kms, numbers_not_in_oper)

        within_delta_oper = within_delta[
            ["Дата звонка", "Время звонка_y", "Принимающий номер", "Длительность_y"]
        ].rename(columns={"Время звонка_y": "Время звонка", "Длительность_y": "Длительность"})

        other_calls_oper = self.find_non_matched_calls(self.df_oper, within_delta_oper, numbers_not_in_kms)

        out_of_delta = other_calls_kms.merge(
            other_calls_oper,
            on=["Дата звонка", "Принимающий номер"],
            suffixes=("_kms", "_oper"),
            how="outer",
            indicator=True,
        ).sort_values(by=["Дата звонка", "Время звонка_kms", "Время звонка_oper"])

        out_of_delta = self.process_out_of_delta(out_of_delta)

  
        numbers_not_in_oper["Время звонка"] = pd.to_datetime(numbers_not_in_oper["Время звонка"], unit="s", errors='coerce').dt.strftime("%H:%M:%S")
        numbers_not_in_oper["Длительность"] = pd.to_datetime(numbers_not_in_oper["Длительность"], unit="s", errors='coerce').dt.strftime("%H:%M:%S")

        numbers_not_in_kms["Время звонка"] = pd.to_datetime(numbers_not_in_kms["Время звонка"], unit="s", errors='coerce').dt.strftime("%H:%M:%S")
        numbers_not_in_kms["Длительность"] = pd.to_datetime(numbers_not_in_kms["Длительность"], unit="s", errors='coerce').dt.strftime("%H:%M:%S")

        within_delta["Время звонка_x"] = pd.to_datetime(within_delta["Время звонка_x"], unit="s", errors='coerce').dt.strftime("%H:%M:%S")
        within_delta["Длительность_x"] = pd.to_datetime(within_delta["Длительность_x"], unit="s", errors='coerce').dt.strftime("%H:%M:%S")
        within_delta["Время звонка_y"] = pd.to_datetime(within_delta["Время звонка_y"], unit="s", errors='coerce').dt.strftime("%H:%M:%S")
        within_delta["Длительность_y"] = pd.to_datetime(within_delta["Длительность_y"], unit="s", errors='coerce').dt.strftime("%H:%M:%S")

        return within_delta, out_of_delta, numbers_not_in_oper, numbers_not_in_kms

    def process_out_of_delta(self, out_of_delta):
        def update_merge(row):
            if row["_merge"] == "both" and pd.isna(row["Время звонка_kms"]) and pd.isna(row["Длительность_kms"]):
                return "right_only"
            elif row["_merge"] == "both" and pd.isna(row["Время звонка_oper"]) and pd.isna(row["Длительность_oper"]):
                return "left_only"
            return row["_merge"]

        out_of_delta["Время звонка_kms"] = pd.to_datetime(out_of_delta["Время звонка_kms"], unit="s", errors='coerce').dt.strftime("%H:%M:%S")
        out_of_delta["Время звонка_oper"] = pd.to_datetime(out_of_delta["Время звонка_oper"], unit="s", errors='coerce').dt.strftime("%H:%M:%S")
        out_of_delta["Длительность_kms"] = pd.to_datetime(out_of_delta["Длительность_kms"], unit="s", errors='coerce').dt.strftime("%H:%M:%S")
        out_of_delta["Длительность_oper"] = pd.to_datetime(out_of_delta["Длительность_oper"], unit="s", errors='coerce').dt.strftime("%H:%M:%S")

        out_of_delta["_merge"] = out_of_delta.apply(update_merge, axis=1)

        out_of_delta["Комментарий"] = out_of_delta["_merge"].map({
            "both": f"Звонок вне дельты ±{self.delta}сек.",
            "left_only": "Звонок из отчета корпоративной детализации",
            "right_only": "Звонок из отчета детализации оператора"
        })

        return out_of_delta[
            ["Дата звонка", "Время звонка_kms", "Длительность_kms", "Принимающий номер", "Время звонка_oper", "Длительность_oper", "Комментарий"]
        ]

    def save_reports_to_excel(self, within_delta, out_of_delta, numbers_not_in_oper, numbers_not_in_kms, output_path="reports.xlsx"):
        with pd.ExcelWriter(output_path) as writer:
            within_delta.to_excel(writer, sheet_name='within_delta', index=False)
            out_of_delta.to_excel(writer, sheet_name='out_of_delta', index=False)
            numbers_not_in_oper.to_excel(writer, sheet_name='numbers_not_in_oper', index=False)
            numbers_not_in_kms.to_excel(writer, sheet_name='numbers_not_in_kms', index=False)


beeline = Beeline("beel_kms.csv", "beel_oper.csv")
within_delta, out_of_delta, numbers_not_in_oper, numbers_not_in_kms = beeline.analyze_data()
beeline.save_reports_to_excel(within_delta, out_of_delta, numbers_not_in_oper, numbers_not_in_kms)

print("DataFrame within_delta:")
print(within_delta.head())

print("\nDataFrame out_of_delta:")
print(out_of_delta.head())

print("\nDataFrame numbers_not_in_oper:")
print(numbers_not_in_oper.head())

print("\nDataFrame numbers_not_in_kms:")
print(numbers_not_in_kms.head())



