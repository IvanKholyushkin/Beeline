import pandas as pd
import numpy as np

class Beeline:
    def __init__(self, path_kms, path_oper, delimiter=";"):
        # Загрузка данных из файлов CSV
        self.df_kms = pd.read_csv(path_kms, delimiter=delimiter)
        self.df_oper = pd.read_csv(path_oper, delimiter=delimiter)
        self.delta = 3  # Дельта для сравнения времени и длительности звонка в секундах
        self.prepare_data()  # Подготовка данных (переименование столбцов, преобразование типов данных)

    def prepare_data(self):
        # Переименование столбцов для удобства работы
        columns_mapping = {
            "to_char": "Дата звонка",
            "to_char.1": "Время звонка",
            "phoneb": "Принимающий номер",
            "to_char.2": "Длительность",
            "?column?": "Длительность округленная до минут",
        }
        self.df_kms.rename(columns=columns_mapping, inplace=True)
        self.df_oper.rename(columns=columns_mapping, inplace=True)

        # Удаление дубликатов
        self.df_kms = self.df_kms.drop_duplicates(ignore_index=True)
        self.df_oper = self.df_oper.drop_duplicates(ignore_index=True)

        # Преобразование столбцов времени и длительности в секунды
        self.df_kms["Время звонка"] = (
            pd.to_timedelta(self.df_kms["Время звонка"]).dt.total_seconds().astype(int)
        )
        self.df_oper["Время звонка"] = (
            pd.to_timedelta(self.df_oper["Время звонка"]).dt.total_seconds().astype(int)
        )
        self.df_kms["Длительность"] = (
            pd.to_timedelta(self.df_kms["Длительность"]).dt.total_seconds().astype(int)
        )
        self.df_oper["Длительность"] = (
            pd.to_timedelta(self.df_oper["Длительность"]).dt.total_seconds().astype(int)
        )

    def create_within_delta(self, df, time_col, duration_col):
        # Создание DataFrame с необходимыми столбцами и переименование столбцов времени и длительности
        return df[
            ["Дата звонка", time_col, "Принимающий номер", duration_col]
        ].rename(columns={time_col: "Время звонка", duration_col: "Длительность"})

    def find_non_matched_calls(self, df_main, within_delta, numbers_not_in):
        # Поиск несовпадающих звонков в основном DataFrame
        non_matched_calls = df_main.merge(
            within_delta,
            on=["Дата звонка", "Время звонка", "Принимающий номер", "Длительность"],
            how="left",
            indicator=True,
        )
        # Оставляем только те звонки, которые есть только в основном DataFrame
        non_matched_calls = non_matched_calls[
            non_matched_calls["_merge"] == "left_only"
        ][["Дата звонка", "Время звонка", "Принимающий номер", "Длительность"]]

        # Поиск несовпадающих звонков во втором DataFrame
        other_calls = non_matched_calls.merge(
            numbers_not_in,
            on=["Дата звонка", "Время звонка", "Принимающий номер", "Длительность"],
            how="left",
            indicator=True,
        )
        return other_calls[
            other_calls["_merge"] == "left_only"
        ][["Дата звонка", "Время звонка", "Принимающий номер", "Длительность"]]

    def process_out_of_delta(self, out_of_delta):
        # Обработка звонков, не попавших в дельту
        mask1 = out_of_delta.duplicated(
            subset=["Дата звонка", "Время звонка_kms", "Длительность_kms", "Принимающий номер"],
            keep="first",
        ) & (out_of_delta["_merge"] == "both")
        out_of_delta.loc[mask1, ["Время звонка_kms", "Длительность_kms"]] = pd.NA

        mask2 = out_of_delta.duplicated(
            subset=["Дата звонка", "Принимающий номер", "Время звонка_oper", "Длительность_oper"],
            keep="first",
        ) & (out_of_delta["_merge"] == "both")
        out_of_delta.loc[mask2, ["Время звонка_oper", "Длительность_oper"]] = pd.NA

        # Преобразование времени и длительности из секунд в формат HH:MM:SS
        out_of_delta["Время звонка_kms"] = pd.to_datetime(out_of_delta["Время звонка_kms"], unit="s").dt.strftime("%H:%M:%S")
        out_of_delta["Время звонка_oper"] = pd.to_datetime(out_of_delta["Время звонка_oper"], unit="s").dt.strftime("%H:%M:%S")
        out_of_delta["Длительность_kms"] = pd.to_datetime(out_of_delta["Длительность_kms"], unit="s").dt.strftime("%H:%M:%S")
        out_of_delta["Длительность_oper"] = pd.to_datetime(out_of_delta["Длительность_oper"], unit="s").dt.strftime("%H:%M:%S")

        return out_of_delta[
            ["Дата звонка", "Время звонка_kms", "Длительность_kms", "Принимающий номер", "Время звонка_oper", "Длительность_oper"]
        ]

    def get_within_delta(self):
        # Поиск совпадающих звонков с учетом дельты
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

    def analyze_data(self):
        # Получение совпадающих звонков с учетом дельты
        within_delta = self.get_within_delta()
        print(
            f"Количество совпавших звонков: {within_delta.shape[0]} с учётом дельты +-{self.delta}(сек.) по времени и длительности звонка."
        )
        print(
            f"Процент совпадений звонков в файле KMS с учётом дельты +-{self.delta}(сек.): {round(within_delta.shape[0] / self.df_kms.shape[0] * 100, 1)}%"
        )
        print(
            f"Процент совпадений звонков в файле OPER с учётом дельты +-{self.delta}(сек.): {round(within_delta.shape[0] / self.df_oper.shape[0] * 100, 1)}%"
        )

        # Поиск номеров, отсутствующих в одном из файлов
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

        print(
            f"Кол-во номеров в файле KMS отсутствующих в файле OPER: {numbers_not_in_oper.shape[0]}"
        )
        print(
            f"Процент номеров в файле KMS отсутствующих в файле OPER: {round(numbers_not_in_oper.shape[0] / self.df_kms.shape[0] * 100, 1)}%"
        )
        print(
            f"Кол-во номеров в файле OPER отсутствующих в файле KMS: {numbers_not_in_kms.shape[0]}"
        )
        print(
            f"Процент номеров в файле OPER отсутствующих в файле KMS: {round(numbers_not_in_kms.shape[0] / self.df_kms.shape[0] * 100, 1)}%"
        )

        # Создание DataFrame с совпадающими звонками
        within_delta_kms = self.create_within_delta(within_delta, "Время звонка_x", "Длительность_x")
        within_delta_oper = self.create_within_delta(within_delta, "Время звонка_y", "Длительность_y")

        # Поиск звонков, не попавших в дельту
        other_calls_kms = self.find_non_matched_calls(self.df_kms, within_delta_kms, numbers_not_in_oper)
        other_calls_oper = self.find_non_matched_calls(self.df_oper, within_delta_oper, numbers_not_in_kms)

        out_of_delta = other_calls_kms.merge(
            other_calls_oper,
            on=["Дата звонка", "Принимающий номер"],
            suffixes=("_kms", "_oper"),
            how="outer",
            indicator=True,
        )

        # Обработка звонков, не попавших в дельту
        out_of_delta_processed = self.process_out_of_delta(out_of_delta)
        return out_of_delta_processed

# Пример вызова класса
beeline = Beeline("beel_kms.csv", "beel_oper.csv")
out_of_delta = beeline.analyze_data()


