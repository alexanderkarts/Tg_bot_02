import pandas as pd

# ===== Колонки =====
FULL_COLUMNS = [
    "Номер ключа", "VIN", "Кол-во фото для сайта", "Модель", "Марка",
    "Место хранения", "Пробег", "Год выпуска", "Цветкузова", "Рег. номер",
    "Дней с даты поступления", "ДЦ приёма", "Цена продажи", "Цена приема",
    "Байер", "Тип сделки", "Тип кузова"
]

PHOTO_COLUMNS = [
    "Номер ключа", "VIN", "Модель", "Марка",
    "Пробег", "Год выпуска", "Цветкузова",
    "Рег. номер", "Дней с даты поступления",
    "ДЦ приёма", "Тип сделки"
]

NOT_SENT_COLUMNS = [
    "Номер ключа", "VIN", "Модель", "Марка",
    "Пробег", "Год выпуска", "Цветкузова",
    "Рег. номер", "Дней с даты поступления",
    "ДЦ приёма", "Байер", "Тип сделки"
]


# ===== Фильтры =====
def full_stock(df: pd.DataFrame) -> pd.DataFrame:
    return df[FULL_COLUMNS]


def without_photo(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["Кол-во фото для сайта"] == 0]
    df = df.sort_values(by="Дней с даты поступления", ascending=False)
    return df[PHOTO_COLUMNS]


def without_storage(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["Кол-во фото для сайта"] != 0]
    df = df[df["Место хранения"].isna() | (df["Место хранения"] == "")]
    df = df.sort_values(by="Дней с даты поступления", ascending=True)
    return df
