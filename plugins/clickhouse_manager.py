from datetime import datetime
from typing import Optional
import logging

logger = logging.getLogger("airflow.task")


class ClickHouseManager:
    def __init__(self, spark, jdbc_url: str, user: str, password: str, database: str = "default"):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.user = user
        self.password = password
        self.database = database
        self.driver = "com.clickhouse.jdbc.ClickHouseDriver"

    def table_exists(self, table_name: str) -> bool:
        """Проверяет существование таблицы в ClickHouse"""
        try:
            query = f"""
                SELECT 1 
                FROM system.tables 
                WHERE database = '{self.database}' 
                AND name = '{table_name}'
                LIMIT 1
            """
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("user", self.user) \
                .option("password", self.password) \
                .option("driver", self.driver) \
                .option("query", query) \
                .load()

            return df.count() > 0
        except Exception as e:
            print(f"Ошибка при проверке существования таблицы: {str(e)}")
            return False

    def get_max_updated_at(self, table_name: str) -> Optional[datetime]:
        """Получает максимальное значение updated_at из указанной таблицы"""
        if not self.table_exists(table_name):
            print(f"Таблица {table_name} не существует")
            return None

        try:
            query = f"""
                SELECT MAX(updated_at) AS max_updated_at 
                FROM {self.database}.{table_name}
            """
            max_timestamp_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("user", self.user) \
                .option("password", self.password) \
                .option("driver", self.driver) \
                .option("query", query) \
                .load()

            if max_timestamp_df.collect()[0]["max_updated_at"] == "":
                print(f"Таблица {table_name} существует, но пустая")
                return None

            max_updated = max_timestamp_df.collect()[0]["max_updated_at"]
            print(f"Максимальная дата в таблице {table_name}: {max_updated}")
            return max_updated
        except Exception as e:
            print(f"Ошибка при получении максимальной даты обновления: {str(e)}")
            return None

    def get_table_columns(self, table_name: str) -> Optional[list]:
        """Получает список колонок таблицы"""
        if not self.table_exists(table_name):
            return None

        try:
            query = f"""
                SELECT name 
                FROM system.columns 
                WHERE database = '{self.database}' 
                AND table = '{table_name}'
            """
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("user", self.user) \
                .option("password", self.password) \
                .option("driver", self.driver) \
                .option("query", query) \
                .load()

            return [row["name"] for row in df.collect()]
        except Exception as e:
            print(f"Ошибка при получении колонок таблицы: {str(e)}")
            return None