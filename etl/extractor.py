# 標準庫
from datetime import datetime, timedelta

# 外部庫
from google.cloud import bigquery
from pandas import DataFrame

def get_midnight_timestamp():
    """
    計算前12小時的時間戳。

    返回:
        int: 前12小時的時間戳。
    """
    now = datetime.now()
    yesterday = now - timedelta(hours=12)
    return int(yesterday.timestamp())

class Extractor:
    """
    Extractor類用於從Google BigQuery中提取資料。

    屬性:
        client (bigquery.Client): 用於與BigQuery進行互動的客戶端物件。

    方法:
        __init__(project_id: str): 初始化Extractor物件並設置BigQuery客戶端。
        fetch_data_as_dataframe(query: str) -> pd.DataFrame: 執行SQL查詢並返回結果為pandas DataFrame。
        save_to_csv(dataframe: pd.DataFrame, file_path: str): 將DataFrame保存為CSV文件。
    """
    def __init__(self, project_id: str):
        """
        初始化Extractor物件。

        參數:
            project_id (str): Google Cloud專案ID，用於初始化BigQuery客戶端。
        """
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        print(f"new_timestamp: {get_midnight_timestamp()}")

    def fetch_data_as_dataframe(self, query: str) -> DataFrame:
        """
        執行SQL查詢並將結果轉換為pandas DataFrame。

        參數:
            query (str): 要執行的SQL查詢字串。

        返回:
            pd.DataFrame: 查詢結果的DataFrame表示。
        """
        # 檢查輸入是否為字串
        if not isinstance(query, str):
            raise TypeError("Query must be a string")

        query_job = self.client.query(query)
        dataframe = query_job.to_dataframe()
        return dataframe

    def extract_cola_data(self) -> DataFrame:
        """
        從 BigQuery 提取 Cola 表格的資料。

        返回:
            DataFrame: 包含 Cola 表格資料的 DataFrame。
        """
        query = f"SELECT DISTINCT * FROM `{self.project_id}.economy.New_cola_air_tickets_price` WHERE `總售價` IS NOT NULL AND `建立時間` > {get_midnight_timestamp()}"
        return self.fetch_data_as_dataframe(query)

    def extract_set_data(self) -> DataFrame:
        """
        從 BigQuery 提取 Set 表格的資料。

        返回:
            DataFrame: 包含 Set 表格資料的 DataFrame。
        """
        query = f"SELECT DISTINCT * FROM `{self.project_id}.economy.New_settour_air_tickets_price` WHERE `票面價格` IS NOT NULL AND CAST(crawl_time AS INT64) > {get_midnight_timestamp()}"
        return self.fetch_data_as_dataframe(query)

    def extract_lion_data(self) -> DataFrame:
        """
        從 BigQuery 提取 Lion 表格的資料。

        返回:
            DataFrame: 包含 Lion 表格資料的 DataFrame。
        """
        query = f"SELECT DISTINCT * FROM `{self.project_id}.economy.New_Lion_air_tickets_price` WHERE `票面價格` IS NOT NULL AND CAST(crawl_time AS INT64) > {get_midnight_timestamp()}"
        return self.fetch_data_as_dataframe(query)

    def extract_eztravel_data(self) -> DataFrame:
        """
        從 BigQuery 提取 Eztravel 表格的資料 (僅非海外供應商)。

        返回:
            DataFrame: 包含 Eztravel 表格資料的 DataFrame。
        """
        # 假設 productDesc = FALSE 代表非海外供應商
        query = f"SELECT DISTINCT * FROM `{self.project_id}.economy.New_Eztravel_air_tickets_price` WHERE `票面價格` IS NOT NULL AND CAST(crawl_time AS INT64) > {get_midnight_timestamp()} AND `海外供應商` = FALSE"
        return self.fetch_data_as_dataframe(query)

    def extract_foreign_supplier_eztravel_data(self) -> DataFrame:
        """
        從 BigQuery 易遊網表格提取有海外供應商的資料。

        返回:
            DataFrame: 包含海外供應商 Eztravel 表格資料的 DataFrame。
        """
        # 假設 productDesc = TRUE 代表海外供應商
        query = f"SELECT DISTINCT * FROM `{self.project_id}.economy.New_Eztravel_air_tickets_price` WHERE `票面價格` IS NOT NULL AND CAST(crawl_time AS INT64) > {get_midnight_timestamp()} AND `海外供應商` = TRUE"
        return self.fetch_data_as_dataframe(query)
    
        
    def extract_rich_data(self) -> DataFrame:
        """
        從 BigQuery 提取 Rich 表格的資料。

        返回:
            DataFrame: 包含 Rich 表格資料的 DataFrame。
        """
        query = f"SELECT DISTINCT * FROM `{self.project_id}.economy.New_richmond_air_tickets_price` WHERE `票面價格` IS NOT NULL AND CAST(crawl_time AS INT64) > {get_midnight_timestamp()}"
        return self.fetch_data_as_dataframe(query)