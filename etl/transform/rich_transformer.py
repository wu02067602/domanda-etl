# 外部庫
from pandas import DataFrame
import logging

# 本地庫
from etl.transform.base_transformer import BaseTransformer

class RichTransformer(BaseTransformer):
    """
    RichTransformer 類別負責處理特定的資料清洗邏輯。
    """

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        清洗資料的主要方法。

        參數：
        df (DataFrame): 需要清洗的資料框。

        回傳：
        DataFrame: 清洗後的資料框。
        """
        df = self.rename_columns_to_chinese(df)
        df = self._handle_flight_number(df)
        df = self._handle_date(df)
        return df

    def rename_columns_to_chinese(self, df: DataFrame) -> DataFrame:
        """
        將資料框的欄位名稱重新命名為中文。

        參數：
        df (DataFrame): 需要重新命名欄位的資料框。

        回傳：
        DataFrame: 欄位名稱重新命名後的資料框。
        """
        rename_columns = {}

        rename_columns['去程日期'] = '出發日期'
        rename_columns['回程日期'] = '返回日期'
        rename_columns['票面價格'] = 'rich_mond_air_tickets_price'
        rename_columns['稅金'] = 'rich_mond_tax'
        rename_columns['去程航班編號1'] = '去程_航班編號1'
        rename_columns['去程艙等1'] = '去程_艙等1'
        rename_columns['回程航班編號1'] = '回程_航班編號1'
        rename_columns['回程艙等1'] = '回程_艙等1'
        rename_columns['去程航班編號2'] = '去程_航班編號2'
        rename_columns['去程艙等2'] = '去程_艙等2'
        rename_columns['去程航班編號3'] = '去程_航班編號3'
        rename_columns['去程艙等3'] = '去程_艙等3'
        rename_columns['回程航班編號2'] = '回程_航班編號2'
        rename_columns['回程艙等2'] = '回程_艙等2'
        rename_columns['回程航班編號3'] = '回程_航班編號3'
        rename_columns['回程艙等3'] = '回程_艙等3'
        
        df.rename(columns=rename_columns, inplace=True)
        return df 
    
    def _handle_flight_number(self, df: DataFrame) -> DataFrame:
        """
        處理航班編號資料。
        """
        logger = logging.getLogger(__name__)
        candidate_cols = [
            *[f'去程_航班編號{i}' for i in range(1, 4)],
            *[f'回程_航班編號{i}' for i in range(1, 4)],
        ]
        flight_cols = [c for c in candidate_cols if c in df.columns]

        if not flight_cols:
            return df

        # 去除空白與轉大寫，並對 1/2 位數字補零至 3 碼
        for col in flight_cols:
            s = df[col].fillna("").astype(str)
            s = s.str.strip().str.replace(r"\s+", "", regex=True).str.upper()
            s = s.str.replace(r"^([A-Z0-9]{2})(\d{2})$", r"\g<1>0\g<2>", regex=True)
            s = s.str.replace(r"^([A-Z0-9]{2})(\d{1})$", r"\g<1>00\g<2>", regex=True)
            df[col] = s

        # 任一非空航班欄位不符合 2 英數字 + 3~4 數字則刪除該列
        invalid_row_mask = None
        for col in flight_cols:
            s = df[col].fillna("").astype(str)
            non_empty = s != ""
            valid_fmt = s.str.match(r"^[A-Z0-9]{2}\d{3,4}$", na=False)
            invalid_col_mask = non_empty & (~valid_fmt)
            invalid_row_mask = invalid_col_mask if invalid_row_mask is None else (invalid_row_mask | invalid_col_mask)

        if invalid_row_mask is not None and invalid_row_mask.any():
            for idx in df.index[invalid_row_mask]:
                offending = {col: df.at[idx, col] for col in flight_cols if isinstance(df.at[idx, col], str) and df.at[idx, col]}
                logger.warning("移除無效航班編號資料 idx=%s 欄位=%s", idx, offending)
            df = df[~invalid_row_mask]

        return df

    def _handle_date(self, df: DataFrame) -> DataFrame:
        """
        處理日期資料。
        """
        df['出發日期'] = df['出發日期'].str.slice(5,10).str.replace('-', '/')
        df['返回日期'] = df['返回日期'].str.slice(5,10).str.replace('-', '/')
        return df
