# 外部庫
from pandas import DataFrame
import logging

# 本地庫
from etl.transform.base_transformer import BaseTransformer

class ForeignSupplierEztravelTransformer(BaseTransformer):
    '''
    ForeignSupplierEztravelTransformer 類別負責處理海外供應商 Eztravel 資料的特定清洗邏輯。
    '''

    def clean_data(self, df: DataFrame) -> DataFrame:
        '''
        清洗資料的主要方法。

        參數：
        df (DataFrame): 需要清洗的資料框。

        回傳：
        DataFrame: 清洗後的資料框。
        '''
        df = self.rename_columns_to_final_target(df)
        df = self._handle_flight_number(df)
        df = self._handle_date(df) # 假設日期處理邏輯與 EztravelTransformer 相似
        # 您可以在這裡加入更多針對海外供應商的特定清理步驟
        return df

    def rename_columns_to_final_target(self, df: DataFrame) -> DataFrame:
        '''
        將資料框的欄位名稱重新命名為最終目標格式，以符合 UnifiedTransformer 中的 supplier_mapping。

        參數：
        df (DataFrame): 需要重新命名欄位的資料框。

        回傳：
        DataFrame: 欄位名稱重新命名後的資料框。
        '''
        rename_map = {}

        # 假設從 BigQuery 過來的原始欄位名與 EztravelTransformer 處理的原始欄位名相似
        # 主要目標是將價格和稅金欄位重命名為 UnifiedTransformer.supplier_mapping 中定義的名稱
        rename_map['票面價格'] = 'foreign_supplier_eztraval_ticket_air_tickets_price'
        rename_map['稅金'] = 'foreign_supplier_eztraval_tax'
        
        # 其他共通欄位，與 EztravelTransformer 中的 rename_columns_to_chinese 相似
        # 這些是 join keys 和其他可能需要的描述性欄位
        rename_map['去程日期'] = '出發日期'
        rename_map['回程日期'] = '返回日期'
        rename_map['去程航班編號1'] = '去程_航班編號1'
        rename_map['去程艙等1'] = '去程_艙等1'
        rename_map['回程航班編號1'] = '回程_航班編號1'
        rename_map['回程艙等1'] = '回程_艙等1'
        rename_map['去程航班編號2'] = '去程_航班編號2'
        rename_map['去程艙等2'] = '去程_艙等2'
        rename_map['去程航班編號3'] = '去程_航班編號3'
        rename_map['去程艙等3'] = '去程_艙等3'
        rename_map['回程航班編號2'] = '回程_航班編號2'
        rename_map['回程艙等2'] = '回程_艙等2'
        rename_map['回程航班編號3'] = '回程_航班編號3'
        rename_map['回程艙等3'] = '回程_艙等3'
        rename_map['海外供應商'] = '海外供應商'
        # productDesc 欄位通常不需要重命名，因為 UnifiedTransformer 可能還需要它來做其他判斷或只是傳遞
        # rename_map['productDesc'] = 'productDesc' # 如果需要的話
        
        df.rename(columns=rename_map, inplace=True)
        return df 
    
    def _handle_flight_number(self, df: DataFrame) -> DataFrame:
        '''
        處理航班編號資料。
        '''
        logger = logging.getLogger(__name__)
        candidate_cols = [
            *[f'去程_航班編號{i}' for i in range(1, 4)],
            *[f'回程_航班編號{i}' for i in range(1, 4)],
        ]
        flight_cols = [c for c in candidate_cols if c in df.columns]

        if not flight_cols:
            return df

        for col in flight_cols:
            s = df[col].fillna("").astype(str)
            s = s.str.strip().str.replace(r"\s+", "", regex=True).str.upper()
            s = s.str.replace(r"^([A-Z0-9]{2})(\d{2})$", r"\g<1>0\g<2>", regex=True)
            s = s.str.replace(r"^([A-Z0-9]{2})(\d{1})$", r"\g<1>00\g<2>", regex=True)
            df[col] = s

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
        '''
        處理日期資料。
        與 EztravelTransformer 中的 _handle_date 相同。
        '''
        if '出發日期' in df.columns:
            df['出發日期'] = df['出發日期'].astype(str).str.slice(5,10).str.replace('-', '/')
        if '返回日期' in df.columns:
            df['返回日期'] = df['返回日期'].astype(str).str.slice(5,10).str.replace('-', '/')
        return df 