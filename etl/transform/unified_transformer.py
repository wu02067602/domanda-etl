import pandas as pd
from pandas import DataFrame
import numpy as np
from scripts.unify_csv import (
    extract_airline_code,
    to_time_hhmm,
    duration_to_minutes,
    split_luggage,
)
class UnifiedTransformer:
    """
    UnifiedTransformer 類負責整合來自各個 Transformer 的清洗結果，並進行最後的欄位對齊、join 價格稅金等。
    """

    def unify_data(self, cola_df: DataFrame, set_df: DataFrame, lion_df: DataFrame ,eztravel_df: DataFrame, foreign_supplier_eztravel_df: DataFrame, rich_df: DataFrame) -> DataFrame:
        """
        整合各來源的清洗結果並產生標準欄位。

        簡介：
        - 先以航班編號、艙等與去回程日期進行 join，彙整其他供應商的票價/稅金。
        - 再以 `unify_csv` 的轉換函式輸出最終欄位（時間、行李、航司代碼、飛行時間等）。

        參數：
        - cola_df：Cola 清洗後資料。
        - set_df：東南清洗後資料。
        - lion_df：雄獅清洗後資料。
        - eztravel_df：易遊網（非海外）清洗後資料。
        - foreign_supplier_eztravel_df：易遊網（海外）清洗後資料。
        - rich_df：山富清洗後資料。

        返回：
        - DataFrame：整合且欄位對齊的最終表格。
        """
        unified_df = self.join_price_and_tax(cola_df, set_df, lion_df, eztravel_df, foreign_supplier_eztravel_df, rich_df)
        unified_df = self._handle_date(unified_df)
        unified_df = self._rename_columns(unified_df)
        unified_df = self._remove_no_tax_data(unified_df)
        unified_df = self._blank_strings_to_nan(unified_df)
        return unified_df

    def join_price_and_tax(self, cola_df: DataFrame, set_df: DataFrame, lion_df: DataFrame, eztravel_df: DataFrame, foreign_supplier_eztravel_df: DataFrame, rich_df: DataFrame) -> DataFrame:
        """
        將各供應商的票價與稅金資訊依航班/艙等/日期進行關聯。

        參數：
        - cola_df：Cola 清洗後 DataFrame。
        - set_df：東南清洗後 DataFrame。
        - lion_df：雄獅清洗後 DataFrame。
        - eztravel_df：易遊網（非海外）清洗後 DataFrame。
        - foreign_supplier_eztravel_df：易遊網（海外）清洗後 DataFrame。
        - rich_df：山富清洗後 DataFrame。

        返回：
        - DataFrame：已關聯票價/稅金的暫存表。
        """
        # 檢查 set_df, lion_df, eztravel_df, foreign_supplier_eztravel_df, rich_df 是否包含指定的欄位，若缺少則添加並填充空值
        required_columns = (
            [f'去程_航班編號{i}' for i in range(1, 4)] +
            [f'去程_艙等{i}' for i in range(1, 4)] +
            [f'回程_航班編號{i}' for i in range(1, 4)] +
            [f'回程_艙等{i}' for i in range(1, 4)]
        )
        for column in required_columns:
            if column not in set_df.columns:
                set_df[column] = pd.NA
            if column not in lion_df.columns:
                lion_df[column] = pd.NA
            if column not in eztravel_df.columns:
                eztravel_df[column] = pd.NA
            if column not in foreign_supplier_eztravel_df.columns:
                foreign_supplier_eztravel_df[column] = pd.NA
            if column not in rich_df.columns:
                rich_df[column] = pd.NA

        # 根據航班編號、艙等和日期進行 join，並使用 suffixes 區分可樂以外的欄位
        join_keys = required_columns + ['出發日期', '返回日期']

        # 正規化函式：
        # - 將缺值與字面上的 "nan"/"none"/"<na>"/"null"/"nat"/"nat"/"NaT" 視為空字串
        # - 去除前後空白、合併多餘空白、轉為大寫
        # - 日期欄位嘗試解析後統一輸出為 MM/DD（盡量對齊各來源的常見格式）
        def _normalize_df_for_join(df: DataFrame) -> DataFrame:
            df = df.copy()
            placeholders = {"", "nan", "none", "<na>", "null", "nat", "nat", "nat"}
            flight_number_cols = [
                *[f'去程_航班編號{i}' for i in range(1, 4)],
                *[f'回程_航班編號{i}' for i in range(1, 4)],
            ]
            cabin_class_cols = [
                *[f'去程_艙等{i}' for i in range(1, 4)],
                *[f'回程_艙等{i}' for i in range(1, 4)],
            ]
            for col in join_keys:
                if col not in df.columns:
                    df[col] = pd.NA
                s = df[col].astype(str)
                s = s.str.strip()
                s = s.str.replace(r"\s+", " ", regex=True)
                sl = s.str.lower()
                s = s.where(~sl.isin(placeholders), '')
                # 將 'nat'/'nat'/'NaT' 等在大小寫轉換後也涵蓋
                s = s.str.upper()
                # 航班編號：移除內部空白，例如 'CX 450' -> 'CX450'
                if col in flight_number_cols:
                    s = s.str.replace(r"\s+", "", regex=True)
                # 艙等：也移除內部空白，例如 '經濟艙 K' -> '經濟艙K'
                if col in cabin_class_cols:
                    s = s.str.replace(r"\s+", "", regex=True)
                df[col] = s
            # 日期特別處理：標準化為 MM/DD
            for dcol in ['出發日期', '返回日期']:
                if dcol in df.columns:
                    s = df[dcol].astype(str)
                    s = s.str.replace('.', '/', regex=False).str.replace('-', '/', regex=False).str.strip()
                    # 去除前綴或尾綴的年份，僅保留月日
                    s = s.str.replace(r'^\s*\d{4}\s*/', '', regex=True)
                    s = s.str.replace(r'/\s*\d{4}\s*$', '', regex=True)
                    # 將 M/D 規範為 MM/DD（零補齊）
                    s = s.str.replace(r'^\s*(\d{1,2})\s*/\s*(\d{1,2})\s*$', lambda m: f"{int(m.group(1)):02d}/{int(m.group(2)):02d}", regex=True)
                    dt = pd.to_datetime(s, format='%m/%d', errors='coerce')
                    # 將可解析者改為 MM/DD，無法解析者維持原值
                    formatted = dt.dt.strftime('%m/%d')
                    df[dcol] = s.where(dt.isna(), formatted)
            return df

        cola_df = _normalize_df_for_join(cola_df)
        set_df = _normalize_df_for_join(set_df)
        lion_df = _normalize_df_for_join(lion_df)
        eztravel_df = _normalize_df_for_join(eztravel_df)
        foreign_supplier_eztravel_df = _normalize_df_for_join(foreign_supplier_eztravel_df)
        rich_df = _normalize_df_for_join(rich_df)
        unified_df = cola_df.merge(set_df, on=join_keys, how='left', suffixes=('', '_set'))
        unified_df = unified_df.merge(lion_df, on=join_keys, how='left', suffixes=('', '_lion'))
        unified_df = unified_df.merge(eztravel_df, on=join_keys, how='left', suffixes=('', '_eztravel'))
        unified_df = unified_df.merge(foreign_supplier_eztravel_df, on=join_keys, how='left', suffixes=('', '_f_eztravel'))
        unified_df = unified_df.merge(rich_df, on=join_keys, how='left', suffixes=('', '_rich'))

        # 去除有標籤的指定欄位 (join_keys 欄位在合併後可能帶有後綴)
        for column in join_keys:
            if column + '_set' in unified_df.columns:
                unified_df = unified_df.drop(columns=[column + '_set'])
            if column + '_lion' in unified_df.columns:
                unified_df = unified_df.drop(columns=[column + '_lion'])
            if column + '_eztravel' in unified_df.columns:
                unified_df = unified_df.drop(columns=[column + '_eztravel'])
            if column + '_f_eztravel' in unified_df.columns:
                unified_df = unified_df.drop(columns=[column + '_f_eztravel'])
            if column + '_rich' in unified_df.columns:
                unified_df = unified_df.drop(columns=[column + '_rich'])

        # 去除剩下的標籤
        unified_df.columns = [col.replace('_set', '').replace('_lion', '').replace('_eztravel', '').replace('_f_eztravel', '').replace('_rich', '') for col in unified_df.columns]

        return unified_df
    
    def _rename_columns(self, df: DataFrame) -> DataFrame:
        """
        轉換欄位為最終輸出格式，並導入 `unify_csv` 的規格化邏輯。

        參數：
        - df：join 後的暫存 DataFrame（中文表頭 + 供應商票價稅金欄位）。

        返回：
        - DataFrame：欄位命名與型態對齊的輸出。
        """
        # 建立新的 DataFrame 來存放轉換後的資料
        new_df = pd.DataFrame()
        
        # 航空公司代碼：改以航班編號解析（與 unify_csv 一致）
        for i in range(1, 4):
            dep_fn_col = f'去程_航班編號{i}'
            ret_fn_col = f'回程_航班編號{i}'
            new_df[f'departure_airline_{i}'] = (
                df[dep_fn_col].apply(extract_airline_code) if dep_fn_col in df.columns else None
            )
            new_df[f'return_airline_{i}'] = (
                df[ret_fn_col].apply(extract_airline_code) if ret_fn_col in df.columns else None
            )
        
        # 機場代碼轉換
        for i in range(1, 4):
            # 去程機場
            if f'去程_出發機場{i}' in df.columns:
                new_df[f'departure_airport_{i}'] = df[f'去程_出發機場{i}'].fillna('').astype(str).str.split().str[0]
            else:
                new_df[f'departure_airport_{i}'] = None
                
            # 去程到達機場
            if f'去程_到達機場{i}' in df.columns:
                new_df[f'departure_arrival_airport_{i}'] = df[f'去程_到達機場{i}'].fillna('').astype(str).str.split().str[0]
            else:
                new_df[f'departure_arrival_airport_{i}'] = None
                
            # 回程機場
            if f'回程_出發機場{i}' in df.columns:
                new_df[f'return_airport_{i}'] = df[f'回程_出發機場{i}'].fillna('').astype(str).str.split().str[0]
            else:
                new_df[f'return_airport_{i}'] = None
                
            # 回程到達機場
            if f'回程_到達機場{i}' in df.columns:
                new_df[f'return_arrival_airport_{i}'] = df[f'回程_到達機場{i}'].fillna('').astype(str).str.split().str[0]
            else:
                new_df[f'return_arrival_airport_{i}'] = None
        
        # 時間轉換：使用 unify_csv.to_time_hhmm
        for i in range(1, 4):
            # 去程/回程出發與到達時間
            new_df[f'departure_flight_time_{i}'] = (
                df[f'去程_出發時間{i}'].apply(to_time_hhmm) if f'去程_出發時間{i}' in df.columns else None
            )
            new_df[f'departure_arrival_flight_time_{i}'] = (
                df[f'去程_到達時間{i}'].apply(to_time_hhmm) if f'去程_到達時間{i}' in df.columns else None
            )
            new_df[f'return_flight_time_{i}'] = (
                df[f'回程_出發時間{i}'].apply(to_time_hhmm) if f'回程_出發時間{i}' in df.columns else None
            )
            new_df[f'return_arrival_flight_time_{i}'] = (
                df[f'回程_到達時間{i}'].apply(to_time_hhmm) if f'回程_到達時間{i}' in df.columns else None
            )
        
        # 機型轉換
        for i in range(1, 4):
            # 去程機型
            if f'去程_機型{i}' in df.columns:
                new_df[f'departure_aircraft_type_{i}'] = df[f'去程_機型{i}']
            else:
                new_df[f'departure_aircraft_type_{i}'] = None
                
            # 回程機型
            if f'回程_機型{i}' in df.columns:
                new_df[f'return_aircraft_type_{i}'] = df[f'回程_機型{i}']
            else:
                new_df[f'return_aircraft_type_{i}'] = None
    
        # 行李：使用 unify_csv.split_luggage 拆為數值與單位
        for i in range(1, 4):
            dep_col = f'去程行李{i}'
            ret_col = f'回程行李{i}'
            if dep_col in df.columns:
                parsed = df[dep_col].apply(split_luggage)
                new_df[f'departure_luggage_value_{i}'] = parsed.apply(lambda t: t[0] if t and len(t) > 0 else None)
                new_df[f'departure_luggage_unit_{i}'] = parsed.apply(lambda t: t[1] if t and len(t) > 1 else None)
            else:
                new_df[f'departure_luggage_value_{i}'] = None
                new_df[f'departure_luggage_unit_{i}'] = None
            if ret_col in df.columns:
                parsed = df[ret_col].apply(split_luggage)
                new_df[f'return_luggage_value_{i}'] = parsed.apply(lambda t: t[0] if t and len(t) > 0 else None)
                new_df[f'return_luggage_unit_{i}'] = parsed.apply(lambda t: t[1] if t and len(t) > 1 else None)
            else:
                new_df[f'return_luggage_value_{i}'] = None
                new_df[f'return_luggage_unit_{i}'] = None

        # 飛行時間：使用 unify_csv.duration_to_minutes
        for i in range(1, 4):
            new_df[f'departure_flight_duration_{i}'] = (
                df[f'去程_飛行時間{i}'].apply(duration_to_minutes) if f'去程_飛行時間{i}' in df.columns else None
            )
            new_df[f'return_flight_duration_{i}'] = (
                df[f'回程_飛行時間{i}'].apply(duration_to_minutes) if f'回程_飛行時間{i}' in df.columns else None
            )
        
        # 航班編號轉換
        for i in range(1, 4):
            # 去程航班編號
            if f'去程_航班編號{i}' in df.columns:
                new_df[f'departure_flight_number_{i}'] = df[f'去程_航班編號{i}']
            else:
                new_df[f'departure_flight_number_{i}'] = None
                
            # 回程航班編號
            if f'回程_航班編號{i}' in df.columns:
                new_df[f'return_flight_number_{i}'] = df[f'回程_航班編號{i}']
            else:
                new_df[f'return_flight_number_{i}'] = None
        
        # 艙等轉換
        for i in range(1, 4):
            # 去程艙等
            if f'去程_艙等{i}' in df.columns:
                new_df[f'departure_cabin_class_{i}'] = df[f'去程_艙等{i}']
            else:
                new_df[f'departure_cabin_class_{i}'] = None
                
            # 回程艙等
            if f'回程_艙等{i}' in df.columns:
                new_df[f'return_cabin_class_{i}'] = df[f'回程_艙等{i}']
            else:
                new_df[f'return_cabin_class_{i}'] = None
        
        # 轉機次數計算（至少為 0）
        for col in df.columns:
            if '航班編號' in col:
                # 僅將空字串或全空白的字串轉為 None，其他值維持不動
                df[col] = df[col].apply(
                    lambda v: (np.nan if isinstance(v, str) and v.strip() == '' else v)
                )
        dep_legs = df[[f'去程_航班編號{i}' for i in range(1, 4)]]
        ret_legs = df[[f'回程_航班編號{i}' for i in range(1, 4)]]
        new_df['departure_transfer_count'] = (dep_legs.notna().sum(axis=1) - 1).clip(lower=0)
        new_df['return_transfer_count'] = (ret_legs.notna().sum(axis=1) - 1).clip(lower=0)

        # GDS類型轉換
        new_df['gds_type'] = df['GDS_Type']
        
        # 價格相關欄位轉換
        new_df['ticket_price'] = df['機票價錢']
        new_df['ticket_price_markup_percentage'] = df['機票價錢加價成數']
        new_df['tax'] = df['稅金']
        new_df['tax_markup_percentage'] = df['稅金加價成數']
        new_df['final_price'] = df['最終價格']
        
        # 日期轉換：保留中文來源並標準化為 YYYY/MM/DD；建立時間沿用原值
        new_df['departure_date'] = df['出發日期']
        new_df['return_date'] = df['返回日期']
        new_df['creation_time'] = df['建立時間']
        
        # 其他供應商價格和稅金
        supplier_mapping = {
            'ezfly':                     {'price': 'ezfly_ticket_price', 
                                          'tax': 'ezfly_tax',
                                          'tax_markup_percentage': 'ezfly_tax_markup_percentage'},
            'eztravel':                  {'price': 'eztravel_ticket_air_tickets_price', 
                                          'tax': 'eztravel_tax',
                                          'tax_markup_percentage': 'eztravel_tax_markup_percentage'},
            'foreign_supplier_eztravel': {'price': 'foreign_supplier_eztraval_ticket_air_tickets_price', 
                                          'tax': 'foreign_supplier_eztraval_tax',
                                          'tax_markup_percentage': 'foreign_supplier_eztraval_tax_markup_percentage'},
            'lion':                      {'price': 'lion_air_tickets_price', 
                                          'tax': 'lion_tax',
                                          'tax_markup_percentage': 'lion_tax_markup_percentage'},
            'settour':                   {'price': 'settour_air_tickets_price', 
                                          'tax': 'settour_tax',
                                          'tax_markup_percentage': 'settour_tax_markup_percentage'},
            'rich':                      {'price': 'rich_mond_air_tickets_price', 
                                          'tax': 'rich_mond_tax',
                                          'tax_markup_percentage': 'rich_mond_tax_markup_percentage'}
        }
        
        for supplier, columns in supplier_mapping.items():
            # 票價
            if columns['price'] in df.columns:
                new_df[columns['price']] = df[columns['price']].apply(lambda x: int(x) if pd.notnull(x) and np.isfinite(x) else x)
            else:
                new_df[columns['price']] = None
                
            # 稅金
            if columns['tax'] in df.columns:
                new_df[columns['tax']] = df[columns['tax']].apply(lambda x: int(x) if pd.notnull(x) and np.isfinite(x) else x)
            else:
                new_df[columns['tax']] = None
        
        # 淨價或票面轉換
        new_df['net_price_or_ticket_price'] = df['淨價或票面']

        # 票價規則類型
        new_df['ticket_rule_type'] = df['票價規則類型']

        # KP
        new_df['kp'] = df['KP']

        # 折扣
        new_df['discount'] = df['折扣']

        # 固定公司要賺的利潤
        new_df['activity_fee_adjustment'] = df['固定金額']

        return new_df

    def _remove_no_tax_data(self, df: DataFrame) -> DataFrame:
        """
        去除雄獅、東南、易遊網、山富稅金都沒有任何資料的資料列。

        參數：
            df (DataFrame): 要處理的 DataFrame。

        返回：
            DataFrame: 處理後的 DataFrame。
        """
        df = df[df['lion_tax'].notna() | df['settour_tax'].notna() | df['eztravel_tax'].notna() | df['rich_mond_tax'].notna() | df['foreign_supplier_eztraval_tax'].notna()]
        return df

    def _handle_date(self, df: DataFrame) -> DataFrame:
        """
        將 `出發日期` 與 `返回日期` 的年份設定為 `出發年份` 與 `返回年份`。

        參數：
        - df：包含 `出發日期`、`返回日期`、`出發年份` 與 `返回年份` 的 DataFrame。

        返回：
        - DataFrame：新增/覆寫 `出發日期` 與 `返回日期` 後的 DataFrame。

        注意：
        - 此函數是為了解決在 join_price_and_tax 中，`出發日期` 與 `返回日期` 的格式必須為 MM/DD ，但是最終輸出需要為 YYYY/MM/DD 的問題。
        """
        # 將 `出發日期` 與 `返回日期` 的年份設定為 `出發年份` 與 `返回年份`。
        df['出發日期'] = df['出發年份'] + '/' + df['出發日期']
        df['返回日期'] = df['返回年份'] + '/' + df['返回日期']
        
        # 刪除 `出發年份` 與 `返回年份`。
        df = df.drop(columns=['出發年份', '返回年份'])
        return df

    def _blank_strings_to_nan(self, df: DataFrame) -> DataFrame:
        """
        將 df 中所有空字串或全空白的字串轉為空。
        """
        df = df.copy()
        return df.map(lambda v: (np.nan if isinstance(v, str) and v.strip() == '' else v))