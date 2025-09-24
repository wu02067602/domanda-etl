# 外部庫
from pandas import DataFrame
import time

# 本地庫
from etl.transform.base_transformer import BaseTransformer
from scripts.unify_csv import (
    to_date_yyyy_slash_mm_slash_dd,
    split_luggage,
)


class ColaTransformer(BaseTransformer):
    """
    ColaTransformer
    採用 `scripts.unify_csv` 的清洗邏輯，將 Cola 原始資料規整為後續整併可使用的標準欄位格式。

    目標：
    - 以一致且可維護的方式，取得後續整併所需的關鍵中文欄位：
      `去程_航班編號{1..3}`、`回程_航班編號{1..3}`、`去程_艙等{1..3}`、`回程_艙等{1..3}`、`出發日期`、`返回日期`、`去程行李{1..3}`、`回程行李{1..3}`。
    - 日期採用 YYYY/MM/DD 格式，避免日後再推斷年份。

    注意：
    - 僅針對 Cola 做清洗更動；其他來源不受影響。
    - 行李欄位以 `unify_csv.split_luggage` 規整為無多餘空白、單位標準化（件 / 公斤）的字串，例如："1件"、"25公斤"。
    """

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        清洗傳入的 DataFrame，返回清洗後的 DataFrame。

        參數：
        - df：原始 Cola 資料表（DataFrame）。

        返回：
        - DataFrame：欄位與格式經規整後的 DataFrame。

        範例：
        - 產出欄位將包含（若原始資料具備）：`去程_航班編號1`、`回程_航班編號1`、`去程_艙等1`、`回程_艙等1`、`出發日期`、`返回日期`、`去程行李1`、`回程行李1` 等。
        """
        df = self._rename_columns_to_standard(df)
        df = self._normalize_cabin_class(df)
        df = self._normalize_luggage(df)
        df = self._handle_date(df)
        df = self._ensure_required_columns(df)
        df = self._ensure_metadata(df)
        return df

    def _rename_columns_to_standard(self, df: DataFrame) -> DataFrame:
        """
        將 Cola 原始欄位名稱重新命名為整併所需的標準欄位名稱。

        包含：
        - 航班編號／艙等（去/回程 x 最多 3 段）
        - 起飛/降落時間、機場、機型、飛行時間（與 `unified_transformer` 一致的底線命名）
        - 價格/稅金/總售價等欄位，對齊內部既有命名（如「基礎票價」→「機票價錢」）
        - GDS Type 改為 GDS_Type
        """
        rename_map = {}
        for i in (1, 2, 3):
            # 航班編號
            rename_map[f'去程航班編號{i}'] = f'去程_航班編號{i}'
            rename_map[f'回程航班編號{i}'] = f'回程_航班編號{i}'
            # 艙等
            rename_map[f'去程艙等與艙等編碼{i}'] = f'去程_艙等{i}'
            rename_map[f'回程艙等與艙等編碼{i}'] = f'回程_艙等{i}'
            # 時間
            rename_map[f'去程起飛時間{i}'] = f'去程_出發時間{i}'
            rename_map[f'去程降落時間{i}'] = f'去程_到達時間{i}'
            rename_map[f'回程起飛時間{i}'] = f'回程_出發時間{i}'
            rename_map[f'回程降落時間{i}'] = f'回程_到達時間{i}'
            # 機場
            rename_map[f'去程起飛機場{i}'] = f'去程_出發機場{i}'
            rename_map[f'去程降落機場{i}'] = f'去程_到達機場{i}'
            rename_map[f'回程起飛機場{i}'] = f'回程_出發機場{i}'
            rename_map[f'回程降落機場{i}'] = f'回程_到達機場{i}'
            # 機型
            rename_map[f'去程飛機公司及型號{i}'] = f'去程_機型{i}'
            rename_map[f'回程飛機公司及型號{i}'] = f'回程_機型{i}'
            # 飛行時間
            rename_map[f'去程飛行時間{i}'] = f'去程_飛行時間{i}'
            rename_map[f'回程飛行時間{i}'] = f'回程_飛行時間{i}'

        # 價格/稅金/其他
        rename_map['基礎票價'] = '機票價錢'
        rename_map['票價加價成數'] = '機票價錢加價成數'
        rename_map['總售價'] = '最終價格'
        rename_map['票型'] = '淨價或票面'
        rename_map['公式類型'] = '票價規則類型'
        rename_map['GDS Type'] = 'GDS_Type'
        rename_map['折讓百分比'] = 'KP'

        existing = {k: v for k, v in rename_map.items() if k in df.columns}
        if existing:
            df.rename(columns=existing, inplace=True)
        return df

    def _split_flight_and_class(self, df: DataFrame) -> DataFrame:
        """
        分割航班號與艙等為獨立欄位。

        簡介：
        - 針對 `去程_航班號{n}` 與 `回程_航班號{n}` 欄位（若存在），以空白分割為「航班編號」與「艙等」兩欄，
          並以 `去程_航班編號{n}`、`去程_艙等{n}`（或回程同名規則）寫回。
        - 若該欄皆為空，則建立對應之「航班編號」與「艙等」欄位為空值。

        參數：
        - df：包含航班與艙等資訊的 DataFrame。

        返回：
        - DataFrame：完成分割後的 DataFrame（原 `*_航班號*` 欄位會移除）。
        """
        def split_and_clean(column_prefix: str, df_in: DataFrame) -> None:
            for col in df_in.columns:
                if col.startswith(column_prefix):
                    suffix = col[-1]
                    flight_col = f"{column_prefix.replace('航班號', '')}航班編號{suffix}"
                    class_col = f"{column_prefix.replace('航班號', '')}艙等{suffix}"
                    if df_in[col].isnull().all():
                        df_in[flight_col] = None
                        df_in[class_col] = None
                    else:
                        df_in[[flight_col, class_col]] = df_in[col].str.split(' ', n=1, expand=True)
                        df_in[flight_col] = df_in[flight_col].str.strip()
                    df_in.drop(columns=[col], inplace=True)

        split_and_clean('去程_航班號', df)
        split_and_clean('回程_航班號', df)
        return df

    def _handle_date(self, df: DataFrame) -> DataFrame:
        """
        由第一段去回程起飛時間推導日期（MM/DD）與年份。

        作法：
        - 以 `unify_csv.to_date_yyyy_slash_mm_slash_dd` 解析 `去程_出發時間1` 與 `回程_出發時間1`，
          寫入 `出發日期` 與 `返回日期` 欄位，並設定 `出發年份` 與 `返回年份` 欄位。

        參數：
        - df：包含 `去程_出發時間1`、`回程_出發時間1` 的 DataFrame。

        返回：
        - DataFrame：新增/覆寫 `出發日期`、`返回日期`、`出發年份`、`返回年份` 後的 DataFrame。

        注意：
        - 此處設定年分的原因是因為在 unified_transformer 的最後一步中，會將 `出發日期` 與 `返回日期` 的年份設定為 `出發年份` 與 `返回年份`。
        """
        dep_src = df['去程_出發時間1'] if '去程_出發時間1' in df.columns else None
        ret_src = df['回程_出發時間1'] if '回程_出發時間1' in df.columns else None
        if dep_src is not None:
            df['出發日期'] = dep_src.apply(to_date_yyyy_slash_mm_slash_dd)
            df['出發年份'] = dep_src.apply(lambda x: x.split('-')[0])
            df['出發日期'] = df['出發日期'].str.slice(5,10).str.replace('-', '/')
        if ret_src is not None:
            df['返回日期'] = ret_src.apply(to_date_yyyy_slash_mm_slash_dd)
            df['返回年份'] = ret_src.apply(lambda x: x.split('-')[0])
            df['返回日期'] = df['返回日期'].str.slice(5,10).str.replace('-', '/')
        return df

    def _normalize_cabin_class(self, df: DataFrame) -> DataFrame:
        """
        規整艙等欄位字串（去除空白）。

        參數：
        - df：包含艙等欄位的 DataFrame。

        返回：
        - DataFrame：艙等欄位移除多餘空白後的 DataFrame。
        """
        for col in df.columns:
            if '艙等' in col:
                df[col] = df[col].astype(str).str.replace(' ', '', regex=False)
        return df

    def _normalize_luggage(self, df: DataFrame) -> DataFrame:
        """
        規整行李欄位字串：將數值與單位正規化並組回簡潔表示。

        作法：
        - 使用 `unify_csv.split_luggage` 解析每一個 `*行李{n}` 欄位的值為（數值, 單位）。
        - 若可解析到數值，重寫為「<數值><單位>」（例如："1件"、"25公斤"），否則設為空字串。

        參數：
        - df：包含 `去程行李{n}`、`回程行李{n}` 欄位的 DataFrame。

        返回：
        - DataFrame：行李欄位格式統一後的 DataFrame。
        """
        for col in list(df.columns):
            if '行李' in col:
                series = df[col]
                # 若整欄皆為空，跳過處理
                if series.isnull().all():
                    continue
                parsed = series.apply(lambda x: split_luggage(x))
                df[col] = parsed.apply(lambda t: (f"{int(t[0]) if t[0] is not None and float(t[0]).is_integer() else t[0]}{t[1]}" if t[0] is not None and t[1] else (f"{t[0]}" if t[0] is not None else '')))
        return df

    def _ensure_required_columns(self, df: DataFrame) -> DataFrame:
        """
        確保 join 需要的鍵欄位存在，不存在則以空值補齊。

        包含：
        - `去程_航班編號{1..3}`、`去程_艙等{1..3}`、`回程_航班編號{1..3}`、`回程_艙等{1..3}`
        - `出發日期`、`返回日期`
        """
        for i in (1, 2, 3):
            for base in (f'去程_航班編號{i}', f'去程_艙等{i}', f'回程_航班編號{i}', f'回程_艙等{i}'):
                if base not in df.columns:
                    df[base] = None
        if '出發日期' not in df.columns:
            df['出發日期'] = None
        if '返回日期' not in df.columns:
            df['返回日期'] = None
        return df

    def _ensure_metadata(self, df: DataFrame) -> DataFrame:
        """
        確保後續輸出所需的中繼欄位存在。

        - `建立時間`：若無則以當前 epoch 秒補上
        - `KP`：若無則以空字串補上
        """
        if '建立時間' not in df.columns:
            df['建立時間'] = time.time()
        if 'KP' not in df.columns:
            df['KP'] = ''
        return df