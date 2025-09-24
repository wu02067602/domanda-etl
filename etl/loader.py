from sqlalchemy import create_engine, text
import pandas as pd
import traceback
import numpy as np
import logging
from datetime import datetime
import os
from google.cloud.sql.connector import Connector, IPTypes

class Loader:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._create_connection()

    def load_to_cloud_sql(self, df):
        """
        將 DataFrame 寫入 Cloud SQL。

        參數：
        df (DataFrame): 需要寫入的資料。
        """
        if df is None or df.empty:
            raise ValueError("DataFrame 不能為空")
            
        try:
            # 將所有 NaN 值替換為 None
            df = df.replace({np.nan: None})
            
            # 過濾掉 gds_type 為空的資料
            original_len = len(df)
            df = df[df['gds_type'].notna()]
            filtered_len = len(df)
            if filtered_len < original_len:
                self.logger.info(f"已過濾掉 {original_len - filtered_len} 筆 gds_type 為空的資料")
            
            table_name = 'domanda.flight_ticket_price_compare'
            self.logger.info(f"準備寫入 {len(df)} 筆資料到 {table_name}")
            self.logger.info(f"DataFrame 的欄位：{df.columns.tolist()}")
            
            # 構建 INSERT 語句
            columns = ', '.join(df.columns)
            values = ', '.join([f":{col}" for col in df.columns])
            insert_sql = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({values})
            """
            
            # 將 DataFrame 轉換為字典列表，並確保所有值都是 Python 原生類型
            data_dicts = []
            for _, row in df.iterrows():
                data_dict = {}
                for col in df.columns:
                    value = row[col]
                    if isinstance(value, np.integer):
                        value = int(value)
                    elif isinstance(value, np.floating):
                        value = float(value)
                    elif isinstance(value, np.bool_):
                        value = bool(value)
                    data_dict[col] = value
                data_dicts.append(data_dict)
            
            # 執行批量 INSERT
            with self.engine.begin() as conn:
                result = conn.execute(text(insert_sql), data_dicts)
                self.logger.info(f"插入結果：{result.rowcount} 筆資料已插入")
            
            # 驗證資料是否成功寫入
            verification_query = f"""
            SELECT * FROM {table_name} 
            WHERE departure_flight_number_1 = :flight_num
            AND return_flight_number_1 = :return_num
            AND creation_time = :creation_time
            """
            
            with self.engine.begin() as conn:
                result = conn.execute(
                    text(verification_query),
                    {
                        'flight_num': str(df['departure_flight_number_1'].iloc[0]),
                        'return_num': str(df['return_flight_number_1'].iloc[0]),
                        'creation_time': float(df['creation_time'].iloc[0])
                    }
                ).fetchall()
                
            if len(result) > 0:
                self.logger.info(f"成功寫入 {len(result)} 筆資料")
            else:
                self.logger.warning("警告：資料可能未成功寫入")
            
            self.logger.info(f"成功將資料寫入到表格 {table_name}")
        except Exception as e:
            self.logger.error(f"寫入資料時發生錯誤: {str(e)}")
            self.logger.error("詳細錯誤訊息：")
            self.logger.error(traceback.format_exc())
            raise RuntimeError("寫入資料到 Cloud SQL 失敗") from e

    def _create_connection(self):
        """
        建立與 Cloud SQL 的連線。
        根據環境變數決定使用本地連線或 Cloud SQL 連線。

        返回：
        Connection: 資料庫連線物件。
        """
        try:
            # 從環境變數獲取資料庫配置
            db_user = os.getenv('DB_USER', 'flypa')
            db_pass = os.getenv('DB_PASS', 'Flypa25151868')
            db_name = os.getenv('DB_NAME', 'flypa')
            instance_connection_name = os.getenv('INSTANCE_CONNECTION_NAME', 'testing-cola-rd:asia-east1:testing-cola-rd-postgres')
            IS_CLOUD = os.getenv('IS_CLOUD', 'false').lower() == 'true'

            self.logger.info("嘗試連接到資料庫...")

            # 判斷是否在 Cloud Run 環境
            if IS_CLOUD:
                self.logger.info("使用 Cloud SQL 連線配置")
                # 使用 Cloud SQL Connector
                connector = Connector()
                
                def getconn():
                    conn = connector.connect(
                        instance_connection_name,
                        "pg8000",
                        user=db_user,
                        password=db_pass,
                        db=db_name,
                        ip_type=IPTypes.PRIVATE
                    )
                    return conn

                # 創建引擎
                self.engine = create_engine(
                    "postgresql+pg8000://",
                    creator=getconn,
                    connect_args={"options": "-c search_path=domanda"}
                )
            else:
                self.logger.info("使用本地資料庫連線配置")
                # 本地開發環境連線字串
                connection_string = f'postgresql://{db_user}:{db_pass}@localhost:5432/{db_name}?options=-c search_path=domanda'
                self.engine = create_engine(connection_string)

            self.connection = self.engine.connect()
            self.logger.info("成功建立資料庫連接")
            
            # 測試連接
            test_query = "SELECT 1"
            result = pd.read_sql_query(test_query, self.engine)
            self.logger.info("資料庫連接測試成功")

        except Exception as e:
            self.logger.error(f"建立資料庫連接時發生錯誤: {str(e)}")
            self.logger.error("詳細錯誤訊息：")
            self.logger.error(traceback.format_exc())
            raise RuntimeError("無法建立資料庫連接") from e

    def backup_table(self):
        try:
            current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_table_name = f"backup_flight_ticket_price_compare_{current_time}"
            
            self.logger.info(f"開始建立備份表：{backup_table_name}")
            
            # 新增 timeout 和鎖檢查
            create_backup_sql = f"""
            CREATE TABLE {backup_table_name} AS 
            SELECT * FROM domanda.flight_ticket_price_compare
            WHERE (SELECT pg_try_advisory_lock(hashtext('backup_lock')))
            """
            
            # 設定 statement_timeout
            with self.engine.begin() as conn:
                conn.execute(text("SET statement_timeout = '300000'"))  # 設定 5 分鐘超時
                conn.execute(text(create_backup_sql))
                # 釋放鎖
                conn.execute(text("SELECT pg_advisory_unlock_all()"))

            self.logger.info(f"成功建立備份表：{backup_table_name}")
            
            self._cleanup_old_backups()
            return backup_table_name
                
        except Exception as e:
            self.logger.error(f"建立備份表時發生錯誤: {str(e)}")
            self.logger.error("詳細錯誤訊息：")
            self.logger.error(traceback.format_exc())
            # 確保釋放鎖
            try:
                with self.engine.begin() as conn:
                    conn.execute(text("SELECT pg_advisory_unlock_all()"))
            except:
                pass
            raise RuntimeError("建立備份表失敗") from e
    
    def truncate_and_load(self, df):
        """
        執行全刪全寫操作。
        
        步驟：
        1. 建立資料表備份
        2. 清空原表
        3. 寫入新資料
        4. 如果過程中發生錯誤，自動回滾到備份
        
        參數：
        df (DataFrame): 需要寫入的新資料
        
        異常：
        - ValueError: 當 DataFrame 為空時
        - RuntimeError: 當資料庫操作失敗時
        """
        if df is None or df.empty:
            raise ValueError("DataFrame 不能為空")

        try:
            # 1. 建立備份
            backup_table = self.backup_table()
            self.logger.info(f"已建立備份表：{backup_table}")

            # 2. 清空原表
            with self.engine.begin() as conn:
                self.logger.info("開始清空原表...")
                conn.execute(text("TRUNCATE TABLE domanda.flight_ticket_price_compare"))
                
            # 3. 寫入新資料
            self.logger.info(f"開始寫入 {len(df)} 筆新資料...")
            df = df.replace({np.nan: None})
                
            self.load_to_cloud_sql(df)
            self.logger.info("全刪全寫操作成功")

        except Exception as e:
            self.logger.error(f"全刪全寫操作失敗: {str(e)}")
            self.logger.error("開始執行回滾操作...")
            self.restore_from_backup()
            raise RuntimeError("全刪全寫操作失敗，已回滾到備份狀態") from e
    
    def restore_from_backup(self):
        """
        從最近的備份還原資料。
        
        步驟：
        1. 找出最新的備份表
        2. 清空原表
        3. 從備份表複製資料到原表
        
        異常：
        - FileNotFoundError: 當找不到可用的備份表時
        - RuntimeError: 當還原操作失敗時
        """
        try:
            # 找出最新的備份表
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name LIKE 'backup_flight_ticket_price_compare_%'
            ORDER BY table_name DESC
            LIMIT 1
            """
            
            with self.engine.begin() as conn:
                result = conn.execute(text(query)).fetchone()
                
                if not result:
                    error_msg = "找不到可用的備份表"
                    self.logger.error(error_msg)
                    raise FileNotFoundError(error_msg)
                
                latest_backup = result[0]
                self.logger.info(f"找到最新的備份表：{latest_backup}")
                
                # 在事務中執行還原操作
                self.logger.info("開始執行還原操作...")
                
                # 清空原表
                self.logger.info("清空原表...")
                conn.execute(text("TRUNCATE TABLE domanda.flight_ticket_price_compare"))
                
                # 從備份表複製資料
                self.logger.info(f"從備份表 {latest_backup} 複製資料...")
                restore_sql = f"""
                INSERT INTO domanda.flight_ticket_price_compare
                SELECT * FROM {latest_backup}
                """
                conn.execute(text(restore_sql))
                
                # 驗證還原結果
                original_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {latest_backup}")
                ).scalar()
                restored_count = conn.execute(
                    text("SELECT COUNT(*) FROM domanda.flight_ticket_price_compare")
                ).scalar()
                
                if original_count != restored_count:
                    raise RuntimeError(f"還原後的資料量不一致：備份表 {original_count} 筆，還原後 {restored_count} 筆")
                
                self.logger.info(f"成功還原 {restored_count} 筆資料")
                
        except FileNotFoundError as e:
            raise RuntimeError("找不到可用的備份表") from e
        except Exception as e:
            self.logger.error(f"還原資料時發生錯誤: {str(e)}")
            self.logger.error("詳細錯誤訊息：")
            self.logger.error(traceback.format_exc())
            raise RuntimeError("還原資料失敗") from e
    
    def _cleanup_old_backups(self):
        """
        清理舊的備份表，只保留最新的 3 個備份。
        
        步驟：
        1. 獲取所有備份表列表
        2. 按照建立時間排序
        3. 刪除超出保留數量的舊備份
        
        異常：
        - RuntimeError: 當清理操作失敗時
        """
        try:
            # 設定要保留的備份數量
            MAX_BACKUPS = 3
            
            # 獲取所有備份表
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name LIKE 'backup_flight_ticket_price_compare_%'
            ORDER BY table_name DESC
            """
            
            with self.engine.begin() as conn:
                backup_tables = [row[0] for row in conn.execute(text(query)).fetchall()]
                
                # 如果備份表數量超過限制，刪除最舊的備份
                if len(backup_tables) > MAX_BACKUPS:
                    tables_to_delete = backup_tables[MAX_BACKUPS:]
                    for table_name in tables_to_delete:
                        self.logger.info(f"刪除舊的備份表：{table_name}")
                        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                        
        except Exception as e:
            self.logger.error(f"清理舊備份表時發生錯誤: {str(e)}")
            self.logger.error("詳細錯誤訊息：")
            self.logger.error(traceback.format_exc())
            raise RuntimeError("清理舊備份表失敗") from e

if __name__ == "__main__":
    import pickle
    with open("data.pkl", "rb") as file:
        df = pickle.load(file)

    loader = Loader()
    loader.truncate_and_load(df)
