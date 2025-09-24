from etl.extractor import Extractor
from etl.transform.cola_transformer import ColaTransformer
from etl.transform.set_transformer import SetTransformer
from etl.transform.lion_transformer import LionTransformer
from etl.transform.eztravel_transformer import EztravelTransformer
from etl.transform.foreign_supplier_eztravel_transformer import ForeignSupplierEztravelTransformer
from etl.transform.rich_transformer import RichTransformer
from etl.transform.unified_transformer import UnifiedTransformer
from etl.loader import Loader

class Pipeline:
    def __init__(self, project_id: str):
        """
        初始化 Pipeline 物件。

        參數：
        project_id (str): 專案 ID。
        """
        self.extractor = Extractor(project_id=project_id)
        self.cola_transformer = ColaTransformer()
        self.set_transformer = SetTransformer()
        self.lion_transformer = LionTransformer()
        self.eztravel_transformer = EztravelTransformer()
        self.foreign_supplier_eztravel_transformer = ForeignSupplierEztravelTransformer()
        self.rich_transformer = RichTransformer()
        self.unified_transformer = UnifiedTransformer()
        self.loader = Loader()

    def run(self):
        """
        執行整個 ETL 流程。
        流程步驟：
        1. 從 BigQuery 提取資料
        2. 清洗資料
        3. 整合資料
        4. 寫入 Cloud SQL
        """
        cola_df = self.extractor.extract_cola_data()
        set_df = self.extractor.extract_set_data()
        lion_df = self.extractor.extract_lion_data()
        eztravel_df = self.extractor.extract_eztravel_data()
        foreign_supplier_eztravel_df = self.extractor.extract_foreign_supplier_eztravel_data()
        rich_df = self.extractor.extract_rich_data()
        cola_cleaned_df = self.cola_transformer.clean_data(df=cola_df)
        set_cleaned_df = self.set_transformer.clean_data(df=set_df)
        lion_cleaned_df = self.lion_transformer.clean_data(df=lion_df)
        eztravel_cleaned_df = self.eztravel_transformer.clean_data(df=eztravel_df)
        foreign_supplier_eztravel_cleaned_df = self.foreign_supplier_eztravel_transformer.clean_data(df=foreign_supplier_eztravel_df)
        rich_cleaned_df = self.rich_transformer.clean_data(df=rich_df)
        unified_df = self.unified_transformer.unify_data(cola_df=cola_cleaned_df,
                                                         set_df=set_cleaned_df,
                                                         lion_df=lion_cleaned_df,
                                                         eztravel_df=eztravel_cleaned_df,
                                                         foreign_supplier_eztravel_df=foreign_supplier_eztravel_cleaned_df,
                                                         rich_df=rich_cleaned_df)
        unified_df = unified_df.sort_values('creation_time', ascending=False).drop_duplicates(subset=[col for col in unified_df.columns if col != 'creation_time'], keep='first')
        self.loader.truncate_and_load(unified_df)
