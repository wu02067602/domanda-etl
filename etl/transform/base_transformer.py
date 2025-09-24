from abc import ABC, abstractmethod

class BaseTransformer(ABC):
    """
    BaseTransformer 類別作為所有 Transformer 的基礎類別，提供通用的數據清理方法。
    """

    @abstractmethod
    def clean_data(self, df):
        """
        抽象方法，必須在子類中實現。

        參數：
            df (DataFrame): 需要清理的 DataFrame。

        返回：
            DataFrame: 清理後的 DataFrame。
        """
        pass
