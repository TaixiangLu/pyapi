from dao.BaseDao.BaseMysql import BaseMysql
from apps.zhuge_borough.model.AnalysisDay import AnalysisDay


class AnalysisDayDao(BaseMysql):
    def __init__(self, *args, **kwargs):
        self.model = AnalysisDay()
        super().__init__(conf_name=kwargs.get("conf_name", "analysis"), model=kwargs.get("model", self.model))
