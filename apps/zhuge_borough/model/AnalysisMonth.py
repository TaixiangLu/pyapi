from model.BaseModel.Base import Base


class AnalysisMonth(Base):
    __tablename__ = 'new_month_netease'
    fields = {
        'id': 0,
        'datetime': 0,
        'volume_total': 0,
        'volume_totalarea': 0,
        'volume_totalprice': 0,
        'volume_price': 0,
        'sell_rate': 0,
        'created': 0,
        'updated': 0,
    }
