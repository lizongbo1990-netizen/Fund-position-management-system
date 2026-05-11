"""
=========================================================
聚宽数据源模块
=========================================================
注意: 需要聚宽API Key (个人开发者免费申请)
申请地址: https://joinquant.com/data
"""

class JoinQuantAPI:
    """聚宽数据接口"""
    
    def __init__(self, token=None):
        """
        初始化聚宽API
        
        参数:
            token: 聚宽API Token, 需要从 https://joinquant.com/data 申请
                  免费个人开发者: 5000次/日, 10万次/月
        """
        self.token = token
        self.available = False
        
        if token:
            try:
                import jqdatasdk
                jqdatasdk.auth(token.split(',')[0] if ',' in token else token, '')
                self.available = True
                print("聚宽API认证成功")
            except ImportError:
                print("请安装聚宽SDK: pip install jqdatasdk")
            except Exception as e:
                print(f"聚宽API认证失败: {e}")
    
    def is_available(self):
        """检查是否可用"""
        return self.available
    
    def get_sector_flow(self, date=None):
        """
        获取申万行业资金流
        
        参数:
            date: 日期 (YYYY-MM-DD), 默认为上一个交易日
        
        返回:
            DataFrame: 行业资金流数据
        """
        if not self.available:
            return None
        
        try:
            import jqdatasdk as jq
            from datetime import datetime, timedelta
            
            if not date:
                # 获取上一个交易日
                date = jq.get_trade_days(end_date=datetime.now(), count=1)[0]
            
            # 聚宽的申万行业成分数据
            # 使用 get_industry 方法获取行业分类
            # 注意: 聚宽不直接提供资金流数据，需要结合其他接口
            
            return None
            
        except Exception as e:
            print(f"获取聚宽数据失败: {e}")
            return None


# 聚宽行业分类代码映射
JQ_INDUSTRY_CODES = {
    '801010': '农林牧渔',
    '801020': '采掘',
    '801030': '化工',
    '801040': '钢铁',
    '801050': '有色金属',
    '801060': '电子',
    '801080': '通信',
    '801110': '计算机',
    '801120': '轻工制造',
    '801130': '纺织服装',
    '801150': '家用电器',
    '801210': '汽车',
    '801220': '电力设备',
    '801230': '国防军工',
    '801310': '建筑装饰',
    '801320': '建筑材料',
    '801330': '房地产',
    '801410': '商业贸易',
    '801420': '交通运输',
    '801430': '金融',
    '801450': '休闲服务',
    '801510': '综合',
    '801610': '食品饮料',
    '801620': '医药生物',
    '801630': '公用事业',
    '801710': '传媒',
    '801720': '银行',
    '801730': '非银金融',
}


if __name__ == "__main__":
    # 测试 (需要配置token)
    print("=== 聚宽API测试 ===")
    
    # 方式1: 不带token (检查是否安装)
    jq = JoinQuantAPI()
    print(f"可用: {jq.is_available()}")
    
    # 方式2: 带token (需要您申请)
    # jq = JoinQuantAPI(token="17737506140,Vip19900919Lzb")