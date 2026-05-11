"""
=========================================================
ETF数据获取模块
=========================================================
"""

import akshare as ak
import pandas as pd
import time

class ETFDataFetcher:
    """ETF数据获取器"""
    
    @staticmethod
    def get_etf_spot_data(max_retries=3):
        """获取ETF实时数据（含资金流）"""
        for attempt in range(max_retries):
            try:
                df = ak.fund_etf_spot_em()
                print(f"获取ETF实时数据成功: {len(df)} 条")
                return df
            except Exception as e:
                print(f"获取ETF数据失败 (尝试 {attempt+1}/{max_retries}): {str(e)[:50]}")
                if attempt < max_retries - 1:
                    time.sleep(2)
        return None
    
    @staticmethod
    def get_etf_flow_by_category(period='10日'):
        """
        按行业/主题分类获取ETF资金流
        
        返回:
            dict: {行业名称: {主力净流入, ETF列表}}
        """
        df = ETFDataFetcher.get_etf_spot_data()
        
        if df is None:
            return {}
        
        # 主力净流入 = 超大单 + 大单
        if '超大单净流入-净额' in df.columns and '大单净流入-净额' in df.columns:
            df['主力净流入'] = df['超大单净流入-净额'].fillna(0) + df['大单净流入-净额'].fillna(0)
        else:
            # 使用其他可用字段
            for col in ['主力净流入', '净流入', '今日主力净流入']:
                if col in df.columns:
                    df['主力净流入'] = df[col].fillna(0)
                    break
            else:
                df['主力净流入'] = 0
        
        # ETF行业/主题分类
        # 通过ETF名称关键词匹配行业
        category_keywords = {
            '半导体': ['半导体', '芯片', '集成电路', '光刻'],
            '新能源': ['新能源', '光伏', '锂电', '锂电池', '储能', '氢能'],
            '医药': ['医药', '医疗', '生物', '中药', '疫苗', '创新药'],
            '金融': ['金融', '银行', '证券', '保险', '非银'],
            '消费': ['消费', '食品', '饮料', '白酒', '家电', '纺织'],
            '科技': ['科技', '计算机', '软件', '互联网', 'AI', '人工智能'],
            '军工': ['军工', '国防', '航天', '航空', '船舶'],
            '新能源车': ['新能源车', '新能源汽车', '电动车', '智能汽车'],
            '5G': ['5G', '通信', '物联网', '数字化'],
            '创业板': ['创业板', '创成长', '创50'],
            '沪深300': ['沪深300', '300'],
            '中证500': ['中证500', '500'],
            '中证1000': ['中证1000', '1000'],
            '上证50': ['上证50', '50'],
            '黄金': ['黄金', '贵金属'],
            '港股': ['港股', '恒生', 'H股', 'QDII'],
            '创业板': ['创业板', '科创'],
            '科创': ['科创', '科技创新'],
        }
        
        # 分类统计
        category_flow = {}
        
        for category, keywords in category_keywords.items():
            # 筛选包含关键词的ETF
            mask = df['基金代码'].astype(str).str.contains('|'.join(keywords), case=False, na=False) | \
                   df['基金简称'].str.contains('|'.join(keywords), case=False, na=False) if '基金简称' in df.columns else pd.Series([False]*len(df))
            
            category_etfs = df[mask]
            
            if len(category_etfs) > 0:
                total_flow = category_etfs['主力净流入'].sum()
                category_flow[category] = {
                    'main_flow': total_flow,
                    'etf_count': len(category_etfs),
                    'etfs': category_etfs[['基金代码', '基金简称', '主力净流入']].head(5).to_dict('records') if '基金简称' in category_etfs.columns else []
                }
        
        return category_flow
    
    @staticmethod
    def get_top_etf_flow(limit=10):
        """获取ETF资金流TOP"""
        df = ETFDataFetcher.get_etf_spot_data()
        
        if df is None:
            return []
        
        # 计算主力净流入
        if '超大单净流入-净额' in df.columns and '大单净流入-净额' in df.columns:
            df['主力净流入'] = df['超大单净流入-净额'].fillna(0) + df['大单净流入-净额'].fillna(0)
        else:
            # 尝试其他字段
            for col in ['主力净流入', '净流入', '今日主力净流入']:
                if col in df.columns:
                    df['主力净流入'] = df[col].fillna(0)
                    break
            else:
                df['主力净流入'] = 0
        
        # 排序
        df_sorted = df.sort_values('主力净流入', ascending=False)
        
        # 返回TOP
        result = []
        for _, row in df_sorted.head(limit).iterrows():
            result.append({
                'name': row.get('基金简称', row.get('代码', '')),
                'code': row.get('基金代码', ''),
                'main_flow': row['主力净流入'],
                'change_pct': row.get('涨跌幅', 0)
            })
        
        return result


if __name__ == "__main__":
    print("=== ETF数据获取测试 ===")
    
    # 获取TOP ETF
    top_etfs = ETFDataFetcher.get_top_etf_flow(10)
    print(f"\nETF资金流TOP10:")
    for i, etf in enumerate(top_etfs, 1):
        print(f"  {i}. {etf['name']}: {etf['main_flow']/10000:.0f}万")
    
    # 按行业分类
    print("\n\n按行业分类的ETF资金流:")
    category_flow = ETFDataFetcher.get_etf_flow_by_category()
    for cat, data in sorted(category_flow.items(), key=lambda x: x[1]['main_flow'], reverse=True)[:10]:
        print(f"  {cat}: {data['main_flow']/10000:.0f}万 ({data['etf_count']}只ETF)")