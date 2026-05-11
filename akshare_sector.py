"""
=========================================================
板块资金流向API模块 - AkShare 数据源
=========================================================
"""

import akshare as ak
import time
import pandas as pd

class AkShareSectorAPI:
    """使用 AkShare 获取板块资金流数据"""

    @staticmethod
    def get_sector_flow(indicator="10日", sector_type="概念资金流", max_retries=3):
        """获取板块资金流数据"""
        period_map = {'1': '今日', '5': '5日', '10': '10日'}
        api_indicator = period_map.get(str(indicator), '10日')

        for attempt in range(max_retries):
            try:
                df = ak.stock_sector_fund_flow_rank(
                    indicator=api_indicator,
                    sector_type=sector_type
                )
                return df
            except Exception as e:
                print(f"获取失败 (尝试 {attempt+1}/{max_retries}): {str(e)[:100]}")
                if attempt < max_retries - 1:
                    time.sleep(3)
        return None

    @staticmethod
    def get_combined_flow(indicator="10日", max_retries=3):
        """获取申万二级行业资金流"""
        if indicator not in ['1', '5', '10', '20', '60']:
            indicator = '10日'

        if indicator not in ['1', '5', '10']:
            df_industry = AkShareSectorAPI.get_sector_flow('5', "行业资金流", max_retries)
        else:
            df_industry = AkShareSectorAPI.get_sector_flow(indicator, "行业资金流", max_retries)

        result = {}

        if df_industry is not None and not df_industry.empty:
            for _, row in df_industry.iterrows():
                cols = df_industry.columns.tolist()
                name = row[cols[1]] if len(cols) > 1 else ''
                if not name:
                    continue

                change = row[cols[2]] if len(cols) > 2 else 0
                flow_col = cols[3] if len(cols) > 3 else None
                flow = row[flow_col] if flow_col else 0

                result[name] = {
                    'name': name,
                    'main_flow': float(flow) if pd.notna(flow) else 0,
                    'change_pct': float(change) if pd.notna(change) else 0,
                    'sector_type': '行业',
                    'rank': row[cols[0]] if len(cols) > 0 else 0
                }

        return result

    @staticmethod
    def get_top_flow_sectors(indicator="10日", limit=10):
        """获取资金流向TOP板块"""
        combined = AkShareSectorAPI.get_combined_flow(indicator, max_retries=3)

        seen_bases = set()
        result_list = []
        suffixes = ['Ⅱ', 'Ⅲ', 'Ⅳ', 'Ⅰ']

        for name, data in combined.items():
            flow = data.get('main_flow', 0)
            if flow == 0:
                continue

            base_name = name
            while base_name and base_name[-1] in suffixes:
                base_name = base_name[:-1]

            if base_name in seen_bases:
                continue
            seen_bases.add(base_name)
            result_list.append({**data, 'name': name, 'base_name': base_name})

        inflow = []
        outflow = []

        for item in result_list:
            flow = item.get('main_flow', 0)
            display_name = item.get('base_name', item['name'])
            if flow > 0:
                inflow.append({'name': display_name, 'main_flow': flow, 'change_pct': item.get('change_pct', 0), 'sector_type': item.get('sector_type', '行业')})
            elif flow < 0:
                outflow.append({'name': display_name, 'main_flow': flow, 'change_pct': item.get('change_pct', 0), 'sector_type': item.get('sector_type', '行业'), 'flow_value': flow})

        inflow.sort(key=lambda x: x.get('main_flow', 0), reverse=True)
        outflow.sort(key=lambda x: x.get('main_flow', 0))

        return {
            'inflow_top10': inflow[:limit],
            'outflow_top10': outflow[:limit]
        }


if __name__ == "__main__":
    print("=== 测试 AkShare 数据获取 ===")
    result = AkShareSectorAPI.get_top_flow_sectors("5日")

    if result:
        print("\n净流入TOP10:")
        for i, item in enumerate(result['inflow_top10'][:10], 1):
            print(f"  {i}. {item['name']}: {item['main_flow']/10000:.0f}万 涨跌:{item['change_pct']}%")

        print("\n净流出TOP10:")
        for i, item in enumerate(result['outflow_top10'][:10], 1):
            print(f"  {i}. {item['name']}: {item['main_flow']/10000:.0f}万 涨跌:{item['change_pct']}%")
    else:
        print("获取失败")
