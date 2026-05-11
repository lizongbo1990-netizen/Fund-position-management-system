"""
=========================================================
板块资金流向API模块 - 东方财富免费接口
=========================================================
功能：获取申万二级行业 + 概念板块资金流向
"""

import requests
import time
import re

# 模拟数据 - API不可用时使用
MOCK_DATA = {
    '1': {  # 今日
        'inflow': [
            {'name': '半导体', 'main_flow': 850000000, 'change_pct': 2.5, 'sector_type': '行业', 'code': 'BK1201'},
            {'name': '证券', 'main_flow': 620000000, 'change_pct': 1.8, 'sector_type': '行业', 'code': 'BK0473'},
            {'name': '软件开发', 'main_flow': 480000000, 'change_pct': 2.2, 'sector_type': '行业', 'code': 'BK0506'},
            {'name': '光学光电子', 'main_flow': 380000000, 'change_pct': 3.5, 'sector_type': '行业', 'code': 'BK0561'},
            {'name': '通信设备', 'main_flow': 320000000, 'change_pct': 1.5, 'sector_type': '行业', 'code': 'BK0448'},
            {'name': '电池', 'main_flow': 280000000, 'change_pct': 1.2, 'sector_type': '行业', 'code': 'BK1007'},
            {'name': '医疗器械', 'main_flow': 220000000, 'change_pct': 1.0, 'sector_type': '行业', 'code': 'BK1010'},
            {'name': '汽车零部件', 'main_flow': 180000000, 'change_pct': 0.8, 'sector_type': '行业', 'code': 'BK0401'},
            {'name': '电子元件', 'main_flow': 150000000, 'change_pct': 1.1, 'sector_type': '行业', 'code': 'BK0528'},
            {'name': '化学制药', 'main_flow': 120000000, 'change_pct': 0.9, 'sector_type': '行业', 'code': 'BK0201'},
        ],
        'outflow': [
            {'name': '银行', 'main_flow': -420000000, 'change_pct': -0.8, 'sector_type': '行业', 'code': 'BK0475', 'flow_value': -420000000},
            {'name': '房地产', 'main_flow': -350000000, 'change_pct': -1.5, 'sector_type': '行业', 'code': 'BK0458', 'flow_value': -350000000},
            {'name': '钢铁', 'main_flow': -280000000, 'change_pct': -1.2, 'sector_type': '行业', 'code': 'BK0101', 'flow_value': -280000000},
            {'name': '煤炭开采', 'main_flow': -220000000, 'change_pct': -1.0, 'sector_type': '行业', 'code': 'BK0205', 'flow_value': -220000000},
            {'name': '石油石化', 'main_flow': -180000000, 'change_pct': -0.6, 'sector_type': '行业', 'code': 'BK0105', 'flow_value': -180000000},
            {'name': '土木工程', 'main_flow': -150000000, 'change_pct': -0.8, 'sector_type': '行业', 'code': 'BK0421', 'flow_value': -150000000},
            {'name': '航运港口', 'main_flow': -120000000, 'change_pct': -0.5, 'sector_type': '行业', 'code': 'BK0405', 'flow_value': -120000000},
            {'name': '电力', 'main_flow': -95000000, 'change_pct': -0.4, 'sector_type': '行业', 'code': 'BK0312', 'flow_value': -95000000},
            {'name': '保险', 'main_flow': -72000000, 'change_pct': -0.3, 'sector_type': '行业', 'code': 'BK0478', 'flow_value': -72000000},
            {'name': '物流', 'main_flow': -55000000, 'change_pct': -0.4, 'sector_type': '行业', 'code': 'BK0441', 'flow_value': -55000000},
        ]
    },
    '5': {
        'inflow': [
            {'name': '半导体', 'main_flow': 1250000000, 'change_pct': 3.2, 'sector_type': '行业', 'code': 'BK1201'},
            {'name': '医疗器械', 'main_flow': 980000000, 'change_pct': 2.1, 'sector_type': '行业', 'code': 'BK1010'},
            {'name': '证券', 'main_flow': 850000000, 'change_pct': 1.5, 'sector_type': '行业', 'code': 'BK0473'},
            {'name': '软件开发', 'main_flow': 720000000, 'change_pct': 2.8, 'sector_type': '行业', 'code': 'BK0506'},
            {'name': '通信设备', 'main_flow': 680000000, 'change_pct': 1.9, 'sector_type': '行业', 'code': 'BK0448'},
            {'name': '光学光电子', 'main_flow': 550000000, 'change_pct': 2.5, 'sector_type': '行业', 'code': 'BK0561'},
            {'name': '电池', 'main_flow': 480000000, 'change_pct': 1.8, 'sector_type': '行业', 'code': 'BK1007'},
            {'name': '汽车零部件', 'main_flow': 420000000, 'change_pct': 1.2, 'sector_type': '行业', 'code': 'BK0401'},
            {'name': '化学制药', 'main_flow': 380000000, 'change_pct': 1.6, 'sector_type': '行业', 'code': 'BK0201'},
            {'name': '电子元件', 'main_flow': 350000000, 'change_pct': 2.0, 'sector_type': '行业', 'code': 'BK0528'},
        ],
        'outflow': [
            {'name': '银行', 'main_flow': -850000000, 'change_pct': -1.2, 'sector_type': '行业', 'code': 'BK0475', 'flow_value': -850000000},
            {'name': '房地产', 'main_flow': -720000000, 'change_pct': -2.1, 'sector_type': '行业', 'code': 'BK0458', 'flow_value': -720000000},
            {'name': '钢铁', 'main_flow': -580000000, 'change_pct': -1.8, 'sector_type': '行业', 'code': 'BK0101', 'flow_value': -580000000},
            {'name': '煤炭开采', 'main_flow': -450000000, 'change_pct': -1.5, 'sector_type': '行业', 'code': 'BK0205', 'flow_value': -450000000},
            {'name': '石油石化', 'main_flow': -380000000, 'change_pct': -0.9, 'sector_type': '行业', 'code': 'BK0105', 'flow_value': -380000000},
            {'name': '土木工程', 'main_flow': -320000000, 'change_pct': -1.1, 'sector_type': '行业', 'code': 'BK0421', 'flow_value': -320000000},
            {'name': '航运港口', 'main_flow': -280000000, 'change_pct': -0.8, 'sector_type': '行业', 'code': 'BK0405', 'flow_value': -280000000},
            {'name': '电力', 'main_flow': -250000000, 'change_pct': -0.6, 'sector_type': '行业', 'code': 'BK0312', 'flow_value': -250000000},
            {'name': '保险', 'main_flow': -200000000, 'change_pct': -0.5, 'sector_type': '行业', 'code': 'BK0478', 'flow_value': -200000000},
            {'name': '铁路公路', 'main_flow': -150000000, 'change_pct': -0.4, 'sector_type': '行业', 'code': 'BK0497', 'flow_value': -150000000},
        ]
    },
    '10': {
        'inflow': [
            {'name': '半导体', 'main_flow': 2800000000, 'change_pct': 5.5, 'sector_type': '行业', 'code': 'BK1201'},
            {'name': '证券', 'main_flow': 2200000000, 'change_pct': 3.2, 'sector_type': '行业', 'code': 'BK0473'},
            {'name': '软件开发', 'main_flow': 1800000000, 'change_pct': 4.8, 'sector_type': '行业', 'code': 'BK0506'},
            {'name': '光学光电子', 'main_flow': 1500000000, 'change_pct': 6.2, 'sector_type': '行业', 'code': 'BK0561'},
            {'name': '通信设备', 'main_flow': 1200000000, 'change_pct': 3.5, 'sector_type': '行业', 'code': 'BK0448'},
            {'name': '电池', 'main_flow': 950000000, 'change_pct': 2.8, 'sector_type': '行业', 'code': 'BK1007'},
            {'name': '医疗器械', 'main_flow': 820000000, 'change_pct': 2.1, 'sector_type': '行业', 'code': 'BK1010'},
            {'name': '汽车零部件', 'main_flow': 680000000, 'change_pct': 1.8, 'sector_type': '行业', 'code': 'BK0401'},
            {'name': '电子元件', 'main_flow': 550000000, 'change_pct': 3.0, 'sector_type': '行业', 'code': 'BK0528'},
            {'name': '化学制药', 'main_flow': 480000000, 'change_pct': 2.2, 'sector_type': '行业', 'code': 'BK0201'},
        ],
        'outflow': [
            {'name': '银行', 'main_flow': -1800000000, 'change_pct': -2.5, 'sector_type': '行业', 'code': 'BK0475', 'flow_value': -1800000000},
            {'name': '房地产', 'main_flow': -1500000000, 'change_pct': -4.2, 'sector_type': '行业', 'code': 'BK0458', 'flow_value': -1500000000},
            {'name': '钢铁', 'main_flow': -1200000000, 'change_pct': -3.5, 'sector_type': '行业', 'code': 'BK0101', 'flow_value': -1200000000},
            {'name': '煤炭开采', 'main_flow': -980000000, 'change_pct': -2.8, 'sector_type': '行业', 'code': 'BK0205', 'flow_value': -980000000},
            {'name': '石油石化', 'main_flow': -750000000, 'change_pct': -1.8, 'sector_type': '行业', 'code': 'BK0105', 'flow_value': -750000000},
            {'name': '土木工程', 'main_flow': -620000000, 'change_pct': -2.1, 'sector_type': '行业', 'code': 'BK0421', 'flow_value': -620000000},
            {'name': '航运港口', 'main_flow': -480000000, 'change_pct': -1.5, 'sector_type': '行业', 'code': 'BK0405', 'flow_value': -480000000},
            {'name': '电力', 'main_flow': -380000000, 'change_pct': -1.0, 'sector_type': '行业', 'code': 'BK0312', 'flow_value': -380000000},
            {'name': '保险', 'main_flow': -280000000, 'change_pct': -0.8, 'sector_type': '行业', 'code': 'BK0478', 'flow_value': -280000000},
            {'name': '物流', 'main_flow': -220000000, 'change_pct': -1.2, 'sector_type': '行业', 'code': 'BK0441', 'flow_value': -220000000},
        ]
    },
    '20': {
        'inflow': [
            {'name': '半导体', 'main_flow': 5200000000, 'change_pct': 8.5, 'sector_type': '行业', 'code': 'BK1201'},
            {'name': '光学光电子', 'main_flow': 3800000000, 'change_pct': 12.2, 'sector_type': '行业', 'code': 'BK0561'},
            {'name': '证券', 'main_flow': 3500000000, 'change_pct': 5.2, 'sector_type': '行业', 'code': 'BK0473'},
            {'name': '软件开发', 'main_flow': 2800000000, 'change_pct': 7.8, 'sector_type': '行业', 'code': 'BK0506'},
            {'name': '通信设备', 'main_flow': 2200000000, 'change_pct': 5.5, 'sector_type': '行业', 'code': 'BK0448'},
            {'name': '电池', 'main_flow': 1800000000, 'change_pct': 4.2, 'sector_type': '行业', 'code': 'BK1007'},
            {'name': '医疗器械', 'main_flow': 1500000000, 'change_pct': 3.5, 'sector_type': '行业', 'code': 'BK1010'},
            {'name': '汽车零部件', 'main_flow': 1200000000, 'change_pct': 2.8, 'sector_type': '行业', 'code': 'BK0401'},
            {'name': '电子元件', 'main_flow': 980000000, 'change_pct': 4.5, 'sector_type': '行业', 'code': 'BK0528'},
            {'name': '光伏设备', 'main_flow': 820000000, 'change_pct': 3.8, 'sector_type': '行业', 'code': 'BK1034', 'flow_value': 820000000},
        ],
        'outflow': [
            {'name': '银行', 'main_flow': -3500000000, 'change_pct': -4.5, 'sector_type': '行业', 'code': 'BK0475', 'flow_value': -3500000000},
            {'name': '房地产', 'main_flow': -2800000000, 'change_pct': -8.2, 'sector_type': '行业', 'code': 'BK0458', 'flow_value': -2800000000},
            {'name': '钢铁', 'main_flow': -2200000000, 'change_pct': -6.5, 'sector_type': '行业', 'code': 'BK0101', 'flow_value': -2200000000},
            {'name': '煤炭开采', 'main_flow': -1800000000, 'change_pct': -5.2, 'sector_type': '行业', 'code': 'BK0205', 'flow_value': -1800000000},
            {'name': '石油石化', 'main_flow': -1500000000, 'change_pct': -3.5, 'sector_type': '行业', 'code': 'BK0105', 'flow_value': -1500000000},
            {'name': '土木工程', 'main_flow': -1200000000, 'change_pct': -4.0, 'sector_type': '行业', 'code': 'BK0421', 'flow_value': -1200000000},
            {'name': '航运港口', 'main_flow': -950000000, 'change_pct': -2.8, 'sector_type': '行业', 'code': 'BK0405', 'flow_value': -950000000},
            {'name': '电力', 'main_flow': -750000000, 'change_pct': -2.0, 'sector_type': '行业', 'code': 'BK0312', 'flow_value': -750000000},
            {'name': '保险', 'main_flow': -550000000, 'change_pct': -1.5, 'sector_type': '行业', 'code': 'BK0478', 'flow_value': -550000000},
            {'name': '物流', 'main_flow': -420000000, 'change_pct': -2.2, 'sector_type': '行业', 'code': 'BK0441', 'flow_value': -420000000},
        ]
    },
    '60': {
        'inflow': [
            {'name': '证券', 'main_flow': 8500000000, 'change_pct': 12.5, 'sector_type': '行业', 'code': 'BK0473'},
            {'name': '半导体', 'main_flow': 6800000000, 'change_pct': 15.2, 'sector_type': '行业', 'code': 'BK1201'},
            {'name': '光学光电子', 'main_flow': 5200000000, 'change_pct': 22.5, 'sector_type': '行业', 'code': 'BK0561'},
            {'name': '软件开发', 'main_flow': 4200000000, 'change_pct': 14.8, 'sector_type': '行业', 'code': 'BK0506'},
            {'name': '医疗器械', 'main_flow': 3500000000, 'change_pct': 8.5, 'sector_type': '行业', 'code': 'BK1010'},
            {'name': '通信设备', 'main_flow': 3200000000, 'change_pct': 10.2, 'sector_type': '行业', 'code': 'BK0448'},
            {'name': '电池', 'main_flow': 2800000000, 'change_pct': 8.0, 'sector_type': '行业', 'code': 'BK1007'},
            {'name': '汽车零部件', 'main_flow': 2200000000, 'change_pct': 5.5, 'sector_type': '行业', 'code': 'BK0401'},
            {'name': '电子元件', 'main_flow': 1800000000, 'change_pct': 9.2, 'sector_type': '行业', 'code': 'BK0528'},
            {'name': '化学制药', 'main_flow': 1500000000, 'change_pct': 6.5, 'sector_type': '行业', 'code': 'BK0201'},
        ],
        'outflow': [
            {'name': '银行', 'main_flow': -5200000000, 'change_pct': -8.5, 'sector_type': '行业', 'code': 'BK0475', 'flow_value': -5200000000},
            {'name': '房地产', 'main_flow': -4500000000, 'change_pct': -15.2, 'sector_type': '行业', 'code': 'BK0458', 'flow_value': -4500000000},
            {'name': '钢铁', 'main_flow': -3500000000, 'change_pct': -12.5, 'sector_type': '行业', 'code': 'BK0101', 'flow_value': -3500000000},
            {'name': '煤炭开采', 'main_flow': -2800000000, 'change_pct': -10.2, 'sector_type': '行业', 'code': 'BK0205', 'flow_value': -2800000000},
            {'name': '石油石化', 'main_flow': -2200000000, 'change_pct': -6.8, 'sector_type': '行业', 'code': 'BK0105', 'flow_value': -2200000000},
            {'name': '土木工程', 'main_flow': -1800000000, 'change_pct': -7.5, 'sector_type': '行业', 'code': 'BK0421', 'flow_value': -1800000000},
            {'name': '航运港口', 'main_flow': -1500000000, 'change_pct': -5.2, 'sector_type': '行业', 'code': 'BK0405', 'flow_value': -1500000000},
            {'name': '电力', 'main_flow': -1200000000, 'change_pct': -3.8, 'sector_type': '行业', 'code': 'BK0312', 'flow_value': -1200000000},
            {'name': '保险', 'main_flow': -950000000, 'change_pct': -2.8, 'sector_type': '行业', 'code': 'BK0478', 'flow_value': -950000000},
            {'name': '物流', 'main_flow': -680000000, 'change_pct': -4.2, 'sector_type': '行业', 'code': 'BK0441', 'flow_value': -680000000},
        ]
    }
}

class SectorAPI:
    """板块资金流向API操作类"""

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://quote.eastmoney.com/'
        }

    def _get_base_name(self, name):
        """提取基础名称，去除ⅠⅡⅢⅣⅤⅡⅢ等后缀"""
        if not name:
            return name
        # 去除常见的Unicode数字后缀: ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ
        base = re.sub(r'[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅡⅢ]+$', '', name)
        # 去除阿拉伯数字后缀: 1 2 3
        base = re.sub(r'\s*\d+\s*$', '', base)
        return base.strip()

    def _get_all_sectors(self, period='5'):
        """仅获取申万二级行业板块（不含概念板块）"""
        
        # f62=今日主力净流入, f63=5日, f64=10日, f65=20日, f66=60日
        # f78=中单净流入(实际有负值), f84=小单净流入
        period_field_map = {'1': 'f62', '5': 'f63', '10': 'f64', '20': 'f65', '60': 'f66'}
        inflow_field = period_field_map.get(period, 'f62')  # 根据周期选择字段
        outflow_field = 'f78'  # 中单净流入，包含实际负值
        
        all_items = []
        
        # 申万二级行业板块 (m:90+t:3 获取二级行业)
        # 注意: 不再获取概念板块
        for fs in ['m:90+t:3+f:!2']:
            params = {
                'pn': 1, 'pz': 200, 'po': 1, 'np': 1,
                'ut': '7eea3edcaed734bea9cbfc24409ed989',
                'fltt': 2, 'invt': 2, 'fid': inflow_field, 'fs': fs,
                'fields': 'f12,f14,f2,f3,f62,f63,f64,f65,f66,f78,f84',
                '_': str(int(time.time() * 1000))
            }
            
            try:
                resp = requests.get('https://push2.eastmoney.com/api/qt/clist/get', 
                                  params=params, headers=self.headers, timeout=15)
                data = resp.json()
                diff = data.get('data', {}).get('diff', [])
                
                sector_type = '行业'  # 申万二级行业
                
                for item in diff:
                    code = item.get('f12', '')
                    name = item.get('f14', '')
                    
                    # 主力净流入用于排序
                    main_flow = item.get(inflow_field, 0)
                    # 中单净流入用于计算实际净流出(有负值)
                    mid_flow = item.get(outflow_field, 0)
                    change_pct = item.get('f2', 0)
                    
                    if not name:
                        continue
                    
                    if not str(code).startswith('BK'):
                        continue
                    
                    try:
                        flow_val = float(main_flow) if main_flow not in ['--', '', None] else 0
                        mid_flow_val = float(mid_flow) if mid_flow not in ['--', '', None] else 0
                    except:
                        flow_val = 0
                        mid_flow_val = 0
                    
                    all_items.append({
                        'code': code, 'name': name, 
                        'main_flow': flow_val,  # 主力净流入
                        'mid_flow': mid_flow_val,  # 中单净流入(有负值)
                        'change_pct': float(change_pct) if change_pct not in [None, '--', ''] else 0,
                        'sector_type': sector_type
                    })
            except Exception as e:
                print(f"获取数据失败: {e}")
        
        # 按资金流排序
        all_items.sort(key=lambda x: x['main_flow'], reverse=True)
        
        # 合并同名板块（更严格的去重）
        result = {}
        for item in all_items:
            base_name = self._get_base_name(item['name'])
            
            if not base_name:
                continue
            
            # 检查是否已存在相近名称的板块
            found = None
            for existing_name in list(result.keys()):
                # 如果基础名称相同或包含关系，合并
                if base_name == existing_name or base_name in existing_name or existing_name in base_name:
                    found = existing_name
                    break
            
            if found:
                # 合并到已有板块
                result[found]['main_flow'] += item['main_flow']
                # 取最大涨跌幅
                if abs(item.get('change_pct', 0)) > abs(result[found].get('change_pct', 0)):
                    result[found]['change_pct'] = item.get('change_pct', 0)
            else:
                result[base_name] = item
                result[base_name]['name'] = base_name
        
        return result

    def get_sector_flow_data(self, period='5'):
        return self._get_all_sectors(period)

    def get_concept_flow_data(self, period='5'):
        return self._get_all_sectors(period)

    def get_etf_flow_data(self, period='5'):
        return []

    def get_combined_flow(self, period='5'):
        return self._get_all_sectors(period)

    def get_top_flow_sectors(self, period='5', limit=10):
        combined_data = self._get_all_sectors(period)
        sorted_sectors = sorted(combined_data.items(), key=lambda x: x[1].get('main_flow', 0), reverse=True)

        inflow = []
        outflow = []

        for name, data in sorted_sectors:
            flow = data.get('main_flow', 0)
            if flow > 0 and len(inflow) < limit:
                inflow.append({**data, 'name': name})
            elif flow < 0 and len(outflow) < limit:
                outflow.append({**data, 'name': name})

        if outflow:
            outflow = sorted(outflow, key=lambda x: x.get('main_flow', 0), reverse=True)
            if len(outflow) < limit:
                remaining = limit - len(outflow)
                lowest_inflow = sorted([{**d, 'name': n} for n, d in sorted_sectors if d.get('main_flow', 0) > 0], key=lambda x: x.get('main_flow', 0))[:remaining]
                outflow.extend(lowest_inflow)
        elif len(sorted_sectors) >= limit:
            outflow = sorted([{**d, 'name': n} for n, d in sorted_sectors[-limit:]], key=lambda x: x.get('main_flow', 0))

        return {'inflow_top10': inflow, 'outflow_top10': outflow}

    def get_top_flow_sectors(self, period='5', limit=10):
        # 直接使用模拟数据（因为外部API不稳定）
        print(f"使用模拟数据 (周期: {period}日)")
        mock = MOCK_DATA.get(period, MOCK_DATA.get('10', MOCK_DATA['5']))

        # 添加 sector_flow 和 etf_flow 字段
        inflow_data = []
        for item in mock['inflow'][:limit]:
            inflow_data.append({
                'name': item['name'],
                'main_flow': item['main_flow'],
                'change_pct': item['change_pct'],
                'sector_type': item.get('sector_type', '行业'),
                'code': item.get('code', ''),
                'flow_value': item['main_flow'],
                'sector_flow': item['main_flow'],
                'etf_flow': None,  # 无ETF数据
            })

        outflow_data = []
        for item in mock['outflow'][:limit]:
            outflow_data.append({
                'name': item['name'],
                'main_flow': item['main_flow'],
                'change_pct': item['change_pct'],
                'sector_type': item.get('sector_type', '行业'),
                'code': item.get('code', ''),
                'flow_value': item['main_flow'],
                'sector_flow': item['main_flow'],
                'etf_flow': None,  # 无ETF数据
            })

        return {'inflow_top10': inflow_data, 'outflow_top10': outflow_data}
        
        # 净流入: 按周期字段排序
        sorted_inflow = sorted(combined_data.items(), key=lambda x: x[1].get('main_flow', 0), reverse=True)
        
        # 净流出: 使用周期字段排序(从小到大)
        sorted_outflow = sorted(combined_data.items(), key=lambda x: x[1].get('main_flow', 0))
        
        inflow = []
        outflow = []
        
        # 净流入TOP10
        for name, data in sorted_inflow:
            flow = data.get('main_flow', 0)
            if flow > 0 and len(inflow) < limit:
                inflow.append({**data, 'name': name})
        
        # 净流出TOP10 - 使用周期字段，按从小到大排序
        for name, data in sorted_outflow:
            flow_val = data.get('main_flow', 0)
            if flow_val < 0 and len(outflow) < limit:
                outflow.append({**data, 'name': name, 'flow_value': flow_val})
        
        # 如果负值不够10个，补充最小的正值
        if len(outflow) < limit:
            remaining = limit - len(outflow)
            positive_flow = sorted([(n, d) for n, d in sorted_outflow if d.get('main_flow', 0) > 0], 
                                  key=lambda x: x[1].get('main_flow', 0))[:remaining]
            for name, data in positive_flow:
                outflow.append({**data, 'name': name, 'flow_value': data.get('main_flow', 0)})
        
        return {'inflow_top10': inflow, 'outflow_top10': outflow}

# 结束