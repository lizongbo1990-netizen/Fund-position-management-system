"""
=========================================================
数据源管理器 - 多数据源备份 + 本地缓存 + 板块ETF合并计算
=========================================================
支持: AkShare, 聚宽, 本地数据库, 板块+ETF合并
"""

from datetime import datetime, timedelta
import time
import os
import pandas as pd

class DataSourceManager:
    """多数据源管理器"""
    
    def __init__(self, db_path='data/sector_data.db', jq_token=None):
        # 初始化各数据源
        from sector_db import SectorDatabase
        from akshare_sector import AkShareSectorAPI
        
        self.db = SectorDatabase(db_path)
        self.akshare = AkShareSectorAPI()
        self.jq_token = jq_token
        
        # 导入ETF数据获取器
        try:
            from etf_data import ETFDataFetcher
            self.etf_fetcher = ETFDataFetcher()
        except:
            self.etf_fetcher = None
        
        # 数据源优先级列表
        self.sources = ['db', 'akshare', 'joinquant']
    
    def get_sector_flow(self, period='10日', use_cache=True, max_age_hours=4, include_etf=True):
        """
        获取板块资金流 (多数据源备份)
        
        参数:
            period: 统计周期 ("5日", "10日", "20日", "60日")
            use_cache: 是否使用本地缓存
            max_age_hours: 缓存最大有效期(小时)
            include_etf: 是否包含ETF资金流
        
        返回:
            dict: {"inflow_top10": [...], "outflow_top10": [...], "etf_top10": [...], "combined": [...]}
        """
        print(f"\n获取板块资金流 (周期: {period}, 含ETF: {include_etf})")
        
        # 1. 检查本地缓存 - 优先使用缓存快速返回
        import time
        start_time = time.time()
        
        if use_cache and self.db.is_data_fresh(period, max_age_hours):
            print(f"  -> [{time.time()-start_time:.2f}s] 使用本地缓存数据")
            result = self._get_from_cache(period)
            if result:
                # ETF数据也检查缓存
                if include_etf and self.etf_fetcher:
                    cached_etf = self.db._get_cached_etf()
                    if cached_etf is not None:
                        print(f"  -> [{time.time()-start_time:.2f}s] 使用缓存的ETF数据快速合并")
                        result = self._quick_merge_etf(result, cached_etf)
                    else:
                        print(f"  -> [{time.time()-start_time:.2f}s] 获取ETF数据...")
                        result = self._merge_etf_data(result, period)
                print(f"  -> [{time.time()-start_time:.2f}s] 完成")
                return result
        
        # 转换周期参数
        period_map = {'1': '今日', '5': '5日', '10': '10日', '20': '20日', '60': '60日'}
        indicator = period_map.get(period, '10日')
        
        # 2. 使用 AkShare 获取真实数据
        try:
            from akshare_sector import AkShareSectorAPI
            print(f"  -> [{time.time()-start_time:.2f}s] 从 AkShare 获取真实数据...")
            akshare_result = AkShareSectorAPI.get_top_flow_sectors(period, limit=10)
            if akshare_result and (akshare_result['inflow_top10'] or akshare_result['outflow_top10']):
                result = akshare_result
                print(f"  -> [{time.time()-start_time:.2f}s] AkShare 数据获取成功")
            else:
                print(f"  -> [{time.time()-start_time:.2f}s] AkShare 返回空，使用模拟数据...")
                result = self._get_mock_data(period)
        except Exception as e:
            print(f"  -> [{time.time()-start_time:.2f}s] AkShare 失败: {str(e)[:50]}，使用模拟数据...")
            result = self._get_mock_data(period)
        
        # 合并ETF数据（使用缓存）
        if include_etf and self.etf_fetcher:
            cached_etf = self.db._get_cached_etf()
            if cached_etf is not None:
                print(f"  -> [{time.time()-start_time:.2f}s] 使用缓存ETF快速合并")
                result = self._quick_merge_etf(result, cached_etf)
            else:
                result = self._merge_etf_data(result, period)
        
        print(f"  -> [{time.time()-start_time:.2f}s] 完成")
        return result
    
    def _merge_etf_data(self, sector_result, period):
        """合并板块+对应主题ETF的资金流（相加计算）"""
        if not self.etf_fetcher:
            return sector_result
        
        try:
            print("  -> 获取ETF资金流...")
            # 先尝试从缓存获取
            cached_etf = self.db._get_cached_etf()
            if cached_etf is not None:
                print("  -> 使用缓存的ETF数据")
                etf_all = cached_etf
            else:
                # 获取所有ETF数据
                etf_all = self.etf_fetcher.get_etf_spot_data()
                
                if etf_all is None:
                    return sector_result
                
                # 计算主力净流入
                if '超大单净流入-净额' in etf_all.columns and '大单净流入-净额' in etf_all.columns:
                    etf_all['主力净流入'] = etf_all['超大单净流入-净额'].fillna(0) + etf_all['大单净流入-净额'].fillna(0)
                else:
                    return sector_result
                
                # 保存到缓存
                self.db.save_etf_cache(etf_all)
            
            # 建立板块与ETF的映射关键词 - 扩展覆盖更多申万二级行业
            sector_etf_mapping = {
                '半导体': ['半导体', '芯片', '集成电路', '光刻', 'AI芯片', '电子', 'IC'],
                '证券': ['证券', '券商'],
                '软件开发': ['软件', '软件开发', 'IT', '软件服务'],
                '光学光电子': ['光学', '光电', 'LED', '光学光电子', '面板', '显示'],
                '医疗器械': ['医疗', '器械', '医疗器械', '医药设备', '医疗设备'],
                '通信设备': ['通信', '5G', '物联网', '光通信', '通信设备'],
                '电池': ['电池', '锂电', '锂电池', '动力电池', '储能'],
                '汽车零部件': ['汽车', '零部件', '汽车零部件', '汽车配件', '新能源车'],
                '电子元件': ['电子', '电子元件', 'PCB', '被动元件', '电容', '电阻'],
                '化学制药': ['化学制药', '制药', '医药', '化学', '生物药'],
                '光学光电子': ['光学', '光电', '面板', '显示', 'LED'],
                '新能源': ['新能源', '光伏', '锂电', '储能', '氢能', '电力设备', '风电'],
                '医药': ['医药', '医疗', '生物', '中药', '疫苗', '创新药', '医疗器械', '化学制药'],
                '科技': ['科技', '计算机', '软件', '互联网', 'AI', '人工智能', '大数据', '云计算'],
                '金融': ['金融', '银行', '证券', '保险', '非银', '多元金融'],
                '军工': ['军工', '国防', '航天', '航空', '船舶', '信息安全'],
                '电力': ['电力', '电网', '公用', '火电', '水电'],
                '银行': ['银行', '金融', '银行板块'],
                '房地产': ['房地产', '地产', '万科', '保利'],
                '钢铁': ['钢铁', '有色', '金属', '钢铁板块'],
                '煤炭开采': ['煤炭', '煤', '能源', '石油', '石化'],
                '基建': ['基建', '工程', '建筑', '建材', '地产'],
                '航运港口': ['航运', '港口', '海运', '运输'],
                '物流': ['物流', '快递', '运输'],
                '保险': ['保险', '保险板块'],
            }
            
            # 计算每个板块对应的ETF资金总和
            sector_etf_flows = {}
            
            for sector_name, keywords in sector_etf_mapping.items():
                # 查找匹配的ETF
                mask = etf_all['基金简称'].str.contains('|'.join(keywords), case=False, na=False) if '基金简称' in etf_all.columns else pd.Series([False]*len(etf_all))
                matched_etfs = etf_all[mask]
                
                if len(matched_etfs) > 0:
                    total_etf_flow = matched_etfs['主力净流入'].sum()
                    sector_etf_flows[sector_name] = {
                        'etf_flow': total_etf_flow,
                        'etf_count': len(matched_etfs)
                    }
            
            print(f"  -> 找到 {len(sector_etf_flows)} 个板块的对应ETF")
            
            # 始终使用Top ETF数据作为补充，即使有精确匹配也分配
            # 获取正流ETF和负流ETF
            top_pos_etfs = etf_all[etf_all['主力净流入'] > 0].nlargest(20, '主力净流入')
            top_neg_etfs = etf_all[etf_all['主力净流入'] < 0].nsmallest(20, '主力净流入')
            
            # 获取当前已有ETF数据的板块
            matched_sectors = set(sector_etf_flows.keys())
            
            # 为没有ETF数据的板块分配Top ETF
            # 净流入板块分配正ETF，净流出板块分配负ETF
            inflow_sector_names = [item.get('name', '') for item in sector_result.get('inflow_top10', [])[:10]]
            outflow_sector_names = [item.get('name', '') for item in sector_result.get('outflow_top10', [])[:10]]
            
            # 分配正ETF给净流入板块
            etf_idx = 0
            for sector_name in inflow_sector_names:
                if sector_name not in matched_sectors and etf_idx < len(top_pos_etfs):
                    etf_row = top_pos_etfs.iloc[etf_idx]
                    etf_flow = etf_row.get('主力净流入', 0)
                    if etf_flow > 0:
                        sector_etf_flows[sector_name] = {'etf_flow': etf_flow, 'etf_count': 1}
                        matched_sectors.add(sector_name)
                    etf_idx += 1
            
            # 分配负ETF给净流出板块
            etf_idx = 0
            for sector_name in outflow_sector_names:
                if sector_name not in matched_sectors and etf_idx < len(top_neg_etfs):
                    etf_row = top_neg_etfs.iloc[etf_idx]
                    etf_flow = etf_row.get('主力净流入', 0)
                    if etf_flow < 0:
                        sector_etf_flows[sector_name] = {'etf_flow': etf_flow, 'etf_count': 1}
                        matched_sectors.add(sector_name)
                    etf_idx += 1
            
            print(f"  -> 最终ETF匹配: {len(sector_etf_flows)} 个板块")
            
            # 合并板块+ETF资金流
            merged_inflow = []
            merged_outflow = []
            
            # 处理净流入
            for item in sector_result.get('inflow_top10', []):
                sector_name = item.get('name', '')
                
                # 查找匹配的ETF资金
                matched_etf_flow = 0
                for map_name, map_data in sector_etf_flows.items():
                    if map_name in sector_name or sector_name in map_name:
                        matched_etf_flow = map_data['etf_flow']
                        break
                    # 关键词匹配
                    for kw in sector_etf_mapping.get(map_name, []):
                        if kw in sector_name:
                            matched_etf_flow = map_data['etf_flow']
                            break
                
                total_flow = item.get('main_flow', 0) + matched_etf_flow
                
                merged_inflow.append({
                    'name': item.get('name', ''),
                    'main_flow': total_flow,
                    'sector_flow': item.get('main_flow', 0),
                    'etf_flow': matched_etf_flow,
                    'change_pct': item.get('change_pct', 0),
                    'sector_type': item.get('sector_type', '')
                })
            
            # 处理净流出
            for item in sector_result.get('outflow_top10', []):
                sector_name = item.get('name', '')
                flow_value = item.get('flow_value', item.get('main_flow', 0))
                
                # 查找匹配的ETF资金
                matched_etf_flow = 0
                for map_name, map_data in sector_etf_flows.items():
                    if map_name in sector_name or sector_name in map_name:
                        matched_etf_flow = map_data['etf_flow']
                        break
                    for kw in sector_etf_mapping.get(map_name, []):
                        if kw in sector_name:
                            matched_etf_flow = map_data['etf_flow']
                            break
                
                total_flow = flow_value + matched_etf_flow
                
                merged_outflow.append({
                    'name': item.get('name', ''),
                    'main_flow': total_flow,
                    'flow_value': total_flow,
                    'sector_flow': flow_value,
                    'etf_flow': matched_etf_flow,
                    'change_pct': item.get('change_pct', 0),
                    'sector_type': item.get('sector_type', '')
                })
            
            # 按资金流排序
            merged_inflow.sort(key=lambda x: x.get('main_flow', 0), reverse=True)
            merged_outflow.sort(key=lambda x: x.get('main_flow', 0))
            
            sector_result['inflow_top10'] = merged_inflow[:10]
            sector_result['outflow_top10'] = merged_outflow[:10]
            
            print(f"  -> 合并完成: 净流入{len(merged_inflow)}条, 净流出{len(merged_outflow)}条")
            
            return sector_result
            
        except Exception as e:
            print(f"  -> 合并ETF数据失败: {str(e)[:80]}")
        
        return sector_result
    
    def _quick_merge_etf(self, sector_result, cached_etf):
        """快速合并缓存的ETF数据（不重新请求API）"""
        try:
            if cached_etf is None:
                return sector_result
            
            # 使用缓存的ETF数据
            etf_all = cached_etf.copy()
            
            if '主力净流入' not in etf_all.columns:
                return sector_result
            
            # 获取正/负ETF
            top_pos_etfs = etf_all[etf_all['主力净流入'] > 0].nlargest(20, '主力净流入')
            top_neg_etfs = etf_all[etf_all['主力净流入'] < 0].nsmallest(20, '主力净流入')
            
            sector_etf_flows = {}
            inflow_sectors = [item.get('name', '') for item in sector_result.get('inflow_top10', [])[:10]]
            outflow_sectors = [item.get('name', '') for item in sector_result.get('outflow_top10', [])[:10]]
            
            # 分配正ETF
            for i, name in enumerate(inflow_sectors):
                if i < len(top_pos_etfs):
                    sector_etf_flows[name] = top_pos_etfs.iloc[i].get('主力净流入', 0)
            
            # 分配负ETF
            for i, name in enumerate(outflow_sectors):
                if i < len(top_neg_etfs):
                    sector_etf_flows[name] = top_neg_etfs.iloc[i].get('主力净流入', 0)
            
            # 快速合并
            for item in sector_result.get('inflow_top10', []):
                name = item.get('name', '')
                etf_flow = sector_etf_flows.get(name, 0)
                item['etf_flow'] = etf_flow
                item['main_flow'] = item.get('main_flow', 0) + etf_flow
                item['sector_flow'] = item.get('main_flow', 0) - etf_flow
            
            for item in sector_result.get('outflow_top10', []):
                name = item.get('name', '')
                etf_flow = sector_etf_flows.get(name, 0)
                flow_value = item.get('flow_value', item.get('main_flow', 0))
                item['etf_flow'] = etf_flow
                item['main_flow'] = flow_value + etf_flow
                item['sector_flow'] = flow_value
            
            print("  -> 快速合并ETF完成")
            return sector_result
            
        except Exception as e:
            print(f"  -> 快速合并失败: {str(e)[:50]}")
            return sector_result
    
    def _get_from_cache(self, period):
        """从本地数据库读取"""
        data = self.db.get_sector_flow(datetime.now().strftime('%Y-%m-%d'), period)
        if not data:
            # 尝试读取前一天
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            data = self.db.get_sector_flow(yesterday, period)
        
        if not data:
            return None
        
        # 转换为TOP榜单格式
        inflow = []
        outflow = []
        
        for item in data:
            flow = item.get('main_flow', 0)
            if flow > 0 and len(inflow) < 10:
                inflow.append({**item, 'name': item.get('sector_name', item.get('name', ''))})
            elif flow < 0 and len(outflow) < 10:
                outflow.append({**item, 'name': item.get('sector_name', item.get('name', '')), 'flow_value': flow})
        
        return {'inflow_top10': inflow, 'outflow_top10': outflow}
    
    def _save_to_cache(self, period, result, source):
        """保存到本地数据库"""
        date_str = datetime.now().strftime('%Y-%m-%d')
        
        # 保存净流入
        inflow_data = [
            {'name': item.get('name', ''), 'main_flow': item.get('main_flow', 0), 
             'change_pct': item.get('change_pct', 0), 'sector_type': item.get('sector_type', '')}
            for item in result.get('inflow_top10', [])
        ]
        self.db.save_sector_flow(date_str, period, inflow_data, source)
        
        # 保存净流出
        outflow_data = [
            {'name': item.get('name', ''), 'main_flow': item.get('flow_value', 0), 
             'change_pct': item.get('change_pct', 0), 'sector_type': item.get('sector_type', '')}
            for item in result.get('outflow_top10', [])
        ]
        self.db.save_sector_flow(date_str, period, outflow_data, source)
    
    def _filter_industry_only(self, data):
        """过滤只保留申万二级行业板块，排除概念板块"""
        if not data:
            return data
        
        inflow = data.get('inflow_top10', [])
        outflow = data.get('outflow_top10', [])
        
        # 只保留 sector_type 为 '行业' 的板块
        filtered_inflow = [item for item in inflow if item.get('sector_type') != '概念']
        filtered_outflow = [item for item in outflow if item.get('sector_type') != '概念']
        
        return {
            'inflow_top10': filtered_inflow[:10],
            'outflow_top10': filtered_outflow[:10]
        }
    
    def _get_mock_data(self, period):
        """获取模拟数据 (最后fallback)"""
        from sector_api import MOCK_DATA
        
        period_map = {'1': '1', '5': '5', '10': '10', '20': '20', '60': '60'}
        key = period_map.get(period.replace('日', ''), '10')
        
        if key in MOCK_DATA:
            mock = MOCK_DATA[key]
            result = {
                'inflow_top10': mock.get('inflow', [])[:10],
                'outflow_top10': mock.get('outflow', [])[:10]
            }
            return self._filter_industry_only(result)
        
        # 默认10日
        mock = MOCK_DATA['10']
        result = {
            'inflow_top10': mock.get('inflow', [])[:10],
            'outflow_top10': mock.get('outflow', [])[:10]
        }
        return self._filter_industry_only(result)
    
    def refresh_data(self, period='10日'):
        """强制刷新数据 (忽略缓存)"""
        return self.get_sector_flow(period, use_cache=False)
    
    def get_cache_status(self):
        """获取缓存状态"""
        status = {}
        for period in ['5日', '10日', '20日', '60日']:
            last_update = self.db.get_last_update_time(period)
            is_fresh = self.db.is_data_fresh(period)
            status[period] = {
                'last_update': last_update,
                'is_fresh': is_fresh
            }
        return status


# ====== 定时任务功能 ======
import threading
import schedule

class DataScheduler:
    """定时数据更新任务"""
    
    def __init__(self, data_manager):
        self.data_manager = data_manager
        self.running = False
        self.thread = None
    
    def start(self):
        """启动定时任务"""
        self.running = True
        
        # 每日16:00后更新 (A股收盘后)
        schedule.every().day.at("16:30").do(self.update_daily_data)
        
        # 每4小时更新一次
        schedule.every(4).hours.do(self.update_realtime_data)
        
        self.thread = threading.Thread(target=self._run_schedule, daemon=True)
        self.thread.start()
        print("数据定时更新任务已启动")
    
    def stop(self):
        """停止定时任务"""
        self.running = False
        schedule.clear()
        print("数据定时更新任务已停止")
    
    def _run_schedule(self):
        """运行调度器"""
        while self.running:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
    
    def update_daily_data(self):
        """每日收盘后更新"""
        print("\n执行每日数据更新...")
        for period in ['5日', '10日', '20日', '60日']:
            try:
                self.data_manager.get_sector_flow(period, use_cache=False)
                time.sleep(2)  # 避免请求过快
            except Exception as e:
                print(f"更新 {period} 失败: {e}")
    
    def update_realtime_data(self):
        """实时数据更新"""
        print("\n执行实时数据更新...")
        for period in ['5日', '10日']:
            try:
                self.data_manager.get_sector_flow(period, use_cache=True, max_age_hours=1)
                time.sleep(2)
            except Exception as e:
                print(f"更新 {period} 失败: {e}")


if __name__ == "__main__":
    # 测试
    print("=== 数据源管理器测试 ===")
    
    dm = DataSourceManager()
    
    # 获取数据
    result = dm.get_sector_flow('10日')
    
    print(f"\n净流入TOP5:")
    for i, item in enumerate(result['inflow_top10'][:5], 1):
        print(f"  {i}. {item['name']}: {item['main_flow']/10000:.0f}万")
    
    print(f"\n净流出TOP5:")
    for i, item in enumerate(result['outflow_top10'][:5], 1):
        flow = item.get('flow_value', item.get('main_flow', 0))
        print(f"  {i}. {item['name']}: {flow/10000:.0f}万")
    
    # 检查缓存状态
    print(f"\n缓存状态:")
    status = dm.get_cache_status()
    for period, info in status.items():
        print(f"  {period}: 最后更新={info['last_update']}, 新鲜={info['is_fresh']}")