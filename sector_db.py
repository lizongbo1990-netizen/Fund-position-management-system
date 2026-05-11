"""
=========================================================
本地数据库模块 - SQLite存储板块资金流数据
=========================================================
"""

import sqlite3
import os
from datetime import datetime

class SectorDatabase:
    """本地SQLite数据库存储"""
    
    def __init__(self, db_path='data/sector_data.db'):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """初始化数据库"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 创建板块资金流表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sector_flow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                period TEXT NOT NULL,
                sector_name TEXT NOT NULL,
                sector_type TEXT,
                main_flow REAL,
                change_pct REAL,
                source TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, period, sector_name)
            )
        ''')
        
        # 创建ETF数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS etf_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT,
                net_value REAL,
                unit_net REAL,
                total规模 REAL,
                net规模 REAL,
                net_change REAL,
                source TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, code)
            )
        ''')
        
        # 创建元数据表 - 记录最后更新时间
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def save_sector_flow(self, date, period, data_list, source='akshare'):
        """保存板块资金流数据"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for item in data_list:
            cursor.execute('''
                INSERT OR REPLACE INTO sector_flow 
                (date, period, sector_name, sector_type, main_flow, change_pct, source)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                date, period, 
                item.get('name', ''),
                item.get('sector_type', ''),
                item.get('main_flow', 0),
                item.get('change_pct', 0),
                source
            ))
        
        # 更新元数据
        cursor.execute('''
            INSERT OR REPLACE INTO metadata (key, value, updated_at)
            VALUES (?, ?, ?)
        ''', (f'sector_flow_{period}', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        print(f"保存 {len(data_list)} 条板块数据 ({date} {period} {source})")
    
    def get_sector_flow(self, date, period):
        """获取板块资金流数据"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT sector_name, sector_type, main_flow, change_pct
            FROM sector_flow
            WHERE date = ? AND period = ?
            ORDER BY main_flow DESC
        ''', (date, period))
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'name': row[0],
                'sector_type': row[1],
                'main_flow': row[2],
                'change_pct': row[3]
            })
        
        conn.close()
        return results
    
    def get_last_update_time(self, period):
        """获取最后更新时间"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT value FROM metadata
            WHERE key = ?
        ''', (f'sector_flow_{period}',))
        
        result = cursor.fetchone()
        conn.close()
        
        return result[0] if result else None
    
    def is_data_fresh(self, period, max_age_hours=4):
        """检查数据是否新鲜 (默认4小时内)"""
        last_update = self.get_last_update_time(period)
        if not last_update:
            return False
        
        try:
            last_dt = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
            hours_diff = (datetime.now() - last_dt).total_seconds() / 3600
            return hours_diff < max_age_hours
        except:
            return False
    
    def save_etf_cache(self, etf_df):
        """保存ETF数据缓存"""
        import pickle
        import os
        cache_dir = os.path.dirname(self.db_path)
        cache_file = os.path.join(cache_dir, 'etf_cache.pkl')
        os.makedirs(cache_dir, exist_ok=True)
        
        with open(cache_file, 'wb') as f:
            pickle.dump({
                'data': etf_df,
                'timestamp': datetime.now()
            }, f)
    
    def _get_cached_etf(self):
        """获取缓存的ETF数据"""
        import pickle
        import os
        cache_dir = os.path.dirname(self.db_path)
        cache_file = os.path.join(cache_dir, 'etf_cache.pkl')
        
        if not os.path.exists(cache_file):
            return None
        
        try:
            with open(cache_file, 'rb') as f:
                cache = pickle.load(f)
            
            # 检查是否在2小时内
            hours_diff = (datetime.now() - cache['timestamp']).total_seconds() / 3600
            if hours_diff < 2:
                return cache['data']
        except:
            pass
        
        return None


if __name__ == "__main__":
    # 测试
    db = SectorDatabase()
    
    # 测试保存数据
    test_data = [
        {'name': '半导体', 'main_flow': 1000000000, 'change_pct': 2.5, 'sector_type': '行业'},
        {'name': '证券', 'main_flow': 800000000, 'change_pct': 1.2, 'sector_type': '行业'},
    ]
    
    db.save_sector_flow('2025-04-29', '10日', test_data, 'test')
    
    # 测试读取
    data = db.get_sector_flow('2025-04-29', '10日')
    print(f"读取到 {len(data)} 条数据")
    print(data)
    
    # 检查数据新鲜度
    print(f"10日数据新鲜度: {db.is_data_fresh('10日')}")