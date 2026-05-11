"""
=========================================================
数据库模块 - SQLite数据库操作
=========================================================
用于存储基金持仓数据
"""

import sqlite3
import os
from datetime import datetime

class Database:
    """数据库操作类"""

    def __init__(self):
        """初始化数据库连接"""
        self.db_path = os.path.join(os.path.dirname(__file__), 'investment.db')
        self.conn = None

    def get_connection(self):
        """获取数据库连接"""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
        return self.conn

    def init_db(self):
        """初始化数据库表"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # 创建用户表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 创建基金持仓表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS funds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                fund_code TEXT NOT NULL,
                fund_name TEXT,
                hold_amount REAL DEFAULT 0,
                hold_income REAL DEFAULT 0,
                hold_shares REAL DEFAULT 0,
                cost_price REAL DEFAULT 0,
                base_nav REAL DEFAULT 0,
                base_nav_date TEXT DEFAULT '',
                yesterday_nav REAL DEFAULT 0,
                yesterday_income REAL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ''')

        # 创建板块资金流向缓存表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sector_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period TEXT NOT NULL,
                data TEXT,
                update_time TEXT
            )
        ''')

        # 创建系统设置表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT NOT NULL UNIQUE,
                value TEXT
            )
        ''')

        # 创建加仓记录表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS buy_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_id INTEGER NOT NULL,
                buy_amount REAL NOT NULL,
                buy_fee_rate REAL DEFAULT 0,
                buy_fee REAL DEFAULT 0,
                buy_time TEXT,
                effective_date TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 创建减仓记录表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sell_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_id INTEGER NOT NULL,
                sell_shares REAL NOT NULL,
                sell_amount REAL NOT NULL,
                sell_fee_rate REAL DEFAULT 0,
                sell_fee REAL DEFAULT 0,
                sell_time TEXT,
                effective_date TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        conn.commit()
        print("数据库初始化完成")

    def get_all_funds(self):
        """获取所有基金持仓"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM funds ORDER BY id DESC')
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def add_fund(self, fund_code, fund_name, hold_amount, hold_income):
        """添加基金持仓"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO funds (fund_code, fund_name, hold_amount, hold_income, hold_shares, cost_price)
            VALUES (?, ?, ?, ?, 0, 0)
        ''', (fund_code, fund_name, hold_amount, hold_income))
        conn.commit()

    def update_fund(self, fund_id, fund_code, fund_name, hold_amount, hold_income):
        """更新基金持仓"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE funds
            SET fund_code = ?, fund_name = ?, hold_amount = ?, hold_income = ?,
                updated_at = ?
            WHERE id = ?
        ''', (fund_code, fund_name, hold_amount, hold_income, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), fund_id))
        conn.commit()

    def update_fund_shares_and_cost(self, fund_id, hold_shares, cost_price):
        """更新基金持有份额和成本价"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE funds
            SET hold_shares = ?, cost_price = ?, updated_at = ?
            WHERE id = ?
        ''', (hold_shares, cost_price, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), fund_id))
        conn.commit()

    def update_fund_base_nav(self, fund_id, base_nav, base_nav_date, yesterday_nav, yesterday_income):
        """更新基金基准净值和昨日数据"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE funds
            SET base_nav = ?, base_nav_date = ?, yesterday_nav = ?, yesterday_income = ?, updated_at = ?
            WHERE id = ?
        ''', (base_nav, base_nav_date, yesterday_nav, yesterday_income, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), fund_id))
        conn.commit()

    def delete_fund(self, fund_id):
        """删除基金持仓"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM funds WHERE id = ?', (fund_id,))
        conn.commit()

    def get_fund_by_code(self, fund_code):
        """根据代码获取基金"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM funds WHERE fund_code = ?', (fund_code,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def save_sector_cache(self, period, data):
        """保存板块资金流向缓存"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # 先删除旧数据
        cursor.execute('DELETE FROM sector_cache WHERE period = ?', (period,))

        # 插入新数据
        cursor.execute('''
            INSERT INTO sector_cache (period, data, update_time)
            VALUES (?, ?, ?)
        ''', (period, data, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        conn.commit()

    def get_sector_cache(self, period):
        """获取板块资金流向缓存"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM sector_cache WHERE period = ?', (period,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def add_buy_record(self, fund_id, buy_amount, buy_fee_rate, buy_fee, buy_time, effective_date):
        """添加加仓记录"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO buy_records (fund_id, buy_amount, buy_fee_rate, buy_fee, buy_time, effective_date)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (fund_id, buy_amount, buy_fee_rate, buy_fee, buy_time, effective_date))
        conn.commit()
        return cursor.lastrowid

    def add_sell_record(self, fund_id, sell_shares, sell_amount, sell_fee_rate, sell_fee, sell_time, effective_date):
        """添加减仓记录"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO sell_records (fund_id, sell_shares, sell_amount, sell_fee_rate, sell_fee, sell_time, effective_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (fund_id, sell_shares, sell_amount, sell_fee_rate, sell_fee, sell_time, effective_date))
        conn.commit()
        return cursor.lastrowid

    def update_fund_amount_only(self, fund_id, new_amount):
        """仅更新基金持有金额"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE funds SET hold_amount = ?, updated_at = ? WHERE id = ?
        ''', (new_amount, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), fund_id))
        conn.commit()

    def add_fund_amount(self, fund_id, add_amount):
        """增加基金持有金额"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT hold_amount FROM funds WHERE id = ?', (fund_id,))
        row = cursor.fetchone()
        if row:
            new_amount = row[0] + add_amount
            cursor.execute('''
                UPDATE funds SET hold_amount = ?, updated_at = ? WHERE id = ?
            ''', (new_amount, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), fund_id))
            conn.commit()
            return new_amount
        return None

    def get_pending_buy_records(self):
        """获取待生效的加仓记录"""
        conn = self.get_connection()
        cursor = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute('SELECT * FROM buy_records WHERE effective_date <= ? AND status = ?', (today, 'pending'))
        return [dict(row) for row in cursor.fetchall()]

    def process_pending_trades(self, user_id):
        """处理待生效的交易记录，更新持仓金额"""
        conn = self.get_connection()
        cursor = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')

        cursor.execute('SELECT * FROM buy_records br JOIN funds f ON br.fund_id = f.id WHERE f.user_id = ? AND br.effective_date <= ? AND br.status = ?', (user_id, today, 'pending'))
        pending_buys = [dict(row) for row in cursor.fetchall()]

        for record in pending_buys:
            new_amount = self.add_fund_amount(record['fund_id'], record['buy_amount'])
            if new_amount:
                self.update_buy_record_status(record['id'], 'applied')
                print(f"处理加仓记录 {record['id']}: 添加 {record['buy_amount']} 元, 新持有金额 {new_amount}")

        cursor.execute('SELECT * FROM sell_records sr JOIN funds f ON sr.fund_id = f.id WHERE f.user_id = ? AND sr.effective_date <= ? AND sr.status = ?', (user_id, today, 'pending'))
        pending_sells = [dict(row) for row in cursor.fetchall()]

        for record in pending_sells:
            self.update_sell_record_status(record['id'], 'applied')
            print(f"处理减仓记录 {record['id']}")

    def get_pending_sell_records(self):
        """获取待生效的减仓记录"""
        conn = self.get_connection()
        cursor = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute('SELECT * FROM sell_records WHERE effective_date <= ? AND status = ?', (today, 'pending'))
        return [dict(row) for row in cursor.fetchall()]

    def get_all_buy_records(self):
        """获取所有买入记录，关联基金名称"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT b.*, f.fund_name, f.fund_code 
            FROM buy_records b 
            LEFT JOIN funds f ON b.fund_id = f.id 
            ORDER BY b.created_at DESC
        ''')
        return [dict(row) for row in cursor.fetchall()]

    def get_all_sell_records(self):
        """获取所有卖出记录，关联基金名称"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT s.*, f.fund_name, f.fund_code 
            FROM sell_records s 
            LEFT JOIN funds f ON s.fund_id = f.id 
            ORDER BY s.created_at DESC
        ''')
        return [dict(row) for row in cursor.fetchall()]

    def update_record_status(self, record_id, table, status):
        """更新记录状态"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(f'UPDATE {table} SET status = ? WHERE id = ?', (status, record_id))
        conn.commit()

    # ==========================================================
    # 用户认证相关方法
    # ==========================================================
    def create_user(self, username, password):
        """创建用户"""
        import hashlib
        conn = self.get_connection()
        cursor = conn.cursor()
        hashed_password = hashlib.sha256(password.encode()).hexdigest()
        try:
            cursor.execute('INSERT INTO users (username, password) VALUES (?, ?)', (username, hashed_password))
            conn.commit()
            return True
        except:
            return False

    def verify_user(self, username, password):
        """验证用户登录"""
        import hashlib
        conn = self.get_connection()
        cursor = conn.cursor()
        hashed_password = hashlib.sha256(password.encode()).hexdigest()
        cursor.execute('SELECT id FROM users WHERE username = ? AND password = ?', (username, hashed_password))
        row = cursor.fetchone()
        return row['id'] if row else None

    def get_user_by_id(self, user_id):
        """根据ID获取用户"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id, username FROM users WHERE id = ?', (user_id,))
        return dict(cursor.fetchone()) if cursor.fetchone() else None

    def get_funds_by_user(self, user_id):
        """获取用户的所有基金持仓"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM funds WHERE user_id = ? ORDER BY id DESC', (user_id,))
        return [dict(row) for row in cursor.fetchall()]

    def add_fund_for_user(self, user_id, fund_code, fund_name, hold_amount, hold_income):
        """为用户添加基金持仓"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO funds (user_id, fund_code, fund_name, hold_amount, hold_income, hold_shares, cost_price)
            VALUES (?, ?, ?, ?, ?, 0, 0)
        ''', (user_id, fund_code, fund_name, hold_amount, hold_income))
        conn.commit()

    def update_fund_for_user(self, fund_id, user_id, fund_code, fund_name, hold_amount, hold_income):
        """更新用户基金持仓"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE funds
            SET fund_code = ?, fund_name = ?, hold_amount = ?, hold_income = ?, updated_at = ?
            WHERE id = ? AND user_id = ?
        ''', (fund_code, fund_name, hold_amount, hold_income, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), fund_id, user_id))
        conn.commit()

    def delete_fund_for_user(self, fund_id, user_id):
        """删除用户基金持仓"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM funds WHERE id = ? AND user_id = ?', (fund_id, user_id))
        conn.commit()

    def get_fund_by_code_for_user(self, user_id, fund_code):
        """根据代码获取用户基金"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM funds WHERE user_id = ? AND fund_code = ?', (user_id, fund_code))
        return dict(cursor.fetchone()) if cursor.fetchone() else None

    def get_buy_records(self, user_id):
        """获取用户的加仓记录"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT br.*, f.fund_code, f.fund_name
            FROM buy_records br
            JOIN funds f ON br.fund_id = f.id
            WHERE f.user_id = ?
            ORDER BY br.created_at DESC
        ''', (user_id,))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def get_sell_records(self, user_id):
        """获取用户的减仓记录"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT sr.*, f.fund_code, f.fund_name
            FROM sell_records sr
            JOIN funds f ON sr.fund_id = f.id
            WHERE f.user_id = ?
            ORDER BY sr.created_at DESC
        ''', (user_id,))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def update_buy_record_status(self, record_id, status):
        """更新加仓记录状态"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE buy_records SET status = ? WHERE id = ?', (status, record_id))
        conn.commit()

    def update_sell_record_status(self, record_id, status):
        """更新减仓记录状态"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE sell_records SET status = ? WHERE id = ?', (status, record_id))
        conn.commit()