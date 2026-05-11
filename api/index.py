"""
========================================================
个人投资工具 - 主应用入口
========================================================
功能：
1. 场外基金持仓实时收益跟踪
2. A股细分板块资金流向排名

作者：个人投资者
版本：1.0.0
"""

import os
import json
import time
import sqlite3
import threading
import pandas as pd
from datetime import datetime, date, timedelta
from flask import Flask, render_template, jsonify, request, send_file, session
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler

# 导入自定义模块
from fund_api import FundAPI
from database import Database
from data_manager import DataSourceManager

# ==========================================================
# Flask应用配置
# ==========================================================
app = Flask(__name__)
app.config['SECRET_KEY'] = 'investment_tracker_secret_key_2024'
app.config['JSON_AS_ASCII'] = False
CORS(app, supports_credentials=True)

# 初始化各模块
db = Database()
fund_api = FundAPI()
sector_manager = DataSourceManager(jq_token="17737506140,Vip19900919Lzb")

# 全局缓存
fund_cache = {}
sector_cache = {}
last_update_time = None
site_name_cache = '基金持仓管理系统'

# ==========================================================
# 定时任务：自动刷新数据
# ==========================================================
def scheduled_update():
    """定时更新基金实时估值和板块资金流向"""
    global fund_cache, sector_cache, last_update_time
    current_time = datetime.now()

    # 只在交易时间段更新（9:30-15:00）
    if current_time.hour >= 9 and current_time.hour < 15:
        if current_time.weekday() < 5:
            print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] 正在更新数据...")
            fund_cache = fund_api.get_realtime_estimates()
            last_update_time = current_time.strftime('%Y-%m-%d %H:%M:%S')

# 启动定时任务
scheduler = BackgroundScheduler()
scheduler.add_job(func=scheduled_update, trigger='interval', seconds=60)
scheduler.start()

# ==========================================================
# 认证相关
# ==========================================================
def login_required(f):
    """登录装饰器"""
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('user_id'):
            return jsonify({'code': 1, 'msg': '请先登录'})
        return f(*args, **kwargs)
    return decorated_function

def get_current_user_id():
    """获取当前用户ID"""
    return session.get('user_id')

@app.route('/api/auth/register', methods=['POST'])
def register():
    """用户注册"""
    data = request.get_json()
    username = data.get('username', '').strip()
    password = data.get('password', '')

    if not username or not password:
        return jsonify({'code': 1, 'msg': '请填写用户名和密码'})

    if db.create_user(username, password):
        return jsonify({'code': 0, 'msg': '注册成功'})
    return jsonify({'code': 1, 'msg': '用户名已存在'})

# 登录并跳转页面
@app.route('/login', methods=['POST'])
def login_and_redirect():
    """登录后跳转"""
    if request.is_json:
        data = request.get_json()
    else:
        data = request.form.to_dict() if request.form else {}
    
    username = data.get('username', '').strip() if data.get('username') else ''
    password = data.get('password', '')

    user_id = db.verify_user(username, password)
    if user_id:
        session['user_id'] = user_id
        session['username'] = username
        return '<script>window.location.href="/";</script>'
    return '<script>alert("登录失败");window.location.href="/login";</script>'

@app.route('/api/auth/login', methods=['POST'])
def user_login():
    """用户登录"""
    if request.is_json:
        data = request.get_json()
    else:
        data = request.form.to_dict() if request.form else {}
    
    username = data.get('username', '').strip() if data.get('username') else ''
    password = data.get('password', '')

    user_id = db.verify_user(username, password)
    if user_id:
        session['user_id'] = user_id
        session['username'] = username
        return jsonify({'code': 0, 'msg': '登录成功', 'username': username})
    return jsonify({'code': 1, 'msg': '用户名或密码错误'})

@app.route('/api/auth/logout', methods=['POST'])
def user_logout():
    """用户登出"""
    session.clear()
    return jsonify({'code': 0, 'msg': '已登出'})

@app.route('/api/auth/check', methods=['GET'])
def check_auth():
    """检查登录状态"""
    user_id = session.get('user_id')
    if user_id:
        return jsonify({'code': 0, 'logged_in': True, 'username': session.get('username')})
    return jsonify({'code': 0, 'logged_in': False})

# ==========================================================
# 路由：主页
# ==========================================================
@app.route('/login')
def login_page():
    if session.get('user_id'):
        return render_template('index.html')
    return render_template('login.html')

@app.route('/')
def index():
    if not session.get('user_id'):
        return render_template('login.html')
    return render_template('index.html')

@app.route('/sector')
def sector_page():
    if not session.get('user_id'):
        return render_template('login.html')
    return render_template('sector.html')

@app.route('/fund/<fund_code>')
def fund_detail_page(fund_code):
    """基金详情页"""
    if not session.get('user_id'):
        return render_template('login.html')
    return render_template('fund_detail.html', fund_code=fund_code)

@app.route('/api/fund/<fund_code>/detail')
@login_required
def get_fund_detail(fund_code):
    """获取基金详情"""
    try:
        # 检查是否交易时间
        current_weekday = datetime.now().weekday()
        current_hour = datetime.now().hour
        is_trading_time = current_weekday < 5 and 9 <= current_hour < 15
        
        # 获取实时估算
        estimate = fund_api.get_realtime_estimate(fund_code)
        
        # 获取历史净值（400天）
        nav_history = fund_api.get_nav_history(fund_code)
        
        # 非交易时间：使用历史净值作为当前净值
        if not is_trading_time and nav_history:
            current_nav = nav_history[0]['nav']
            current_nav_date = nav_history[0]['date']
            change_pct = 0
        else:
            current_nav = estimate.get('nav', 0)
            current_nav_date = estimate.get('gztime', '')
            change_pct = estimate.get('change_pct', 0)
        
        # 从API获取各周期收益率
        fund_info = fund_api.get_fund_detail_info(fund_code)
        week_pct = fund_info.get('week_pct')
        month_pct = fund_info.get('month_pct')
        month3_pct = fund_info.get('month3_pct')
        month6_pct = fund_info.get('month6_pct')
        year_pct = fund_info.get('year_pct')
        year3_pct = fund_info.get('year3_pct')
        inception_pct = fund_info.get('inception_pct')
        ytd_pct = fund_info.get('ytd_pct')
        
        # 获取基金名称
        fund_name = fund_info.get('name', '')
        
        # 检查用户是否持有
        user_id = get_current_user_id()
        funds = db.get_funds_by_user(user_id)
        in_hold = False
        hold_info = {}
        total_hold_amount = sum(f['hold_amount'] for f in funds)
        for f in funds:
            if f['fund_code'] == fund_code:
                in_hold = True
                hold_info = f
                break
        
        result = {
            'fund_code': fund_code,
            'fund_name': fund_name,
            'nav': round(current_nav, 4),
            'nav_date': current_nav_date,
            'change_pct': round(change_pct, 2),
            'daily_pct': round(change_pct, 2),
            'week_pct': week_pct,
            'month_pct': month_pct,
            'month3_pct': month3_pct,
            'month6_pct': month6_pct,
            'year_pct': year_pct,
            'year3_pct': year3_pct,
            'inception_pct': inception_pct,
            'ytd_pct': ytd_pct,
            'nav_history': nav_history,
            'in_hold': in_hold,
            'hold_amount': hold_info.get('hold_amount', 0),
            'hold_shares': hold_info.get('hold_shares', 0),
            'total_hold_income': hold_info.get('hold_income', 0),
            'total_hold_pct': 0,
            'daily_income': 0,
            'position_ratio': 0
        }
        
        if in_hold and hold_info.get('hold_shares', 0) > 0:
            cost = hold_info['hold_amount'] - hold_info['hold_income']
            if cost > 0:
                result['total_hold_pct'] = round((hold_info['hold_income'] / cost) * 100, 2)
            if total_hold_amount > 0:
                result['position_ratio'] = round((hold_info['hold_amount'] / total_hold_amount) * 100, 2)
            result['daily_income'] = 0
            result['daily_pct'] = round(change_pct, 2)
        
        return jsonify({'code': 0, 'data': result})
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e)})

# ==========================================================
# 基金实时数据
# ==========================================================
@app.route('/api/funds/realtime')
@login_required
def get_funds_realtime():
    """获取基金实际净值和收益"""
    try:
        user_id = get_current_user_id()

        db.process_pending_trades(user_id)

        funds = db.get_funds_by_user(user_id)
        if not funds:
            return jsonify({'code': 0, 'msg': 'success', 'data': [], 'summary': {}})

        result = []
        total_assets = 0
        total_daily_income = 0
        total_hold_amount = 0

        # 第一遍：计算总持有金额
        for fund in funds:
            total_hold_amount += fund['hold_amount']

        # 检查是否交易时间
        current_weekday = datetime.now().weekday()
        current_hour = datetime.now().hour
        is_trading_time = current_weekday < 5 and 9 <= current_hour < 15

        # 批量获取实时估算数据
        fund_codes = [f['fund_code'] for f in funds]
        estimates = fund_api.get_realtime_estimates(fund_codes)
        
        # 非交易时间：批量获取官方净值
        official_navs = {}
        if not is_trading_time:
            official_navs = fund_api.get_latest_navs_from_history(fund_codes)
        
        # 第二遍：计算每只基金的详细数据
        for fund in funds:
            fund_code = fund['fund_code']
            hold_amount = fund['hold_amount']
            hold_income = fund['hold_income']
            yesterday_nav = fund.get('yesterday_nav', 0)
            yesterday_nav_date = fund.get('base_nav_date', '')
            hold_shares = fund.get('hold_shares', 0)

            # 使用批量获取的估算数据
            estimate = estimates.get(fund_code, {})
            current_nav = estimate.get('nav', 0)
            change_pct = estimate.get('change_pct', 0)
            
            # 非交易时间：使用批量获取的官方净值
            nav = 0
            nav_date = ''
            if not is_trading_time:
                official = official_navs.get(fund_code, {})
                nav = official.get('nav', 0)
                nav_date = official.get('date', '')

            # 成本
            cost = hold_amount - hold_income

            # 计算
            if yesterday_nav > 0 and hold_shares > 0:
                yesterday_market_value = hold_shares * yesterday_nav
                base_hold_income = hold_income
                base_hold_pct = (base_hold_income / cost * 100) if cost > 0 else 0
                
                # 检查是否周末或非交易时间
                current_weekday = datetime.now().weekday()
                current_hour = datetime.now().hour
                
                print(f"DEBUG {fund_code}: weekday={current_weekday}, hour={current_hour}, nav={nav}, current_nav={current_nav}")
                
                                # 周末或非交易时间：用最新净值
                if current_weekday >= 5 or current_hour < 9 or current_hour >= 15:
                    if current_weekday >= 5:
                        print(f"  -> weekend branch")
                    else:
                        print(f"  -> non-trading hours branch")
                    
                    # 检查官方净值日期
                    use_estimate = False
                    today_str = datetime.now().strftime('%Y-%m-%d')
                    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

                    if nav_date:
                        try:
                            nav_dt = datetime.strptime(nav_date, '%Y-%m-%d')
                            # 如果官方净值是今天或昨天，计算真实的change_pct
                            if nav_date == today_str and yesterday_nav > 0:
                                change_pct = ((nav - yesterday_nav) / yesterday_nav) * 100
                                current_nav = nav
                                print(f"  -> 使用今日官方净值 {nav}, change_pct = {change_pct:.2f}%")
                                # 更新基准：今天净值作为明天的昨日净值
                                db.update_fund_base_nav(fund['id'], nav, nav_date, nav, current_income)
                                print(f"  -> 更新基准: yesterday_nav={nav}, yesterday_income={current_income}")
                            elif nav_date == yesterday:
                                current_nav = nav
                                change_pct = 0
                            else:
                                use_estimate = True
                                print(f"  -> nav date {nav_date} is old, using estimate")
                        except:
                            use_estimate = True
                            pass

                    # 使用估算数据
                    if use_estimate and estimate.get('nav', 0) > 0:
                        current_nav = estimate.get('nav', 0)
                        change_pct = estimate.get('change_pct', 0)
                    elif not nav_date:
                        # 没有官方净值，使用估算
                        current_nav = estimate.get('nav', 0)
                        change_pct = estimate.get('change_pct', 0)
                    
                    if current_nav > 0:
                        current_market_value = hold_shares * current_nav
                        current_income = current_market_value - cost
                        total_hold_pct = (current_income / cost * 100) if cost > 0 else 0
                        if change_pct != 0:
                            daily_income = yesterday_market_value * (change_pct / 100)
                            daily_pct = change_pct
                        else:
                            daily_income = 0
                            daily_pct = 0
                    else:
                        current_market_value = hold_shares * yesterday_nav
                        current_income = hold_income
                        total_hold_pct = base_hold_pct
                        daily_income = 0
                        daily_pct = 0
                # 交易时间：基准+当日变化
                elif current_nav > 0:
                    total_hold_pct = base_hold_pct + change_pct
                    daily_income = yesterday_market_value * (change_pct / 100)
                    current_income = base_hold_income + daily_income
                    current_market_value = yesterday_market_value + daily_income
                    daily_pct = change_pct
                else:
                    current_market_value = hold_shares * nav if nav > 0 else hold_shares * yesterday_nav
                    current_income = current_market_value - cost
                    total_hold_pct = (current_income / cost * 100) if cost > 0 else 0
                    daily_income = 0
                    daily_pct = 0
            else:
                current_market_value = hold_amount
                current_income = hold_income
                daily_income = 0
                daily_pct = 0
                total_hold_pct = (hold_income / cost * 100) if cost > 0 else 0

            # 持仓占比
            position_ratio = round((hold_amount / total_hold_amount) * 100, 2) if total_hold_amount > 0 else 0

            total_assets += current_market_value
            total_daily_income += daily_income

            result.append({
                'id': fund['id'],
                'fund_code': fund_code,
                'fund_name': fund['fund_name'],
                'hold_amount': hold_amount,
                'hold_income': round(current_income, 2),
                'nav': round(current_nav, 4),
                'nav_date': nav_date,
                'change_pct': round(change_pct, 2),
                'daily_income': round(daily_income, 2),
                'daily_pct': round(daily_pct, 2),
                'total_hold_income': round(current_income, 2),
                'total_hold_pct': round(total_hold_pct, 2),
                'position_ratio': position_ratio,
                'hold_shares': round(hold_shares, 2),
                'cost_price': round((cost / hold_shares), 4) if hold_shares > 0 else 0
            })

        total_hold_income_sum = sum(r['total_hold_income'] for r in result)
        total_cost = total_hold_amount - total_hold_income_sum
        total_hold_pct_sum = (total_hold_income_sum / total_cost * 100) if total_cost > 0 else 0

        summary = {
            'total_assets': round(total_assets, 2),
            'total_hold_amount': round(total_hold_amount, 2),
            'total_hold_income': round(total_hold_income_sum, 2),
            'total_hold_pct': round(total_hold_pct_sum, 2),
            'total_daily_income': round(total_daily_income, 2),
            'total_daily_pct': round((total_daily_income / (total_hold_amount - total_hold_income_sum) * 100), 2) if total_hold_amount - total_hold_income_sum > 0 else 0,
            'fund_count': len(funds),
            'update_time': last_update_time if last_update_time else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        return jsonify({'code': 0, 'msg': 'success', 'data': result, 'summary': summary})
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e), 'data': []})

# ==========================================================
# 基金搜索
# ==========================================================
@app.route('/api/funds/search')
@login_required
def search_funds():
    """搜索基金"""
    try:
        keyword = request.args.get('q', '')
        if not keyword:
            return jsonify({'code': 0, 'data': []})

        results = fund_api.search_funds(keyword)
        return jsonify({'code': 0, 'data': results})
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e), 'data': []})

# ==========================================================
# 基金持仓编辑
# ==========================================================
@app.route('/api/funds/<int:fund_id>', methods=['PUT'])
@login_required
def update_fund(fund_id):
    """更新基金持仓"""
    try:
        data = request.get_json()
        fund_code = data.get('fund_code', '').strip()
        fund_name = data.get('fund_name', '').strip()
        hold_amount = float(data.get('hold_amount', 0))
        hold_income = float(data.get('hold_income', 0))
        
        if not fund_code:
            return jsonify({'code': 1, 'msg': '基金代码不能为空'})
        
        db.update_fund(fund_id, fund_code, fund_name, hold_amount, hold_income)
        return jsonify({'code': 0, 'msg': 'success'})
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e)})

# ==========================================================
# 交易记录页面
# ==========================================================
@app.route('/trade-records')
@login_required
def trade_records():
    return render_template('trade_records.html')

@app.route('/api/trade-records', methods=['GET'])
@login_required
def get_trade_records():
    """获取交易记录"""
    try:
        user_id = get_current_user_id()
        buy_records = db.get_buy_records(user_id)
        sell_records = db.get_sell_records(user_id)

        result = []
        for r in buy_records:
            result.append({
                'id': r['id'],
                'trade_type': '加仓',
                'fund_code': r['fund_code'],
                'fund_name': r['fund_name'],
                'amount': r['buy_amount'],
                'created_at': r['created_at'],
                'buy_time': r['buy_time'],
                'effective_date': r['effective_date'],
                'status': r['status']
            })
        for r in sell_records:
            result.append({
                'id': r['id'],
                'trade_type': '减仓',
                'fund_code': r['fund_code'],
                'fund_name': r['fund_name'],
                'amount': r['sell_amount'],
                'created_at': r['created_at'],
                'sell_time': r['sell_time'],
                'effective_date': r['effective_date'],
                'status': r['status']
            })

        return jsonify({'code': 0, 'data': result})
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e)})

@app.route('/api/trade-records/cancel', methods=['POST'])
@login_required
def cancel_trade_record():
    """撤回交易记录"""
    try:
        data = request.get_json()
        record_id = data.get('record_id')
        trade_type = data.get('trade_type')

        if trade_type == '加仓':
            db.update_buy_record_status(record_id, 'cancelled')
        elif trade_type == '减仓':
            db.update_sell_record_status(record_id, 'cancelled')

        return jsonify({'code': 0, 'msg': '撤回成功'})
    except Exception as e:
        return jsonify({'code': 1, 'msg': str(e)})

# ==========================================================
# 管理后台
# ==========================================================
@app.route('/admin')
def admin_page():
    if session.get('is_admin'):
        return render_template('admin.html')
    return render_template('admin_login.html')

@app.route('/admin/login', methods=['POST'])
def admin_login():
    data = request.get_json()
    username = data.get('username', '')
    password = data.get('password', '')

    # 硬编码管理员账号
    if username == 'admin' and password == '613962':
        session['is_admin'] = True
        return jsonify({'code': 0, 'msg': '登录成功'})

    return jsonify({'code': 1, 'msg': '用户名或密码错误'})

@app.route('/admin/logout')
def admin_logout():
    session.pop('is_admin', None)
    return redirect('/admin')

@app.route('/admin/api/users', methods=['GET'])
def admin_get_users():
    if not session.get('is_admin'):
        return jsonify({'code': 1, 'msg': '未登录'})

    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT id, username, created_at FROM users ORDER BY id DESC')
    users = [dict(row) for row in cursor.fetchall()]
    return jsonify({'code': 0, 'data': users})

@app.route('/admin/api/users/<int:user_id>', methods=['DELETE'])
def admin_delete_user(user_id):
    if not session.get('is_admin'):
        return jsonify({'code': 1, 'msg': '未登录'})

    # 不允许删除admin用户
    cursor = conn.cursor()
    cursor.execute('SELECT username FROM users WHERE id = ?', (user_id,))
    user = cursor.fetchone()
    if user and user[0] == 'admin':
        return jsonify({'code': 1, 'msg': '不能删除管理员账号'})

    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM funds WHERE user_id = ?', (user_id,))
    cursor.execute('DELETE FROM users WHERE id = ?', (user_id,))
    conn.commit()
    return jsonify({'code': 0, 'msg': '删除成功'})

@app.route('/api/settings', methods=['GET'])
def get_settings():
    """获取网站设置"""
    global site_name_cache
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT value FROM settings WHERE key = ?', ('site_name',))
    row = cursor.fetchone()
    site_name = row[0] if row else '基金持仓管理系统'
    site_name_cache = site_name
    return jsonify({'code': 0, 'data': {'site_name': site_name}})

@app.route('/api/sector/flow')
@login_required
def get_sector_flow():
    """获取板块资金流数据"""
    try:
        period = request.args.get('period', '10日')

        from sector_api import SectorAPI
        sector_api = SectorAPI()

        # 将 period 转换为数字
        period_map = {'5日': '5', '10日': '10', '20日': '20', '1日': '1'}
        period_num = period_map.get(period, '10')

        # 如果是1日，使用mock数据
        if period_num == '1':
            from sector_api import MOCK_DATA
            mock = MOCK_DATA.get('1', MOCK_DATA['1'])
            return jsonify({'code': 0, 'data': {
                'inflow_top10': mock['inflow'][:10],
                'outflow_top10': mock['outflow'][:10]
            }})

        data = sector_api.get_top_flow_sectors(period=period_num, limit=10)
        return jsonify({'code': 0, 'data': data})
    except Exception as e:
        print(f"获取板块资金流失败: {e}")
        return jsonify({'code': 1, 'msg': str(e)})

@app.route('/admin/api/settings', methods=['GET', 'PUT'])
def admin_settings():
    if not session.get('is_admin'):
        return jsonify({'code': 1, 'msg': '未登录'})

    conn = db.get_connection()
    cursor = conn.cursor()

    if request.method == 'GET':
        cursor.execute('SELECT key, value FROM settings WHERE key = ?', ('site_name',))
        row = cursor.fetchone()
        site_name = row[1] if row else '基金持仓管理系统'
        return jsonify({'code': 0, 'data': {'site_name': site_name}})

    # PUT
    data = request.get_json()
    site_name = data.get('site_name', '')

    cursor.execute('SELECT id FROM settings WHERE key = ?', ('site_name',))
    if cursor.fetchone():
        cursor.execute('UPDATE settings SET value = ? WHERE key = ?', (site_name, 'site_name'))
    else:
        cursor.execute('INSERT INTO settings (key, value) VALUES (?, ?)', ('site_name', site_name))
    conn.commit()

    # 更新全局缓存
    global site_name_cache
    site_name_cache = site_name

    return jsonify({'code': 0, 'msg': '保存成功'})

# ==========================================================
# 主程序入口
# ==========================================================
if __name__ == '__main__':
    print("=" * 50)
    print("基金持仓管理系统启动")
    print("访问地址: http://127.0.0.1:5000")
    print("=" * 50)
    
    # 初始化数据库
    db.init_db()
    
    # 启动Flask应用
    app.run(debug=False, host='127.0.0.1', port=5000)
