"""
=========================================================
基金API模块 - 天天基金免费接口
=========================================================
功能：
1. 获取基金基本信息
2. 获取基金实时估算净值（交易日盘中）
3. 获取基金官方净值（盘后）

接口说明：
- 天天基金提供免费API，无需Token
- 估算净值数据在交易时间段内更新
- 官方净值在盘后15:00后更新
"""

import requests
import time
import json
import re
from datetime import datetime

class FundAPI:
    """基金API操作类"""

    def __init__(self):
        """初始化API配置"""
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://fund.eastmoney.com/'
        }
        self.base_url = 'https://fund.eastmoney.com'
        self.cache = {}  # 缓存估算净值
        self.last_fetch = {}  # 上次获取时间
        self.history_cache = {}  # 缓存历史净值
        self.history_cache_time = {}  # 缓存时间

    def get_fund_info(self, fund_code):
        """
        获取基金基本信息

        参数:
            fund_code: 基金代码（如：161039）

        返回:
            dict: 基金信息字典
        """
        try:
            # 使用天天基金实时API (fundgz API)
            url = f'http://fundgz.1234567.com.cn/js/{fund_code}.js'
            
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            # 解析JSONP数据: jsonpgz({...})
            content = response.text
            import re
            match = re.search(r'jsonpgz\((.*)\)', content)
            if not match:
                return None
            
            data = json.loads(match.group(1))
            
            return {
                'name': data.get('name', ''),
                'fund_code': data.get('fundcode', ''),
                'dwjz': data.get('dwjz', 0),  # 单位净值
                'gsz': data.get('gsz', 0),    # 估算净值
                'gszzl': data.get('gszzl', 0),  # 估算涨跌幅
                'gztime': data.get('gztime', '')  # 更新时间
            }
        except Exception as e:
            print(f"获取基金信息失败: {fund_code}, 错误: {e}")
            return None

    def get_realtime_estimate(self, fund_code):
        """
        获取单只基金实时估算净值（交易日盘中）

        参数:
            fund_code: 基金代码

        返回:
            dict: 估算净值信息
        """
        try:
            # 使用天天基金实时API
            url = f'http://fundgz.1234567.com.cn/js/{fund_code}.js'
            response = requests.get(url, headers=self.headers, timeout=10)

            if response.status_code != 200:
                return {'nav': 0, 'change_pct': 0, 'net_value': 0}

            # 解析JSONP数据
            import re
            match = re.search(r'jsonpgz\((.*)\)', response.text)
            if not match:
                return {'nav': 0, 'change_pct': 0, 'net_value': 0}
            
            data = json.loads(match.group(1))
            
            return {
                'nav': float(data.get('gsz', 0)),  # 估算净值
                'change_pct': float(data.get('gszzl', 0)),  # 估算涨跌幅
                'net_value': float(data.get('dwjz', 0)),  # 官方净值
                'gztime': data.get('gztime', '')  # 更新时间
            }
        except Exception as e:
            print(f"获取估算净值失败: {fund_code}, 错误: {e}")
            return {'nav': 0, 'change_pct': 0, 'net_value': 0}

    def get_realtime_estimates(self, fund_codes=None):
        """
        批量获取基金实时估算净值

        参数:
            fund_codes: 基金代码列表，如果为None则不获取

        返回:
            dict: 基金代码 -> 估算净值信息
        """
        result = {}

        if not fund_codes:
            return result

        for fund_code in fund_codes:
            result[fund_code] = self.get_realtime_estimate(fund_code)
            time.sleep(0.05)  # 减少延迟

        return result

    def get_latest_navs_from_history(self, fund_codes):
        """
        批量从历史净值数据中获取最新净值（带缓存）
        缓存1小时
        """
        from datetime import datetime, timedelta
        import time
        
        result = {}
        now = time.time()
        cache_duration = 3600  # 缓存1小时
        
        # 检查是否有缓存且未过期
        need_fetch = []
        for fund_code in fund_codes:
            if fund_code in self.history_cache:
                cache_time = self.history_cache_time.get(fund_code, 0)
                if now - cache_time < cache_duration:
                    result[fund_code] = self.history_cache[fund_code]
                else:
                    need_fetch.append(fund_code)
            else:
                need_fetch.append(fund_code)
        
        if not need_fetch:
            return result  # 全部命中缓存
        
        today = datetime.now()
        end_date = today.strftime('%Y-%m-%d')
        start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
        
        for fund_code in need_fetch:
            try:
                url = f'https://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={fund_code}&page=1&per=5&sdate={start_date}&edate={end_date}'
                response = requests.get(url, headers=self.headers, timeout=5)
                
                if response.status_code == 200:
                    import re
                    nav_matches = re.findall(r'(\d{4}-\d{2}-\d{2})</td><td[^>]*>([\d.]+)</td>', response.text)
                    if nav_matches:
                        latest_date, latest_nav = nav_matches[0]
                        nav_data = {'nav': float(latest_nav), 'date': latest_date}
                        result[fund_code] = nav_data
                        self.history_cache[fund_code] = nav_data
                        self.history_cache_time[fund_code] = now
                        continue
            except:
                pass
            result[fund_code] = {'nav': 0, 'date': ''}
        
        return result
        
        return result

    def get_official_nav(self, fund_code, date_str=None):
        """
        获取基金官方净值（盘后）
        尝试多个数据源获取最新净值

        参数:
            fund_code: 基金代码
            date_str: 查询日期，格式YYYY-MM-DD，默认昨天

        返回:
            dict: 官方净值信息
        """
        # 尝试从历史净值获取最新数据
        history_nav = self.get_latest_nav_from_history(fund_code)
        if history_nav.get('nav', 0) > 0:
            return {'nav': history_nav['nav'], 'acc_nav': 0, 'date': history_nav['date']}
        
        # 备用：使用pingzhong API
        try:
            url = f'https://fund.eastmoney.com/pingzhong/data/{fund_code}.js'
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                import re
                gsz_match = re.search(r'"gsz":"([^"]+)"', response.text)
                dwjz_match = re.search(r'"dwjz":"([^"]+)"', response.text)
                jzrq_match = re.search(r'"jzrq":"([^"]+)"', response.text)
                
                gsz = gsz_match.group(1) if gsz_match else None
                dwjz = dwjz_match.group(1) if dwjz_match else None
                jzrq = jzrq_match.group(1) if jzrq_match else ''
                
                nav = float(dwjz) if dwjz else 0
                if nav == 0 and gsz:
                    nav = float(gsz)
                
                if nav > 0:
                    return {'nav': nav, 'acc_nav': 0, 'date': jzrq}
        except:
            pass
        
        # 备用：使用 fundgz API
        try:
            url = f'http://fundgz.1234567.com.cn/js/{fund_code}.js'
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code != 200:
                return {'nav': 0, 'acc_nav': 0, 'date': ''}
            
            import re
            match = re.search(r'jsonpgz\((.*)\)', response.text)
            if not match:
                return {'nav': 0, 'acc_nav': 0, 'date': ''}
            
            data = json.loads(match.group(1))
            
            dwjz = data.get('dwjz', '0')
            gsz = data.get('gsz', '0')
            jzrq = data.get('jzrq', '')
            
            nav = float(dwjz) if dwjz else 0
            if nav == 0 and gsz:
                nav = float(gsz)
            
            return {'nav': nav, 'acc_nav': 0, 'date': jzrq}
        except Exception as e:
            print(f"获取官方净值失败: {fund_code}, 错误: {e}")
            return {'nav': 0, 'acc_nav': 0, 'date': ''}

    def get_fund_position(self, fund_code):
        """
        获取基金持仓信息（关联板块）

        参数:
            fund_code: 基金代码

        返回:
            dict: 持仓信息
        """
        try:
            url = f'https://fund.eastmoney.com/pingzhong/data/{fund_code}.js'
            response = requests.get(url, headers=self.headers, timeout=10)

            content = response.text

            # 提取持仓股票信息
            if 'zcp' in content:
                start = content.find('zcp=')
                if start != -1:
                    json_start = content.find('[', start)
                    json_end = content.find('];', json_start) + 1
                    if json_start != -1 and json_end != 0:
                        json_str = content[json_start:json_end]
                        positions = json.loads(json_str)

                        return {
                            'positions': [
                                {
                                    'stock_code': p.get('symbol', ''),
                                    'stock_name': p.get('name', ''),
                                    'proportion': p.get('proportion', 0)  # 持仓占比
                                }
                                for p in positions[:10]  # 取前10大持仓
                            ]
                        }

            return {'positions': []}
        except Exception as e:
            print(f"获取基金持仓失败: {fund_code}, 错误: {e}")
            return {'positions': []}

    def get_latest_nav_from_history(self, fund_code):
        """
        从历史净值数据中获取最新净值（用于周末/非交易时间）
        使用F10DataApi获取最近几天的历史净值
        """
        try:
            from datetime import datetime, timedelta
            today = datetime.now()
            end_date = today.strftime('%Y-%m-%d')
            start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
            
            url = f'https://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={fund_code}&page=1&per=5&sdate={start_date}&edate={end_date}'
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code != 200:
                return {'nav': 0, 'date': ''}
            
            import re
            text = response.text
            # 查找日期和净值 - 匹配 "日期" 后紧跟 "净值" 的模式
            # 使用更宽松的正则匹配
            nav_matches = re.findall(r'(\d{4}-\d{2}-\d{2})</td><td[^>]*>([\d.]+)</td>', text)
            
            if nav_matches:
                latest_date, latest_nav = nav_matches[0]
                return {'nav': float(latest_nav), 'date': latest_date}
            
            return {'nav': 0, 'date': ''}
        except Exception as e:
            print(f"获取历史净值失败: {fund_code}, 错误: {e}")
            return {'nav': 0, 'date': ''}

    def get_nav_history(self, fund_code, days=400):
        """获取基金净值历史"""
        try:
            from datetime import datetime, timedelta
            today = datetime.now()
            end_date = today.strftime('%Y-%m-%d')
            start_date = (today - timedelta(days=days)).strftime('%Y-%m-%d')
            
            url = f'https://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={fund_code}&page=1&per=120&sdate={start_date}&edate={end_date}'
            response = requests.get(url, headers=self.headers, timeout=15)
            
            if response.status_code != 200:
                return []
            
            import re
            nav_matches = re.findall(r'(\d{4}-\d{2}-\d{2})</td><td[^>]*>([\d.]+)</td><td[^>]*>([\d.]+)</td><td[^>]*>([+-]?[\d.]+)%', response.text)
            
            result = []
            for date, nav, acc_nav, change_pct in nav_matches:
                result.append({
                    'date': date,
                    'nav': float(nav),
                    'acc_nav': float(acc_nav),
                    'change_pct': float(change_pct)
                })
            
            return result
        except Exception as e:
            print(f"获取净值历史失败: {fund_code}, 错误: {e}")
            return []

    def get_fund_stage_rates(self, fund_code):
        """从pingzhongdata获取阶段收益率"""
        try:
            url = f'https://fund.eastmoney.com/pingzhongdata/{fund_code}.js'
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code != 200:
                return {}
            
            text = response.text
            
            # 从变量获取近一月、三月、半年、一年
            month_pct = re.search(r'var\s+syl_1y\s*=\s*"([^"]+)"', text)
            month3_pct = re.search(r'var\s+syl_3y\s*=\s*"([^"]+)"', text)
            month6_pct = re.search(r'var\s+syl_6y\s*=\s*"([^"]+)"', text)
            year_pct = re.search(r'var\s+syl_1n\s*=\s*"([^"]+)"', text)
            
            # 从Data_netWorthTrend计算近一周涨幅
            week_pct = None
            networth_match = re.search(r'var\s+Data_netWorthTrend\s*=\s*(\[.*?\]);', text, re.DOTALL)
            if networth_match:
                try:
                    data = json.loads(networth_match.group(1))
                    if len(data) >= 5:
                        latest = data[-1]['y']
                        # 从后往前找5个有效交易日，取第4个（跳过最新日和周末）
                        trading_days = []
                        for d in reversed(data):
                            if d['y'] > 0:
                                trading_days.append(d['y'])
                                if len(trading_days) >= 5:
                                    break
                        
                        if len(trading_days) >= 4:
                            week_ago = trading_days[3]
                            if week_ago > 0:
                                week_pct = round(((latest / week_ago) - 1) * 100, 2)
                except Exception as e:
                    pass
            
            # 从Data_netWorthTrend计算成立以来涨幅（第一天的净值）
            inception_pct = None
            if networth_match:
                try:
                    data = json.loads(networth_match.group(1))
                    if len(data) > 0:
                        first_nav = data[0]['y']
                        latest = data[-1]['y']
                        if first_nav > 0:
                            inception_pct = round(((latest - first_nav) / first_nav) * 100, 2)
                except:
                    pass
            
            # 从Data_netWorthTrend计算今年以来涨幅
            ytd_pct = None
            year3_pct = None
            if networth_match:
                try:
                    from datetime import datetime, timedelta
                    data = json.loads(networth_match.group(1))
                    current_year = datetime.now().year

                    # YTD计算 - 使用去年最后一个交易日(12月31日或之前)
                    year_end_nav = None
                    for d in reversed(data):
                        if 'x' in d:
                            dt = datetime.fromtimestamp(d['x'] / 1000)
                            if dt.year == current_year - 1:
                                if d['y'] > 0:
                                    year_end_nav = d['y']
                                    break
                    if year_end_nav and latest > 0:
                        ytd_pct = round(((latest / year_end_nav) - 1) * 100, 2)

                    # 近三年计算 - 找到3年前最接近的交易日（允许偏差几天）
                    target_date = datetime(current_year, datetime.now().month, datetime.now().day) - timedelta(days=3*365)
                    three_years_ago_nav = None
                    min_diff = 365  # 扩大搜索范围到1年内
                    for d in data:
                        if 'x' in d and d['y'] > 0:
                            dt = datetime.fromtimestamp(d['x'] / 1000)
                            diff = (target_date - dt).days  # 只找之前的日期
                            if 0 < diff < min_diff:
                                min_diff = diff
                                three_years_ago_nav = d['y']
                    if three_years_ago_nav and latest > 0:
                        year3_pct = round(((latest / three_years_ago_nav) - 1) * 100, 2)
                except Exception:
                    pass

            # 优先使用API提供的三年数据，否则使用计算值
            year3_pct_api = re.search(r'var\s+syl_3n\s*=\s*"([^"]+)"', text)
            final_year3_pct = float(year3_pct_api.group(1)) if year3_pct_api else year3_pct
            
            return {
                'week_pct': week_pct,
                'month_pct': float(month_pct.group(1)) if month_pct else None,
                'month3_pct': float(month3_pct.group(1)) if month3_pct else None,
                'month6_pct': float(month6_pct.group(1)) if month6_pct else None,
                'year_pct': float(year_pct.group(1)) if year_pct else None,
                'year3_pct': final_year3_pct,
                'inception_pct': inception_pct,
                'ytd_pct': ytd_pct
            }
        except Exception as e:
            print(f"获取阶段收益率失败: {e}")
            return {}

    def get_fund_detail_info(self, fund_code):
        """获取基金详细信息（含各周期涨幅）"""
        try:
            # pingzhongdata API
            url = f'https://fund.eastmoney.com/pingzhongdata/{fund_code}.js'
            response = requests.get(url, headers=self.headers, timeout=10)
            
            text = response.text
            
            # 提取基金名称
            name_match = re.search(r'var\s+fS_name\s*=\s*"([^"]+)"', text)
            name = name_match.group(1) if name_match else ''
            
            # 提取各周期收益率
            def extract_pct(pattern, text):
                match = re.search(pattern, text)
                if match:
                    val = match.group(1)
                    return float(val) if val and val != '-' else 0
                return 0
            
            month_pct = extract_pct(r'var\s+syl_1y\s*=\s*"([^"]+)"', text)
            month3_pct = extract_pct(r'var\s+syl_3y\s*=\s*"([^"]+)"', text)
            month6_pct = extract_pct(r'var\s+syl_6y\s*=\s*"([^"]+)"', text)
            year_pct = extract_pct(r'var\s+syl_1n\s*=\s*"([^"]+)"', text)
            
            # 从 fundapi 获取近一周和成立以来数据
            stage_data = self.get_fund_stage_rates(fund_code)
            
            return {
                'name': name,
                'week_pct': stage_data.get('week_pct'),
                'month_pct': month_pct or stage_data.get('month_pct'),
                'month3_pct': month3_pct or stage_data.get('month3_pct'),
                'month6_pct': month6_pct or stage_data.get('month6_pct'),
                'year_pct': year_pct or stage_data.get('year_pct'),
                'year3_pct': stage_data.get('year3_pct'),
                'inception_pct': stage_data.get('inception_pct'),
                'ytd_pct': stage_data.get('ytd_pct')
            }
        except Exception as e:
            print(f"获取基金详情失败: {fund_code}, 错误: {e}")
            return {}

    def search_funds(self, keyword):
        """搜索基金 - 通过fund code或名称查询"""
        try:
            import re
            import urllib.parse

            keyword = keyword.strip()

            # 如果是6位数字代码，直接验证并返回
            if keyword.isdigit() and len(keyword) == 6:
                url = f'https://fund.eastmoney.com/pingzhongdata/{keyword}.js'
                response = requests.get(url, headers=self.headers, timeout=10)
                if response.status_code == 200:
                    match = re.search(r'var\s+fS_name\s*=\s*"([^"]+)"', response.text)
                    if match:
                        return [{'fund_code': keyword, 'fund_name': match.group(1)}]

            # 尝试搜索接口
            keyword_encoded = urllib.parse.quote(keyword)
            url = f'https://fund.eastmoney.com/data/FundSearchDataAPI.html?m=0&n=20&keywords={keyword_encoded}'

            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code != 200:
                return []

            response.encoding = 'utf-8'
            text = response.text

            # 尝试解析JSON
            try:
                data = json.loads(text)
                if 'Data' in data:
                    results = []
                    for item in data.get('Data', []):
                        results.append({
                            'fund_code': item.get('Cd', ''),
                            'fund_name': item.get('Nm', '')
                        })
                    return results
            except:
                pass

            # 如果搜索接口失败，尝试从基金列表页面获取
            url = 'https://fund.eastmoney.com/data/fundranking.html'
            response = requests.get(url, headers=self.headers, timeout=15)
            response.encoding = 'utf-8'

            pattern = r'"(\d{6})","([^"]+)"'
            matches = re.findall(pattern, response.text)

            keyword_lower = keyword.lower()
            results = []
            for code, name in matches:
                if keyword_lower in name.lower() or keyword_lower in code:
                    results.append({'fund_code': code, 'fund_name': name})
                    if len(results) >= 20:
                        break

            return results
        except Exception as e:
            print(f"搜索基金失败: {e}")
            return []