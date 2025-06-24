import requests
import pymysql
import pandas as pd
from datetime import datetime

# 数据库配置
MYSQL_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASSWORD = '123456'
MYSQL_DB = 'stock_data'
MYSQL_CHARSET = 'utf8mb4'

# 创建数据库和表（如不存在）
def init_db():
    conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, charset=MYSQL_CHARSET)
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS stock_data DEFAULT CHARSET utf8mb4;")
    conn.select_db(MYSQL_DB)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_stock (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(20),
            name VARCHAR(50),
            price FLOAT,
            date DATE
        );
    ''')
    conn.commit()
    cursor.close()
    conn.close()

# 示例：爬取新浪财经A股部分数据（可根据实际需求更换数据源）
def fetch_stock_data():
    url = "https://hq.sinajs.cn/list=sh600000,sz000001"
    headers = {
        'Referer': 'https://finance.sina.com.cn',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    response.encoding = 'gbk'
    lines = response.text.strip().split('\n')
    data = []
    for line in lines:
        parts = line.split('"')
        if len(parts) < 2:
            continue
        info = parts[1].split(',')
        if len(info) < 4:
            continue
        code = line.split('=')[0].split('_')[-1]
        name = info[0]
        price = float(info[3]) if info[3] else 0.0
        data.append({'code': code, 'name': name, 'price': price, 'date': datetime.now().date()})
    return pd.DataFrame(data)

# 保存数据到 MySQL
def save_to_mysql(df):
    conn = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset=MYSQL_CHARSET
    )
    cursor = conn.cursor()
    for _, row in df.iterrows():
        sql = "INSERT INTO daily_stock (code, name, price, date) VALUES (%s, %s, %s, %s)"
        cursor.execute(sql, (row['code'], row['name'], row['price'], row['date']))
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    try:
        init_db()
        df = fetch_stock_data()
        if not df.empty:
            save_to_mysql(df)
            print("数据已保存到MySQL。")
        else:
            print("未获取到股票数据。")
    except Exception as e:
        print(f"发生错误: {e}")
