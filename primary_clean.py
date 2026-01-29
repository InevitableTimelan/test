"""
淘宝母婴数据清洗与分析脚本
作者: 安徽大学 23级互联网金融专业
日期: 2026年
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# 设置中文字体显示
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False
sns.set_style("whitegrid")

# ==================== 数据库连接部分 ====================
def create_db_connection():
    """创建数据库连接"""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="lanlala",  
            password="197312zjf.",  
            database="test"  
        )
        print("数据库连接成功!")
        return connection
    except Error as e:
        print(f"连接失败: {e}")
        return None

# ==================== 数据探索部分 ====================
def show_tables(connection):
    """显示数据库中的所有表"""
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    print("数据库中的表：")
    for table in tables:
        print(f"- {table[0]}")
    cursor.close()
    return tables

def describe_table(connection, table_name):
    """显示表结构"""
    cursor = connection.cursor()
    try:
        cursor.execute(f"DESCRIBE `{table_name}`")
        columns = cursor.fetchall()
        print(f"\n表 '{table_name}' 的结构：")
        print("=" * 60)
        for col in columns:
            print(f"字段名: {col[0]:20} 类型: {col[1]:20} 是否为空: {col[2]}")
        print("=" * 60)
    except Error as e:
        print(f"查看表结构失败：{e}")
    finally:
        cursor.close()

def load_data_to_dataframe(connection, table_name, limit=None):
    """从数据库表读取数据到Pandas DataFrame"""
    if limit:
        query = f"SELECT * FROM `{table_name}` LIMIT {limit}"
    else:
        query = f"SELECT * FROM `{table_name}`"
    
    try:
        df = pd.read_sql(query, connection)
        print(f"成功读取 {len(df)} 行数据")
        return df
    except Exception as e:
        print(f"pd.read_sql读取失败: {e}")
        print("尝试使用cursor.fetchall()方法...")
        
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            print(f"cursor方法成功读取 {len(df)} 行数据")
            return df
        except Exception as e2:
            print(f"两种读取方法都失败: {e2}")
            return None

# ==================== 数据质量检查 ====================
def check_data_quality(df):
    """数据质量检查报告"""
    print("=" * 60)
    print("数据质量检查报告")
    print("=" * 60)
    
    if df is None or len(df) == 0:
        print("数据为空，无法进行检查")
        return None
    
    # 1. 检查缺失值
    print("\n1. 缺失值统计:")
    missing_values = df.isnull().sum()
    missing_percent = (missing_values / len(df)) * 100
    
    missing_df = pd.DataFrame({
        '缺失数量': missing_values,
        '缺失百分比': missing_percent
    })
    
    if missing_df['缺失数量'].sum() == 0:
        print("没有缺失值!")
    else:
        print(missing_df[missing_df['缺失数量'] > 0])
    
    # 2. 检查重复行
    print(f"\n2. 重复行检查:")
    duplicates = df.duplicated().sum()
    print(f" 重复行数: {duplicates}")
    if duplicates == 0:
        print("没有完全重复的行!")
    
    # 3. 数据统计摘要（数值型）
    print("\n3. 数值型列统计摘要:")
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        print(df[numeric_cols].describe())
    else:
        print("没有数值型列")
    
    # 4. 类别型列唯一值数量
    print("\n4. 类别型列唯一值统计:")
    categorical_cols = df.select_dtypes(include=['object']).columns
    for col in categorical_cols:
        unique_count = df[col].nunique()
        print(f"   {col}: {unique_count} 个唯一值")
        if unique_count < 20 and unique_count > 0:
            print(f"     具体值: {df[col].unique()[:10]}")
    
    return missing_df

# ==================== 数据清洗函数 ====================
def clean_taobao_data(df, missing_info):
    """淘宝数据专项清洗函数 - 适配当前表结构"""
    print("=" * 60)
    print("开始专项数据清洗")
    print("=" * 60)
    
    if df is None or len(df) == 0:
        print("数据为空，无法清洗")
        return None
    
    df_clean = df.copy()
    original_shape = df_clean.shape
    changes_log = []
    
    # 1. 处理缺失值
    for column in df_clean.columns:
        missing_count = df_clean[column].isnull().sum()
        if missing_count > 0:
            missing_percent = (missing_count / len(df_clean)) * 100
            
            if missing_percent > 30:
                df_clean.drop(column, axis=1, inplace=True)
                changes_log.append(f"删除列 '{column}' (缺失率 {missing_percent:.1f}%)")
            elif missing_percent > 0:
                # 特殊处理user_id（文本型）
                if column == 'user_id':
                    if not df_clean[column].mode().empty:
                        fill_value = str(df_clean[column].mode()[0])
                    else:
                        fill_value = "unknown_user"
                    df_clean[column].fillna(fill_value, inplace=True)
                    changes_log.append(f"列 '{column}': 用 '{fill_value}' 填充 {missing_count} 个缺失值")
                # 数值型列
                elif df_clean[column].dtype in ['int64', 'float64', 'int32', 'float32', 'int']:
                    fill_value = df_clean[column].median()
                    df_clean[column].fillna(fill_value, inplace=True)
                    changes_log.append(f"列 '{column}': 用中位数 {fill_value} 填充 {missing_count} 个缺失值")
                # 其他文本型列
                else:
                    if not df_clean[column].mode().empty:
                        fill_value = df_clean[column].mode()[0]
                    else:
                        fill_value = "Unknown"
                    df_clean[column].fillna(fill_value, inplace=True)
                    changes_log.append(f"列 '{column}': 用 '{fill_value}' 填充 {missing_count} 个缺失值")
    
    
    # 2. 去除完全重复的行
    duplicates_before = df_clean.duplicated().sum()
    if duplicates_before > 0:
        df_clean.drop_duplicates(inplace=True)
        changes_log.append(f"删除 {duplicates_before} 个完全重复的行")
    
    # 3. 专项清洗：day列格式转换
    if 'day' in df_clean.columns:
        try:
            df_clean['day'] = pd.to_datetime(df_clean['day'].astype(str), format='%Y%m%d', errors='coerce')
            invalid_dates = df_clean['day'].isnull().sum()
            if invalid_dates > 0:
                changes_log.append(f"'day'列中有 {invalid_dates} 个无效日期，已设为NaT")
        except Exception as e:
            changes_log.append(f"'day'列转换失败: {e}")
    
    # 4. 专项清洗：buy_mount列合理性检查
    if 'buy_mount' in df_clean.columns:
        try:
            df_clean['buy_mount'] = pd.to_numeric(df_clean['buy_mount'], errors='coerce')
            invalid_buy = df_clean[df_clean['buy_mount'] <= 0].shape[0]
            if invalid_buy > 0:
                changes_log.append(f"'buy_mount'列中有 {invalid_buy} 个非正值（≤0）")
        except:
            pass
    
    # 5. 专项清洗：property列处理
    if 'property' in df_clean.columns:
        # 提取第一个属性键
        def extract_first_property(prop):
            if pd.isna(prop) or prop == "" or not isinstance(prop, str):
                return None
            if ":" in prop and ";" in prop:
                parts = str(prop).split(';')
                for part in parts:
                    if part and ':' in part:
                        return part.split(':')[0]
            return None
        
        df_clean['first_property_key'] = df_clean['property'].apply(extract_first_property)
        changes_log.append(f"从'property'列提取首属性键，共有 {df_clean['first_property_key'].nunique()} 个唯一键")
    
    # 6. 重置索引
    df_clean.reset_index(drop=True, inplace=True)
    
    # 7. 记录清洗结果
    print(f"\n专项清洗完成!")
    print(f"   原始数据形状: {original_shape}")
    print(f"   清洗后形状: {df_clean.shape}")
    print(f"   删除了 {original_shape[0] - df_clean.shape[0]} 行")
    print(f"   删除了 {original_shape[1] - df_clean.shape[1]} 列")
    
    if changes_log:
        print("\n清洗操作记录:")
        for log in changes_log:
            print(f"   • {log}")
    
    return df_clean, changes_log

# ==================== 生成清洗报告 ====================
def generate_cleaning_report(df_original, df_cleaned, changes_log, report_filename="data_cleaning_report.txt"):
    """生成数据清洗报告"""
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("淘宝母婴数据清洗报告\n")
        f.write("=" * 70 + "\n\n")
        
        f.write(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("1. 数据集概览\n")
        f.write("   " + "=" * 40 + "\n")
        f.write(f"   原始数据: {df_original.shape[0]} 行, {df_original.shape[1]} 列\n")
        f.write(f"   清洗后数据: {df_cleaned.shape[0]} 行, {df_cleaned.shape[1]} 列\n")
        f.write(f"   数据减少: {df_original.shape[0] - df_cleaned.shape[0]} 行 ({((df_original.shape[0] - df_cleaned.shape[0])/df_original.shape[0]*100):.1f}%)\n\n")
        
        f.write("2. 清洗前后字段对比\n")
        f.write("   " + "=" * 40 + "\n")
        f.write(f"   原始字段: {list(df_original.columns)}\n")
        f.write(f"   清洗后字段: {list(df_cleaned.columns)}\n")
        f.write(f"   新增字段: {[col for col in df_cleaned.columns if col not in df_original.columns]}\n\n")
        
        f.write("3. 数据清洗步骤\n")
        f.write("   " + "=" * 40 + "\n")
        for i, log in enumerate(changes_log, 1):
            f.write(f"   {i}. {log}\n")
        
        f.write("\n4. 清洗效果验证\n")
        f.write("   " + "=" * 40 + "\n")
        
        # 缺失值对比
        original_missing = df_original.isnull().sum().sum()
        cleaned_missing = df_cleaned.isnull().sum().sum()
        f.write(f"   缺失值处理: 从 {original_missing} 减少到 {cleaned_missing}\n")
        
        # 重复值对比
        original_duplicates = df_original.duplicated().sum()
        cleaned_duplicates = df_cleaned.duplicated().sum()
        f.write(f"   重复行处理: 从 {original_duplicates} 减少到 {cleaned_duplicates}\n")
        
        f.write("\n5. 建议\n")
        f.write("   " + "=" * 40 + "\n")
        f.write("   • 清洗后的数据已可用于进一步分析\n")
        f.write("   • 建议对first_property_key进行编码以便后续建模\n")
        f.write("   • 可对day列进行时间序列分析\n")
        f.write("   • 可对buy_mount进行分组分析\n")
    
    print(f"清洗报告已保存到: {report_filename}")
    return report_filename

# ==================== 保存清洗后数据 ====================
def save_cleaned_data(df_cleaned, conn, new_table_name="cleaned_taobao_data"):
    """将清洗后的数据保存回数据库"""
    try:
        from sqlalchemy import create_engine
        
        # 创建连接字符串
        engine = create_engine('mysql+mysqlconnector://lanlala:197312zjf.@localhost/test')
        
        # 保存到新表
        df_cleaned.to_sql(new_table_name, engine, if_exists='replace', index=False)
        print(f"清洗后数据已保存到新表 '{new_table_name}'")
        return True
    except ImportError:
        print("未安装sqlalchemy，无法保存到数据库")
        print("正在保存为CSV文件...")
        df_cleaned.to_csv("cleaned_taobao_data.csv", index=False, encoding='utf-8-sig')
        print("清洗后数据已保存为 'cleaned_taobao_data.csv'")
        return False
    except Exception as e:
        print(f"保存失败: {e}")
        return False

# ==================== 主程序 ====================
def main():
    """主程序入口"""
    print("开始淘宝母婴数据清洗项目")
    print("=" * 60)
    
    # 1. 建立数据库连接
    conn = create_db_connection()
    if conn is None:
        return
    
    try:
        # 2. 显示数据库中的表
        tables = show_tables(conn)
        
        if not tables:
            print(" 数据库中没有表")
            return
        
        # 3. 获取表名
        actual_table_name = tables[0][0]
        print(f"\n检测到的表名: {actual_table_name}")
        
        # 4. 显示表结构
        describe_table(conn, actual_table_name)
        
        # 5. 读取数据
        print("\n正在读取数据...")
        df = load_data_to_dataframe(conn, actual_table_name, limit=1000)  # 先读1000行测试
        
        if df is None or len(df) == 0:
            print("无法读取数据，请检查表是否存在且包含数据")
            return
        
        # 6. 显示数据预览
        print("\n数据预览 (前5行):")
        print(df.head())
        print(f"\n数据基本信息:")
        print(f"   数据形状: {df.shape}")
        print(f"   列名: {list(df.columns)}")
        
        # 7. 数据质量检查
        print("\n" + "=" * 60)
        missing_info = check_data_quality(df)
        
        # 8. 数据清洗
        print("\n" + "=" * 60)
        df_cleaned, changes_log = clean_taobao_data(df, missing_info)
        
        if df_cleaned is not None and len(df_cleaned) > 0:
            # 9. 清洗后质量检查
            print("\n" + "=" * 60)
            print("清洗后数据质量复查")
            print("=" * 60)
            check_data_quality(df_cleaned)
            
            # 10. 生成清洗报告
            print("\n" + "=" * 60)
            report_file = generate_cleaning_report(df, df_cleaned, changes_log)
            
            # 11. 保存清洗后数据
            print("\n" + "=" * 60)
            save_cleaned_data(df_cleaned, conn)
            
            print("\n数据清洗项目完成!")
            print("💡 下一步建议:")
            print("   1. 查看生成的数据清洗报告")
            print("   2. 对清洗后的数据进行探索性分析")
            print("   3. 考虑将结果上传到GitHub作为项目展示")
    
    finally:
        # 关闭数据库连接
        if conn.is_connected():
            conn.close()
            print("\n数据库连接已关闭")

# ==================== 程序入口 ====================
if __name__ == "__main__":
    main()