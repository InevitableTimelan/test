# -*- coding: utf-8 -*-
"""
淘宝母婴数据分析与可视化脚本
分析目标：
1. 销售趋势分析（时间维度）
2. 商品类别分析
3. 用户购买行为分析
4. 关键指标可视化
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import mysql.connector 
from mysql.connector import Error
import warnings
warnings.filterwarnings('ignore')

plt.rcParams['font.sans-serif']=['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False
sns.set_style("whitegrid")
sns.set_palette("husl")

COLORS = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', 
          '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

def connect_to_database():
    """连接到数据库"""
    try:
        connection = mysql.connector.connect(
            host = "localhost",
            user = "lanlala",
            password = "197312zjf.",
            database = "test"
            )
        print("数据库连接成功")
        return connection
    except Error as e:
        print(f"连接失败：{e}")
        return None

def load_data_from_table(connection,table_name,limit=None):
    """从指定表中加载数据"""
    if limit:
        query = f"SELECT * FROM `{table_name}` LIMIT {limit}"
    else:
        query = f"SELECT * FROM `{table_name}`"
    
    try:
        df = pd.read_sql(query,connection)
        print(f"成功读取到{len(df)}行数据")
        return df
    except Error as e:
        print(f"读取失败：{e}")
        return None

def preprocess_data(df):
    """数据预处理"""
    if df is None or len(df) == 0 :
        print("数据为空，无法处理！")
        return None
    
    df_processed = df.copy()
    
    print("进行数据预处理……")
    
    if 'user_id' in df_processed.columns:
        df_processed['user_id'] = df_processed['user_id'].astype(str)
    
    if 'day' in df_processed.columns:
        try:
            df_processed['day'] = pd.to_datetime(
                df_processed['day'].astype(str), 
                format='%Y%m%d', 
                errors='coerce'
            )
            print(f"已将day列转换为日期格式")
        except Exception as e:
            print(f"day列转换失败: {e}")
    
    if pd.api.types.is_datetime64_any_dtype(df_processed.get('day')):
        df_processed['year'] = df_processed['day'].dt.year
        df_processed['month'] = df_processed['day'].dt.month
        df_processed['year_month'] = df_processed['day'].dt.to_period('M')
        df_processed['weekday'] = df_processed['day'].dt.weekday
        print(f"已创建时间衍生特征")
        
    print(f"\n数据基本信息:")
    print(f"数据形状: {df_processed.shape}")
    print(f"时间范围: {df_processed['day'].min()} 到 {df_processed['day'].max()}")
    print(f"用户数量: {df_processed['user_id'].nunique()}")
    print(f"商品类别数: {df_processed['cat1'].nunique()}")
    
    return df_processed

def analyze_sales_trend(df):
    """分析销售趋势"""
    print("\n"+"="*60)
    print("销售趋势分析")
    print("="*60)
    
    results = {}
    
    if 'year_month' in df.columns and 'buy_mount' in df.columns:
        monthly_sales = df.groupby('year_month')['buy_mount'].sum().reset_index()
        monthly_sales['year_month'] = monthly_sales['year_month'].astype(str)
        results['monthly_sales'] = monthly_sales
    
    print(f"月度销售趋势:")
    for idx, row in monthly_sales.tail(5).iterrows():
        print(f"{row['year_month']}: {row['buy_mount']} 件")
    
    if 'day' in df.columns:
        daily_sales = df.groupby('day')['buy_mount'].sum().reset_index()
        results['daily_sales'] = daily_sales
    
    return results

def analyze_product_categories(df):
    """分析商品类别"""
    print("\n"+"="*60)
    print("商品类别分析")
    print("="*60)
    
    results = {}
    
    if 'cat1' in df.columns:
        category_sales = df.groupby('cat1').agg({
            'buy_mount':'sum',
            'auction_id':'count'
            }).reset_index()
        category_sales.columns = ['cat1', 'total_quantity', 'transaction_count']
        category_sales = category_sales.sort_values('total_quantity', ascending=False)
        results['category_sales'] = category_sales
        
        print(f"最畅销的商品类别 (按购买量):")
        for idx, row in category_sales.head(5).iterrows():
            print(f"类别 {row['cat1']}: {row['total_quantity']} 件 ({row['transaction_count']} 笔交易)")
    
    return results

def analyze_user_behavior(df):
    """分析用户购买行为"""
    print("\n" + "="*60)
    print("用户行为分析")
    print("="*60)
    
    results = {}
    
    if 'user_id' in df.columns:
        user_purchase_count = df.groupby('user_id').size().reset_index(name='purchase_count')
        user_purchase_count = user_purchase_count.sort_values('purchase_count', ascending=False)
        results['user_purchase_count'] = user_purchase_count
    
        user_stats = df.groupby('user_id').agg({
                   'buy_mount': 'sum',
                   'cat1': 'nunique',
                   'day': 'nunique'
                    }).reset_index()
        user_stats.columns = ['user_id', 'total_quantity', 'unique_categories', 'active_days']
        
        def classify_user(row):
            if row['total_quantity'] > user_stats['total_quantity'].quantile(0.8):
                return '高价值用户'
            elif row['total_quantity'] > user_stats['total_quantity'].quantile(0.5):
                return '中价值用户'
            else:
                return '低价值用户'
            
        user_stats['user_type'] = user_stats.apply(classify_user, axis=1)
        results['user_stats'] = user_stats
        
        print(f"最活跃的用户 (购买次数):")
        for idx, row in user_purchase_count.head(3).iterrows():
            print(f"用户 {row['user_id'][:10]}...: {row['purchase_count']} 次购买")
        
        print(f"\n用户分层统计:")
        print(user_stats['user_type'].value_counts().to_string())
    
    return results

def analyze_purchase_patterns(df):
    """分析购买模式"""
    print("\n" + "="*60)
    print("购买模式分析")
    print("="*60)
    
    results = {}
    
    if 'buy_mount' in df.columns:
       purchase_distribution = df['buy_mount'].describe()
       results['purchase_distribution'] = purchase_distribution
       
       print(f"购买数量统计:")
       print(f"平均值: {purchase_distribution['mean']:.2f}")
       print(f"中位数: {purchase_distribution['50%']:.2f}")
       print(f"最大值: {purchase_distribution['max']}")
       print(f"最小值: {purchase_distribution['min']}")
       
       bins = [0, 1, 5, 10, 20, 50, 100, 1000]
       labels = ['1件', '2-5件', '6-10件', '11-20件', '21-50件', '51-100件', '100+件']
       df['buy_mount_group'] = pd.cut(df['buy_mount'], bins=bins, labels=labels, right=False)
       results['buy_mount_groups'] = df['buy_mount_group'].value_counts()
   
    return results

def create_visualizations(df, analysis_results):
    """创建可视化图表"""
    print("\n" + "="*60)
    print("生成可视化图表")
    print("="*60)
    
    import os
    output_dir = "visualization_results"
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. 月度销售趋势图
    if 'monthly_sales' in analysis_results:
        fig, ax = plt.subplots(figsize=(12, 6))
        monthly_data = analysis_results['monthly_sales']
        
        ax.bar(range(len(monthly_data)), monthly_data['buy_mount'], color=COLORS[0])
        ax.set_xlabel('月份')
        ax.set_ylabel('购买总量')
        ax.set_title('月度购买趋势')
        ax.set_xticks(range(len(monthly_data)))
        ax.set_xticklabels(monthly_data['year_month'], rotation=45, ha='right')
        ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/monthly_sales_trend.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"已保存: {output_dir}/monthly_sales_trend.png")
    
    # 2. 商品类别销售图
    if 'category_sales' in analysis_results:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        category_data = analysis_results['category_sales'].head(10)
        
        # 左图：购买量
        ax1.barh(range(len(category_data)), category_data['total_quantity'], color=COLORS[1])
        ax1.set_yticks(range(len(category_data)))
        ax1.set_yticklabels(category_data['cat1'])
        ax1.set_xlabel('购买总量')
        ax1.set_title('Top 10 商品类别 (按购买量)')
        ax1.invert_yaxis()
        
        # 右图：交易次数
        ax2.barh(range(len(category_data)), category_data['transaction_count'], color=COLORS[2])
        ax2.set_yticks(range(len(category_data)))
        ax2.set_yticklabels(category_data['cat1'])
        ax2.set_xlabel('交易次数')
        ax2.set_title('Top 10 商品类别 (按交易次数)')
        ax2.invert_yaxis()
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/top_categories.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"已保存: {output_dir}/top_categories.png")
    
    # 3. 用户购买行为图
    if 'user_stats' in analysis_results:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        user_stats = analysis_results['user_stats']
        
        # 左图：用户类型分布
        user_type_counts = user_stats['user_type'].value_counts()
        ax1.pie(user_type_counts.values, labels=user_type_counts.index, 
                autopct='%1.1f%%', colors=COLORS[:3], startangle=90)
        ax1.set_title('用户价值分层分布')
        
        # 右图：购买数量分布
        if 'buy_mount_groups' in analysis_results:
            buy_groups = analysis_results['buy_mount_groups']
            ax2.bar(range(len(buy_groups)), buy_groups.values, color=COLORS[3])
            ax2.set_xlabel('购买数量区间')
            ax2.set_ylabel('交易次数')
            ax2.set_title('购买数量分布')
            ax2.set_xticks(range(len(buy_groups)))
            ax2.set_xticklabels(buy_groups.index, rotation=45, ha='right')
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/user_behavior.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"已保存: {output_dir}/user_behavior.png")
    
    # 4. 综合仪表板（多子图）
    fig = plt.figure(figsize=(15, 10))
    
    # 子图1：月度趋势
    ax1 = plt.subplot(2, 2, 1)
    if 'monthly_sales' in analysis_results:
        monthly_data = analysis_results['monthly_sales']
        ax1.plot(range(len(monthly_data)), monthly_data['buy_mount'], 
                marker='o', linewidth=2, color=COLORS[0])
        ax1.set_title('月度销售趋势', fontsize=12, fontweight='bold')
        ax1.set_xlabel('月份')
        ax1.set_ylabel('购买总量')
        ax1.grid(True, alpha=0.3)
        ax1.set_xticks(range(len(monthly_data)))
        ax1.set_xticklabels(monthly_data['year_month'], rotation=45, ha='right')
    
    # 子图2：商品类别
    ax2 = plt.subplot(2, 2, 2)
    if 'category_sales' in analysis_results:
        category_data = analysis_results['category_sales'].head(8)
        ax2.barh(range(len(category_data)), category_data['total_quantity'], color=COLORS[1:9])
        ax2.set_yticks(range(len(category_data)))
        ax2.set_yticklabels(category_data['cat1'])
        ax2.set_title('Top 8 商品类别', fontsize=12, fontweight='bold')
        ax2.set_xlabel('购买总量')
        ax2.invert_yaxis()
    
    # 子图3：购买数量分布
    ax3 = plt.subplot(2, 2, 3)
    if 'buy_mount' in df.columns:
        ax3.hist(df['buy_mount'], bins=30, color=COLORS[4], edgecolor='black', alpha=0.7)
        ax3.set_title('购买数量分布', fontsize=12, fontweight='bold')
        ax3.set_xlabel('购买数量')
        ax3.set_ylabel('频次')
        ax3.grid(True, alpha=0.3)
    
    # 子图4：用户活跃度
    ax4 = plt.subplot(2, 2, 4)
    if 'user_purchase_count' in analysis_results:
        user_data = analysis_results['user_purchase_count'].head(10)
        ax4.bar(range(len(user_data)), user_data['purchase_count'], color=COLORS[5])
        ax4.set_title('Top 10 活跃用户', fontsize=12, fontweight='bold')
        ax4.set_xlabel('用户排名')
        ax4.set_ylabel('购买次数')
        ax4.set_xticks(range(len(user_data)))
        ax4.set_xticklabels([f"用户{i+1}" for i in range(len(user_data))], rotation=45, ha='right')
    
    plt.suptitle('淘宝母婴数据分析仪表板', fontsize=16, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/analysis_dashboard.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"已保存: {output_dir}/analysis_dashboard.png")
    print(f"所有图表已保存到 '{output_dir}' 目录")
    
def generate_analysis_report(df, analysis_results, report_file="analysis_report.txt"):
    """生成分析报告"""
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("="*70 + "\n")
        f.write("淘宝母婴数据分析报告\n")
        f.write("="*70 + "\n\n")
        
        f.write(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("1.数据集概况\n")
        f.write("-"*40 + "\n")
        f.write(f"总交易记录数: {len(df)} 条\n")
        f.write(f"用户数量: {df['user_id'].nunique()} 人\n")
        f.write(f"商品类别数: {df['cat1'].nunique()} 类\n")
        f.write(f"时间范围: {df['day'].min()} 到 {df['day'].max()}\n\n")
        
        f.write("2.关键发现\n")
        f.write("-"*40 + "\n")
        
        # 销售趋势
        if 'monthly_sales' in analysis_results:
            monthly_data = analysis_results['monthly_sales']
            best_month = monthly_data.loc[monthly_data['buy_mount'].idxmax()]
            f.write(f"销售最高的月份: {best_month['year_month']}, 销量: {best_month['buy_mount']} 件\n")
        
        # 商品分析
        if 'category_sales' in analysis_results:
            category_data = analysis_results['category_sales']
            top_category = category_data.iloc[0]
            f.write(f"最畅销的商品类别: {top_category['cat1']}, 总销量: {top_category['total_quantity']} 件\n")
        
        # 用户分析
        if 'user_stats' in analysis_results:
            user_stats = analysis_results['user_stats']
            high_value_users = user_stats[user_stats['user_type'] == '高价值用户'].shape[0]
            f.write(f"高价值用户数量: {high_value_users} 人\n")
        
        f.write("\n3.建议\n")
        f.write("-"*40 + "\n")
        f.write(" 针对高价值用户推出会员计划，提高复购率\n")
        f.write("在销售旺季（如双十一）前增加库存\n")
        f.write("对畅销商品类别进行深度营销\n")
        f.write("分析用户购买模式，优化推荐系统\n")
    
    print(f"分析报告已保存到: {report_file}")

def main():
    """主函数"""
    print("开始淘宝母婴数据分析")
    print("="*60)
    
    # 1. 连接数据库
    conn = connect_to_database()
    if conn is None:
        return
    
    try:
        # 2. 读取数据（可以使用原始表或清洗后的表）
        table_name = "(sample)sam_tianchi_mum_baby_trade_history"  # 或使用清洗后的表名
        print(f"\n正在从表 '{table_name}' 读取数据...")
        df_raw = load_data_from_table(conn, table_name)
        
        if df_raw is None or len(df_raw) == 0:
            print("无法读取数据")
            return
        
        # 3. 数据预处理
        df = preprocess_data(df_raw)
        
        if df is None:
            return
        
        # 4. 执行各项分析
        analysis_results = {}
        
        # 销售趋势分析
        sales_results = analyze_sales_trend(df)
        analysis_results.update(sales_results)
        
        # 商品类别分析
        category_results = analyze_product_categories(df)
        analysis_results.update(category_results)
        
        # 用户行为分析
        user_results = analyze_user_behavior(df)
        analysis_results.update(user_results)
        
        # 购买模式分析
        purchase_results = analyze_purchase_patterns(df)
        analysis_results.update(purchase_results)
        
        # 5. 创建可视化图表
        create_visualizations(df, analysis_results)
        
        # 6. 生成分析报告
        generate_analysis_report(df, analysis_results)
        
        print("\n" + "="*60)
        print("数据分析完成!")
        print("="*60)
        print("输出文件:")
        print("visualization_results/ - 所有可视化图表")
        print("analysis_report.txt - 详细分析报告")
        print("\n下一步建议:")
        print(" 1.查看生成的图表和分析报告")
        print(" 2.可以考虑用Power BI制作交互式仪表板")
        print(" 3.将分析结果整理到GitHub项目")
        
    finally:
        # 关闭数据库连接
        if conn.is_connected():
            conn.close()
            print("\n数据库连接已关闭")

if __name__ == "__main__":
    main()