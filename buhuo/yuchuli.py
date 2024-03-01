import pandas as pd
sku=['supplier_name', 'supplier_part_no', 'customer_name', 'customer_part_no', 'manufacture_name','site']
df1=pd.read_csv(r'F:\最新数据\inbound.csv')
df2=pd.read_csv(r'F:\最新数据\asn.csv')
df3=pd.read_csv(r'F:\最新数据\out_8_28.csv')
df4=pd.read_csv(r'F:\补货实验\inventory_0.csv')
df5=pd.read_csv(r'F:\补货实验\demand_0.csv')
grouped_df1 = df1.groupby(['supplier_name', 'supplier_part_no', 'customer_name', 'customer_part_no', 'manufacture_name','site'])
grouped_df2 = df2.groupby(['supplier_name', 'supplier_part_no', 'customer_name', 'customer_part_no', 'manufacture_name','site'])
grouped_df3 = df3.groupby(['supplier_name', 'supplier_part_no', 'customer_name', 'customer_part_no', 'manufacture_name','site'])
grouped_df4 = df4.groupby(['supplier_name', 'supplier_part_no', 'customer_name', 'customer_part_no', 'manufacture_name','site'])
grouped_df5 = df5.groupby(['supplier_name', 'supplier_part_no', 'customer_name', 'customer_part_no', 'manufacture_name','site'])
#
# 步骤2: 找到这四个DataFrame分组中的交集
common_groups = set(grouped_df1.groups.keys()) & set(grouped_df2.groups.keys()) & set(grouped_df3.groups.keys()) & set(grouped_df4.groups.keys()& set(grouped_df5.groups.keys()))
print(type(common_groups))
# 步骤3: 保留在交集中出现的组的数据
result_df1 = pd.concat([grouped_df1.get_group(group) for group in common_groups if group in grouped_df1.groups])
result_df2 = pd.concat([grouped_df2.get_group(group) for group in common_groups if group in grouped_df2.groups])
result_df3 = pd.concat([grouped_df3.get_group(group) for group in common_groups if group in grouped_df3.groups])
result_df4 = pd.concat([grouped_df4.get_group(group) for group in common_groups if group in grouped_df4.groups])
result_df5 = pd.concat([grouped_df5.get_group(group) for group in common_groups if group in grouped_df5.groups])
result_df3.to_csv(r'F:\补货实验\out_0.csv')
result_df2.to_csv(r'F:\补货实验\asn_0.csv')
result_df1.to_csv(r'F:\补货实验\inbound_0.csv')
result_df4.to_csv(r'F:\补货实验\inventory_0.csv')
result_df5.to_csv(r'F:\补货实验\demand_0.csv')










# 2:拼接deamnd需求预测
# df1=pd.read_csv(r'E:\model\tft_Day_14_14buhuo_pre1.csv')
# df2=pd.read_csv(r'E:\model\tft_Day_14_14buhuo_pre2.csv')
# sku=['customer_name', 'customer_part_no','supplier_name',	'supplier_part_no','manufacture_name','site']
# merged_df =pd.concat([df1, df2]).sort_values(by=sku)
# merged_df.to_csv(r'F:\补货实验\demand_0.csv')






# # 3 对inventory表格进行处理
# df=pd.read_csv(r'F:\最新数据\v_islm_inventory_20240220.csv')
# df['inventory_dt'] = pd.to_datetime(df['inventory_dt'])
# print(df['inventory_dt'])
# grouped_df = df.groupby(sku)
# #定义函数，检查是否有 2023 年 12 月份数据不满 31 天
# def check_december(group):
#     december_data = group[group['inventory_dt'].dt.month == 8]
#     return len(december_data) == 31
# # 过滤不符合条件的组
# filtered_groups = grouped_df.filter(check_december).reset_index()
# filtered_groups =filtered_groups.drop('index',axis=1)
# print(filtered_groups)
# filtered_df = filtered_groups.groupby(sku).filter(lambda x:'2023-07-31' in x['inventory_dt'].dt.strftime('%Y-%m-%d').values)
# print(filtered_df)
# filtered_df.to_csv(r'F:\补货实验\inventory_0.csv')
# # #1:提取公共sku
