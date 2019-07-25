import os
import pandas as pd
import numpy as np

#==========================================================
#用于处理读取
#==========================================================
datapath = '/home/hadoop/sdl/hdfs_data/61/'
documentpath = 'cal_perf_permonth20190725'
#用于读取转化后的数据，由于那些数据没有标题需要自行添加标题
def readonecsv(filename):
    df = pd.read_csv('/home/hadoop/sdl/hdfs_data/61/{}'.format(filename), header = None)
    df.columns = ['tdid','pkgName','is_active','type','type_code','frequecncy','appHash']
    return df

#用于读取并合并当月所有dataframe
def read_merge_df(l_header,l_noheader):
    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    if len(l_header) != 0:
        for i in l_header:
            temp1 = pd.read_csv('{}{}'.format(datapath,i))
            df1 = df1.append(temp1,ignore_index=True)
    if len(l_noheader) != 0:
        for j in l_noheader:
            temp2 = readonecsv(j)
            df2 = df2.append(temp2,ignore_index=True)
    df = df1.append(df2,ignore_index=True)
    print('data has been read')
    return df

#==========================================================
#用于处理需求特征
#===========================================================
#这个函数用于获得一个关于游戏的type与game的索引表
def gettype_hash(df):
    df = df.dropna(subset = ['type_code'])
    df = df[['type','type_code','pkgName','appHash']]
    df = df.drop_duplicates()
    df = df.dropna(subset = ['pkgName'])
    dfgame = df[df['type_code'].str.contains(r'T2\d*')]
    return dfgame


#用于获取游戏type与用户的索引表
def gettype_tdid(df):
    df = df[['type_code','tdid']].dropna(subset= ['type_code'])
    dfgame = df[df['type_code'].str.contains(r'T2\d*')]
    dfgame = dfgame.drop_duplicates()
    dfgame = dfgame.reset_index().drop(['index'],axis = 1)
    return dfgame


#这个函数是用于计算每个用户的总的app安装数量，列名为appHash，以及总的app种类type_code
def gettdid_demand1(df):
    df = df.drop_duplicates(subset = ['tdid','pkgName','appHash'])
    #df = df.dropna(subset = ['type_code'],how = 'any')
    dfgroup = df.groupby('tdid')[['appHash','type_code']]
    tc = dfgroup.agg({'appHash':'nunique','type_code':'nunique'})
    tc = tc.reset_index()
    tc.rename(columns = {'appHash':'app_num','type_code':'app_type_num'},inplace = True)
    return tc

#用于计算每个用户game的个数
def gettdid_gamenum(df):
    df = df[['tdid','type','type_code','appHash']].drop_duplicates(subset = ['tdid','appHash'])
    df = df.dropna(subset = ['type_code'])
    dfgame = df[df['type_code'].str.contains(r'T2\d*')]
    #group1用于计算几种类型的game，总共安装了多少game
    group1 = dfgame.groupby('tdid')
    g1 = group1.agg({'type_code':'nunique','appHash':'nunique'})
    g1 = g1.reset_index()
    g1.rename(columns = {'type_code':'game_type_num','appHash':'game_num'},inplace = True)
    #group2用于计算每种类型有几个game
    group2 = dfgame.groupby(['tdid','type_code'])
    g2 = group2.agg({'appHash':'nunique'})
    g2 = g2.reset_index()
    g2.rename(columns = {'appHash':'game_num_pertype'}, inplace = True)
    r = pd.merge(g1,g2,on = 'tdid',left_index=False, right_index=False)
    #r = r.drop(columns = ['index_x','index_y'])
    return [g1,g2,r]

#计算出每个类别的需求特征
def demand_attr(df, date = 20170131):
    df_type_id = gettype_tdid(df)
    df_app_num = gettdid_demand1(df)
    re1 = gettdid_gamenum(df)
    
    #一、负责得出d1，得出游戏个数，app个数，游戏种类，app种类，游戏比例，游戏类型比例的均值和方差
    #分两步合并，将游戏类型，用户id，用于安装app总数，种类总数，游戏个数，游戏种类总数整合到dataframe：df_d1
    part1 = pd.merge(df_type_id,df_app_num, on = 'tdid')
    df_d1 = pd.merge(part1, re1[0], on = 'tdid')
    df_d1['ratio_game'] = df_d1['game_num']/df_d1['app_num']
    df_d1['ratio_game_type'] = df_d1['game_type_num']/df_d1['app_type_num']
    #g1用于处理df_d1,负责得出游戏个数，app个数，游戏种类，app种类，游戏比例，游戏类型比例的均值和方差
    g1 = df_d1.groupby('type_code')
    g1_cal = g1.agg(['mean','var'])
    
    d1 = g1_cal.reset_index()
    d1.columns = ['type_code','app_m','app_v','apptype_m','apptype_v','gtype_m','gtype_v','game_m','game_v','rgame_m','rgame_v','rgtype_m','rgtype_v']
    
    
    #二、负责得出d2，得出每一类游戏占游戏的总数的比例
    df_d2 = re1[2]
    df_d2['type_ratio'] = df_d2['game_num_pertype']/df_d2['game_num']
    g2 = df_d2.groupby('type_code')['type_ratio']
    g2_cal = g2.agg([('type_ratio_m','mean'),('type_ratio_v','var')])
    d2 = g2_cal.reset_index()
    
    demand = pd.merge(d1,d2,on = 'type_code', how = 'outer')
    demand['date'] = date
    return demand

#=======================================================================
#以下用于计算绩效
#========================================================================
#计算当月总用户数
def gettotal_user_num(df):
    user = df['tdid'].drop_duplicates()
    user_num = user.nunique()
    return user_num

#用于计算游戏与用户id的对应关系表
def getgame_tdid(df):
    df = df[['pkgName','tdid','type_code','is_active']].dropna(subset=['type_code'])
    dfgame = df[df['type_code'].str.contains(r'T2\d*')]
    dfgame = dfgame.drop_duplicates()
    return dfgame


#用于计算绩效
def calperf(df,date = 20170131):
    user_num = gettotal_user_num(df)
    dfgame = getgame_tdid(df)
    #g1用于计算月活跃率，根据talking data的定义月活跃率为当月活跃的用户数，此处得到的最终结果dataframe为mau
    df_g1 = dfgame[dfgame['is_active'] == True]
    g1 = df_g1.groupby(['type_code','pkgName'])
    g1_mau = g1.agg({'tdid':'nunique'})
    mau = g1_mau.reset_index()
#     mau['user_num'] = user_num
    mau.rename(columns = {'tdid':'active_num'},inplace = True)
    #g2用于计算覆盖率，根据talking data的定义为安装的用户数，最终得到的dataframe为cover
    g2 = dfgame.groupby(['type_code','pkgName'])
    g2_cover = g2.agg({'tdid':'nunique'})
    cover = g2_cover.reset_index()
    cover['user_num'] = user_num
    cover.rename(columns = {'tdid':'cover_num'},inplace = True)
    #将两个表合并到一个表
    perf = pd.merge(mau,cover,on = ['pkgName','type_code'],how = 'outer')
#     perf['active_num'] = perf['active_num'].fillna(0)
    #计算月活跃率和覆盖率
    perf['mau'] = perf['active_num']/perf['user_num']
    perf['coverage'] = perf['cover_num']/perf['user_num']
    
    #添加日期
    perf['date'] = date
    return perf

def calmau(df,date = 20170131):
    user_num = gettotal_user_num(df)
    dfgame = getgame_tdid(df)
    #g1用于计算月活跃率，根据talking data的定义月活跃率为当月活跃的用户数，此处得到的最终结果dataframe为mau
    df_g1 = dfgame[dfgame['is_active'] == True]
    g1 = df_g1.groupby(['type_code','pkgName'])
    g1_mau = g1.agg({'tdid':'nunique'})
    mau = g1_mau.reset_index()
    mau['user_num'] = user_num
    mau.rename(columns = {'tdid':'active_num'},inplace = True)
    mau['mau'] = mau['active_num']/mau['user_num']
    mau['date'] = date
    return mau
def calcover(df, date = 20170131):
    user_num = gettotal_user_num(df)
    dfgame = getgame_tdid(df)
    #g2用于计算覆盖率，根据talking data的定义为安装的用户数，最终得到的dataframe为cover
    g2 = dfgame.groupby(['type_code','pkgName'])
    g2_cover = g2.agg({'tdid':'nunique'})
    cover = g2_cover.reset_index()
    cover['user_num'] = user_num
    cover.rename(columns = {'tdid':'cover_num'},inplace = True)
    cover['coverage'] = cover['cover_num']/cover['user_num']
    cover['date'] = date
    return cover
#=====================
#主程序
#=======================
def mainfunc(l_header,l_noheader,date):
    df = read_merge_df(l_header,l_noheader)
#     demand = demand_attr(df, date = date)
    perf = calperf(df,date = date)
#     index_type_game = gettype_hash(df)
#     demand.to_csv(os.path.join(documentpath,'demand_category','{}demandcategory.csv'.format(date)))
#     print('demand table')
#     print(demand.shape)
#     print(demand.columns)
    perf.to_csv(os.path.join(documentpath,'perf','{}perf.csv'.format(date)))
    print('perf table')
    print(perf.shape)
    print(perf.columns)
#     index_type_game.to_csv(os.path.join(documentpath,'index','{}index.csv'.format(date)))
#     print('index table')
#     print(index_type_game.shape)
#     print(index_type_game.columns)
#     #测试用存储
#     demand.to_csv(os.path.join(documentpath,'test','{}demandcategory.csv'.format(date)))
#     perf.to_csv(os.path.join(documentpath,'test','{}perf.csv'.format(date)))
#     index_type_game.to_csv(os.path.join(documentpath,'test','{}index.csv'.format(date)))
#     #测试用存储结束
    print('success {}'.format(date))
    return perf



# l1 = ['part-00001-6bd97f9c-63e7-49d5-9906-3be536accf14-c000.csv_579',
#       'part-00002-6bd97f9c-63e7-49d5-9906-3be536accf14-c000.csv_15',
#       'part-00003-6bd97f9c-63e7-49d5-9906-3be536accf14-c000.csv_482']

# l2 = ['part-00000-7273be2c-97d9-4466-a5b9-5195cfd0769a-c000.csv_289',
#       'part-00000-3f4eb766-c335-42f0-8853-bd84b441e716-c000.csv_918']

# date = 'test123444'

# r = mainfunc(l1,l2,date)

def mainfunc2(l_header,l_noheader,date):
    df = read_merge_df(l_header,l_noheader)
    mau = calmau(df,date = date)
    mau.to_csv(os.path.join(documentpath,'mau','{}mau.csv'.format(date)))
    print('mau table')
    print(mau.shape)
    print(mau.columns)
    return mau
def mainfunc3(l_header,l_noheader,date):
    df = read_merge_df(l_header,l_noheader)
    cover = calcover(df,date = date)
    cover.to_csv(os.path.join(documentpath,'cover','{}cover.csv'.format(date)))
    print('cover table')
    print(cover.shape)
    print(cover.columns)
    return cover
