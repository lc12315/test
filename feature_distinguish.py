#-*- coding:utf-8 -*-
"""
    Description:
    Author: JD (search@jd.com)
    Date:
"""

import sys
#reload(sys).setdefaultencoding("utf-8")
import numpy as np

def is_hex(int_str):
    try:
        int(int_str, 16)
        return True
    except ValueError:
        return False

def int_converter(int_str):
    if int_str.isdigit():
        return int(int_str)
    elif is_hex(int_str):
        return int(int_str, 16)
    return int_str
# 建立从feature_id与feature_name的映射
def parse_feature_proto(proto_file):
    fid2fname = {}

    flag = False
    for line in open(proto_file):
        line = line.strip()
        comment_loc = line.find('//')

        if comment_loc == 0:
            continue
        elif comment_loc > 0:
            line = line[:comment_loc]

        if line == 'enum NumberFeatureID {':
            flag = True
        if flag and line == '}':
            flag = False

        if flag:
            line = line.strip()
            if line and line[-1] == ';':
                tmp1 = line[:-1].split('=')
                tmp = [fe.strip() for fe in tmp1]
                feature_name = '_'.join(tmp[0].lower().split('_')[1:])
                if int_converter(tmp[1]) > 2000:
                    continue
                fid2fname[int_converter(tmp[1])] = feature_name
    return fid2fname
 #计算特征的指标
def calculate_stats(feature_matrix):
    # calculate missing ratio:
    missing_ratio = np.sum(np.isnan(feature_matrix), axis=0, dtype=np.float) / feature_matrix.shape[0]
    mean = np.nanmean(feature_matrix, axis=0)
    std = np.nanstd(feature_matrix, axis=0)
    min = np.nanmin(feature_matrix, axis=0)
    max = np.nanmax(feature_matrix, axis=0)
    quantile_1 = np.nanpercentile(feature_matrix, axis=0, q=25)
    quantile_2 = np.nanpercentile(feature_matrix, axis=0, q=50)
    quantile_3 = np.nanpercentile(feature_matrix, axis=0, q=75)
    return np.vstack((missing_ratio, mean, std, min, max, quantile_1, quantile_2, quantile_3))
#入口函数，拆解数据取出feature字段，并且把fid与fname建立映射，可以实现统计，指标计算等功能
def parse_feature_data(feature_file,output_file):
    feature_data = {}
    with open(feature_file) as f1:
        count = 0    #记录有多少行
        for line in f1.readlines():
            line = line.strip().split("\t")
            if len(line) != 7:
                continue
            feature_str = line[1]
            features = feature_str.split(' ')
            for item in features:
                fid, fvalue = item.strip().split(':')
                fid = int(fid)
                fvalue = float(fvalue)
                if fid in feature_data.keys():
                    feature_data[fid].append(fvalue)
                else:
                    feature_data[fid] = []
                    feature_data[fid].append(fvalue)
            count += 1
    with open('/export/sdb/liuchang173/data/feature_3/feature_3_dt.txt', 'w') as f:
        feature_data[3].sort()
        sum = 0
        count = 0
        bucket = [0,0,0,0,0,0,0,0]
        for row in range(len(feature_data[3])):
            count = count+1
            if feature_data[3][row]!=0:

                sum = sum + feature_data[3][row]
                f.write(str(row)+'\t'+str(feature_data[3][row])+'\n')
                if feature_data[3][row] > 0 and feature_data[3][row] < 100:
                    bucket[0] = bucket[0] + 1
                if feature_data[3][row] >= 100 and feature_data[3][row] < 200:
                    bucket[1] = bucket[1] + 1
                if feature_data[3][row] >= 200 and feature_data[3][row] < 300:
                    bucket[2] = bucket[2] + 1
                if feature_data[3][row] >= 300 and feature_data[3][row] < 400:
                    bucket[3] = bucket[3] + 1
                if feature_data[3][row] >= 400 and feature_data[3][row] < 500:
                    bucket[4] = bucket[4] + 1
                if feature_data[3][row] >= 500 and feature_data[3][row] < 600:
                    bucket[5] = bucket[5] + 1
                if feature_data[3][row] >= 600 and feature_data[3][row] < 700:
                    bucket[6] = bucket[6] + 1
                if feature_data[3][row] >= 700 and feature_data[3][row] < 800:
                    bucket[7] = bucket[7] + 1
        f.write('\n'+'\n')
        mean = sum/count
        f.write("count:"+str(count)+'\n')
        f.write("mean:"+str(mean))
        f.write('\n'+'\n')
        for i in range(len(bucket)):
           f.write("bucket"+str(i)+':'+str(bucket[i])+'\t')
    label_to_feature_stats = {}
    for row in feature_data.keys():
        feature_matrix = np.array(feature_data[row])
        feature_stats = calculate_stats(feature_matrix)
        label_to_feature_stats[int(row)] = feature_stats
    # id_name_map = parse_feature_proto("/export/sdb/liuchang173/data/feature_framework.proto")
    # header = ['label', 'missing_ratio', 'mean', 'std', 'min', 'max', '25%', '50%', '75%']
    # with open(output_file, 'w') as f:
    #
    #
    #   for rows in label_to_feature_stats:
    #      #f.write(str(label_to_feature_stats[rows]))
    #      f.write(id_name_map[rows]+'\t')
    #      f.write('\t'.join([str(x) for x in label_to_feature_stats[rows]]) )
    #
    #      f.write('\n')
         
                        






if __name__ == '__main__':
    fid2fname = parse_feature_proto("/export/sdb/liuchang173/data/feature_framework.proto")
    #print(fid2fname)
    feature_data = parse_feature_data("/export/sdb/liuchang173/data/personalization_data_2019-07-04","/export/sdb/liuchang173/data/output/output.txt")
  
    #print(len(feature_data[0]))
    #print(feature_data)
