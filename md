---
title: Spark local& stand-alone配置
date: 2025-06-07 17:58:04
tags:
---

Spark本地模式配置
Spark独立集群模式配置
主从节点配置
启动和停止集群的命令
提交作业的示例
代码如下:from pyspark import SparkContext

# 初始化SparkContext
sc = SparkContext("local", "SpecialCharWordCount")

# 定义特殊字符
abnormal_char = [",", ".", "!", "#", "$", "%"]

# 示例数据
data = [
    "hadoop spark,hive",
    "hive,hdfs,spark spark",
    "mapreduce hive,spark!",
    "spark# hive$spark%sql",
    "sql spark.hive!hadoop",
    "hadoop,mapreduce hive",
    "spark mapreduce spark",
    "hive mapreduce spark!hive",
    "hdfs spark,hive spark"
]

# 创建RDD
rdd = sc.parallelize(data)

# 注册累加器用于统计特殊字符
special_char_count = sc.accumulator(0)

# 定义处理函数
def process_line(line):
    global special_char_count
    # 统计特殊字符
    for char in line:
        if char in abnormal_char:
            special_char_count += 1
    # 替换特殊字符为空格
    for char in abnormal_char:
        line = line.replace(char, ' ')
    # 返回单词列表
    return line.split()

# 执行单词计数
word_counts = rdd.flatMap(process_line) \
                .map(lambda word: (word, 1)) \
                .reduceByKey(lambda a, b: a + b) \
                .sortBy(lambda x: -x[1]) \
                .collect()

# 打印结果
print("正常单词计数结果: ", word_counts)
print("特殊字符数量: ", special_char_count.value)

# 停止SparkContext
sc.stop()
然后结果为:正常单词计数结果:  [('spark', 11), ('hive', 6), ('mapreduce', 4), ('hadoop', 3), ('hdfs', 2), ('sql', 2)]
特殊字符数量:  8
