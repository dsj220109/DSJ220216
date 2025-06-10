# 搜索引擎日志分析-搜索关键词统计
项目实现
1. 数据准备
日志文件格式为：timestamp,user_id,search_query

2. 代码实现
python
from pyspark import SparkContext, SparkConf
from pyspark.storagelevel import StorageLevel
import jieba
import jieba.analyse

# 初始化Spark
conf = SparkConf().setAppName("SearchKeywordAnalysis")
sc = SparkContext(conf=conf)

# 设置检查点目录
sc.setCheckpointDir("hdfs://localhost:9000/checkpoint")

# 定义停用词列表
stop_words = ["的", "了", "和", "是", "在", "我", "有", "你", "他", "她", "它"]

# 广播停用词
broadcast_stop_words = sc.broadcast(stop_words)

# 初始化累加器
total_searches = sc.accumulator(0)
empty_searches = sc.accumulator(0)

def process_query(query):
    """使用jieba分词处理搜索查询"""
    total_searches.add(1)
    if not query.strip():
        empty_searches.add(1)
        return []
    
    # 分词并过滤停用词
    words = jieba.cut(query)
    filtered_words = [word for word in words 
                     if word not in broadcast_stop_words.value 
                     and len(word) > 1]
    return filtered_words

# 读取日志数据
log_rdd = sc.textFile("hdfs://localhost:9000/logs/search_logs.txt")

# 提取搜索内容
search_rdd = log_rdd.map(lambda line: line.split(",")[2].strip())

# 缓存RDD
search_rdd.persist(StorageLevel.MEMORY_AND_DISK)

# 分词处理
words_rdd = search_rdd.flatMap(process_query)

# 检查点机制
words_rdd.checkpoint()

# 词频统计
word_count_rdd = words_rdd.map(lambda word: (word, 1)) \
                         .reduceByKey(lambda a, b: a + b)

# 排序取前5
top_keywords = word_count_rdd.sortBy(lambda x: x[1], ascending=False) \
                            .take(5)

print("需求1结果: ", top_keywords)

# 打印统计信息
print(f"总搜索次数: {total_searches.value}")
print(f"空搜索次数: {empty_searches.value}")

# 保存结果
word_count_rdd.saveAsTextFile("hdfs://localhost:9000/output/keyword_counts")

# 清理资源
search_rdd.unpersist()
sc.stop()
四、关键技术点
1. RDD构建方式
使用textFile从HDFS读取原始日志

通过map和flatMap转换操作处理数据

使用reduceByKey进行聚合统计

2. 缓存和检查点机制
对频繁使用的search_rdd进行缓存

对计算代价高的words_rdd设置检查点

3. 共享变量使用
使用广播变量broadcast_stop_words分发停用词列表

使用累加器total_searches和empty_searches统计搜索量

4. 性能优化
合理设置分区数

在适当位置进行缓存和检查点

减少不必要的shuffle操作

五、项目结构
text
/search_log_analysis/
│── src/
│   ├── main.py                  # 主程序
│   ├── utils.py                 # 工具函数
│   └── config.py                # 配置参数
│── data/
│   ├── search_logs.txt          # 原始日志
│   └── stopwords.txt            # 停用词表
│── output/                      # 分析结果
│── docs/                        # 文档
│   └── README.md
│── .gitignore
│── requirements.txt
六、运行说明
将日志文件上传至HDFS

修改配置文件中HDFS路径

安装依赖：pip install -r requirements.txt

提交Spark作业：spark-submit src/main.py

七、预期输出
text
需求1结果:  [('学院', 8413), ('智能', 2800), ('建筑', 2716), ('scala', 2310), ('hadoop', 2268)]
总搜索次数: 25000
空搜索次数: 342
