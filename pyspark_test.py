# 综合案例2
"""
·各个城市的销售额排名，从大到小
·全部城市中，有哪些商品在售卖
·北京市有哪些商品在售卖
·文件路径：E:\\Information\\Python\\PY_Projects\\PySpark入门\\综合案例分析资料\\orders.txt
·文件格式：json，需要转python格式
"""

# 导包
from pyspark import SparkConf, SparkContext
from json import loads
import os

# 为pyspark程序配置python解释器
os.environ["PYSPARK_PYTHON"] = "D:\\AppInstall\\python.3.10\\python.exe"

# 构建执行环境的入口对象
conf = SparkConf().setMaster("local[*]").setAppName("test_2")
sc = SparkContext(conf = conf)

# 构建rdd
rdd = sc.textFile("E:\\Information\\Python\\PY_Projects\\PySpark入门\\综合案例分析资料\\orders.txt")

# TODO 计算所有城市销售额排名
result_area_money = (
    rdd.flatMap(
        # split方法分割rdd的元素
        lambda element: element.split("|")
    ).map(
        # json格式转python格式
        lambda element: loads(element)
    ).map(
        # 构建KV型rdd（城市名，商品名），money需要强制类型转换为int
        lambda element: (element["areaName"], int(element["money"]))
    ).reduceByKey(
        # 针对KV型rdd按照传入的聚合逻辑进行分组聚合
        lambda element_1, element_2: element_1 + element_2
    ).sortBy(
        # 按照rdd每个元素的第二位进行排序
        lambda element: element[1],
        # 降序排序
        ascending = False,
        # 分1个区
        numPartitions = 1
    )
)

# TODO 计算所有城市中的在售商品
result_category = (
    rdd.flatMap(
        # split方法分割rdd的元素
        lambda element: element.split("|")
    ).map(
        # json格式转python格式
        lambda element: loads(element)
    ).map(
        # 构建KV型rdd（城市名，商品名）
        lambda element: (element["areaName"], element["category"])
    ).distinct(
        # distinct方法去除rdd中的重复项
    ).reduceByKey(
        # 针对KV型rdd按照传入的聚合逻辑进行分组聚合
        lambda element_1, element_2: element_1 + "," + element_2
    )
)

# TODO 计算北京市在售商品
result_beijing = (
    rdd.flatMap(
        # split方法分割rdd的元素
        lambda element: element.split("|")
    ).map(
        # json格式转python格式
        lambda element: loads(element)
    ).map(
        # 构建KV型rdd（城市名，商品名）
        lambda element: (element["areaName"], element["category"])
    ).filter(
        # 去除其他数据，只保留北京市的数据
        lambda element: element[0] == "北京"
    ).distinct(
        # distinct方法去除rdd中的重复项
    ).reduceByKey(
        # 针对KV型rdd按照传入的聚合逻辑进行分组聚合
        lambda element_1, element_2: element_1 + "," + element_2
    )
)

# 输出rdd
print(
    f"所有城市销售额排行：{result_area_money.collect()}\n"
    f"所有城市在售商品类别：{result_category.collect()}\n"
    f"北京市在售商品类别：{result_beijing.collect()}"
)

# 终止pyspark程序
sc.stop()
