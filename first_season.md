# 初赛设计思路

资源：16C，8G内存，磁盘512G

7张表，共50G数据，一块先预估512G


1. 初始化表结构，共7张表，写死在代码里，不解析和处理 schema_info。
2. 多个线程读取多个文件，每行都按照相同逻辑处理，读出来，处理数据合法性，按主键 hash 后写入指定的子文件（这样处理后的信息是块内无序，块与块之间有序）。
3. 子文件按块读取后只取主键信息，排序后写入目标文件。

测试了一下基础的表数据对应每行的 AVG_ROW_LENGTH 信息

```
+----------------+------------+
| AVG_ROW_LENGTH | TABLE_NAME |
+----------------+------------+
|            843 | customer   |
|            124 | district   |
|             83 | history    |
|            102 | item       |
|             38 | new_orders |
|             87 | order_line |
|             51 | orders     |
|            398 | stock      |
|            194 | warehouse  |
+----------------+------------+
```

customer：1500w 行， 843 * 1500w 约 12G
district：5000行，约 1M
item： 10w 行，约 9M
new_orders：4500w行，约 1.6G
order_line：15000w行，约 12G ++
orders：1500w行，约 730M
stock： 5000w行，约 19～20G
warehouse：500行，约 95KB

一块 buff 预估设计 512M
能存储 
customer 60w行
district 420w
item 500w
new_orders 1500w
order_line 620w
orders 1000w
stock 135w
warehouse 270w
