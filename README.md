## sparktraining
Examples for Spark Training in chinahadoop.cn


## 本地运行Spark方法
  - 下载spark安装包
  - 解压spark安装包
  - 进入spark解压目录下，运行：
  ```bash
  $ bin/spark-shell
  ```
  - 在命令行提示符下拷贝以下代码并查看执行结果
  ```scala
  import scala.math.random

  val tasks = 10
  val n = tasks * 100000

  val count = sc.parallelize(1 until n, tasks).map { i =>
    val x = random * 2 - 1
    val y = random * 2 - 1
    if (x*x + y*y <= 1) 1 else 0
  }.reduce(_ + _)
  println("Pi is roughly " + 4.0 * count / n )
  ```
## 分布式运行Spark方法
  ### 搭建hadoop集群
  Hadoop YARN/HDFS配置文件参考：conf/hadoop目录

  ### 配置Spark客户端，并启动spark history server
  - Spark客户端配置文件参考：conf/spark目录
  - 启动spark history server: sbin/start-history-server.sh

  ### 将spark-shell运行在yarn client或cluster模式
  - yarn client模式：bin/spark-shell --master yarn --deploy-mode client
  - yarn cluster：bin/spark-shell --master yarn --deploy-mode cluster
