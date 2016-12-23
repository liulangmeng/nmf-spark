   nmfspark
1.用SBT工具进行编译链接打包
  修改代码后再次打包时,为了避免覆盖之前的版本,修改build.sbt文件中的version值即可自定义版本号
2.作业提交
   注意：将项目/usr/ideaprojects/nmfspark-master/lib文件夹中所有的非jar包文件拷贝到提交作业系统的/usr/lib文件夹下
   提交shell：
 spark-submit --master spark://192.168.179.150:7077 --class org.apache.spark.mllib.nmf /usr/ideaprojects/nmfspark-master/target/scala-2.10/nmfspark-master-assembly-2.0.jar /usr/ideaprojects/nmfspark-master/input/netscience1589-ph.csv mat 1589 1589 1 5 /usr/ideaprojects/nmfspark-master/output/WW.csv /usr/ideaprojects/nmfspark-master/output/HH.csv 

--master：指定spark集群的地址
--class：指定包中object对象，格式：包名.对象名，后跟jar包的路径
输入参数（8个）：
1）inpath：要分解的矩阵路径
2）variable：矩阵变量名
3）numrows:矩阵的行数
4）numcols:矩阵的列数
5）partitions:矩阵按行的分块数
6）rank:矩阵分解的控制数，如A：5*5，rank=2,则W：5*2，H：2*5
7) outdest1:保存W的文件路径
8）outdest2:保存H的文件路径

3.数据输入格式要求:
  .csv的矩阵形式，如果矩阵太稀疏，可以加平滑解决

