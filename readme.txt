   nmfspark
1.��SBT���߽��б������Ӵ��
  �޸Ĵ�����ٴδ��ʱ,Ϊ�˱��⸲��֮ǰ�İ汾,�޸�build.sbt�ļ��е�versionֵ�����Զ���汾��
2.��ҵ�ύ
   ע�⣺����Ŀ/usr/ideaprojects/nmfspark-master/lib�ļ��������еķ�jar���ļ��������ύ��ҵϵͳ��/usr/lib�ļ�����
   �ύshell��
 spark-submit --master spark://192.168.179.150:7077 --class org.apache.spark.mllib.nmf /usr/ideaprojects/nmfspark-master/target/scala-2.10/nmfspark-master-assembly-2.0.jar /usr/ideaprojects/nmfspark-master/input/netscience1589-ph.csv mat 1589 1589 1 5 /usr/ideaprojects/nmfspark-master/output/WW.csv /usr/ideaprojects/nmfspark-master/output/HH.csv 

--master��ָ��spark��Ⱥ�ĵ�ַ
--class��ָ������object���󣬸�ʽ������.�����������jar����·��
���������8������
1��inpath��Ҫ�ֽ�ľ���·��
2��variable�����������
3��numrows:���������
4��numcols:���������
5��partitions:�����еķֿ���
6��rank:����ֽ�Ŀ���������A��5*5��rank=2,��W��5*2��H��2*5
7) outdest1:����W���ļ�·��
8��outdest2:����H���ļ�·��

3.���������ʽҪ��:
  .csv�ľ�����ʽ���������̫ϡ�裬���Լ�ƽ�����

