#javac -cp .:/opt/tiger/hadoop_deploy/hadoop-1.0.3/* toutiao/FileLineWritable.java
javac -cp .:/FileLineWritable.class:/opt/tiger/hadoop_deploy/hadoop-1.0.3/* toutiao/NFileRecordReader.java
javac -cp .:/opt/tiger/hadoop_deploy/hadoop-1.0.3/*:/opt/tiger/hadoop_deploy/hadoop-1.0.3/lib/* toutiao/NFileInputFormat.java
#jar -uf /opt/tiger/hadoop_deploy/hadoop-1.0.3/contrib/streaming/hadoop-streaming-1.0.3.jar  toutiao/*class
jar -uf /optc/home/baoyongjun/repos/hadoop_deploy/hadoop-1.0.3/contrib/streaming/hadoop-streaming-1.0.3.jar  toutiao/*class
