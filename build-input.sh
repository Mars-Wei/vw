javac -cp .:/opt/tiger/hadoop_deploy/hadoop-1.0.3/* vw/NFileRecordReader.java
javac -cp .:/hadoop_deploy/hadoop-1.0.3/*:/hadoop_deploy/hadoop-1.0.3/lib/* vw/NFileInputFormat.java
jar -uf /repos/hadoop_deploy/hadoop-1.0.3/contrib/streaming/hadoop-streaming-1.0.3.jar  vw/*class
