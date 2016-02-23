mkdir runningMean_class
javac -classpath /opt/hadoop/hadoop-core-1.2.1.jar:/lib/commons-cli-1.2.jar -d runningMean_class RunningMean.java Time_Series.java Date_Util.java
jar -cvf RunningMean.jar -C /home/vcslstudent/runningMean_class/ .
hadoop jar /home/vcslstudent/RunningMean.jar snedeker.cc.project1.cluster.RunningMean /user/vcslstudent/input/sample.txt /user/vcslstudent/RunningMean_out_java
