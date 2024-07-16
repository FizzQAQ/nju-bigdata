# read arg "task num"
# run the corresponding test
task_num=$1
mvn package
hadoop jar target/Project-1.0-SNAPSHOT-jar-with-dependencies.jar proj-in proj-out-$1 proj-out-$1 proj-out-$1
hdfs dfs -get proj-out-$1 output