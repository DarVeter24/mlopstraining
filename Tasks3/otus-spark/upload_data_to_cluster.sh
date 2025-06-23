hdfs dfs -mkdir -p /user/ubuntu/data

hadoop distcp s3a://otus-bucket-b1gtvbvk47oh4lr4r4cb/ /user/ubuntu/data

hdfs dfs -ls /user/ubuntu/data

hadoop distcp s3a://otus-bucket-b1gtvbvk47oh4lr4r4cb/questions.csv /user/ubuntu/data/questions.csv