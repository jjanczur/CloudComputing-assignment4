# CloudComputing-assignment4

This is the repo imported from TU Berlin GitLab with the code for the assignment 4 in Cloud Computing course. 

We got **maximum number of points**, so enjoy our work.    
If you find it useful, you can give us a star.

## Task

The task was to use flink and implement a few paralel computing algorithms. We as well measurred influence of parallelism to the execution time.

# discussion.txt

## Run your programs with different job configurations:

## Two worker nodes

WordCount   

Parallelism: 1 -> execution time: 8   
Parallelism: 2 -> execution time: 11   
Parallelism: 4 -> execution time: 22   

CellCluster (berlin)

Parallelism: 1 -> execution time: 18   
Parallelism: 2 -> execution time: 11   
Parallelism: 4 -> execution time: 22   


Parallelism set to 2 performed the best.    
That is why the following tests are going to be conducted using parallelism set ot 2.   

Iterations: 10   
MNC: 1;3;6  
K: 500  

Result: 14   

Iterations: 100   
MNC: 1;3;6   
K: 500   

Result: 36   

Iterations: 500   
MNC: 1;3;6   
K: 500   

Result: 99   

Iterations: 100   
MNC: 1;3;6   
K: 100   

Result: 31   

Iterations: 100   
MNC: 1;3;6   
K: 500   

Result: 34   

Iterations: 100   
MNC: 1;3;6
K: 1000

Result: 38   


### 5 worker nodes:

--input s3://tub-cc-assignment4-flink/tolstoy-war-and-peace.txt --output s3://tub-cc-assignment4-flink/wordcount.csv   
Parallelism: 1 -> execution time: 11s   
Parallelism: 2 -> execution time: 15s   
Parallelism: 4 -> execution time: 14s   
Parallelism: 5 -> execution time: 9s   
Parallelism: 8 -> execution time: 15s   


--input s3://tub-cc-assignment4-flink/berlin.csv --iterations 100 --mnc 1;3;6 --k 500 --output s3://tub-cc-assignment4-flink/berlinClusters.csv   
Parallelism: 1 -> execution time: 38s   
Parallelism: 2 -> execution time: 28s   
Parallelism: 4 -> execution time: 45s   
Parallelism: 5 -> execution time: 30s   
Parallelism: 8 -> execution time: 37s   


--input s3://tub-cc-assignment4-flink/germany.csv --iterations 100 --mnc 1;3;6 --k 500 --output s3://tub-cc-assignment4-flink/clusters.csv   
Parallelism: 1 -> execution time: 4m17s   
Parallelism: 2 -> execution time: 2m55s   
Parallelism: 4 -> execution time: 1m47s   
Parallelism: 5 -> execution time: 1m40s   
Parallelism: 8 -> execution time: 2m17s   

## Answer the following questions separately for both programs:
### 1. Which steps in your program require communication and synchronization between your workers?

Answers are based the flink web console   
WordCount.java   
	Communication: between GroupCombine - GroupReduce, Sort-Partition - Data Sink.   
	Synchronisation: is required while we are receiving the data. One Worker synchronization.   

CellCluster.java   
	Communication: all steps   
	Synchronisation: writing the output, while finding the first centroid in GroupReduce    

### 2. What resources are your programs bound by? Memory? CPU? Network? Disk?

WordCount.java   
	Network - reading data from s3 bucket is the longest execution step in an optimal parallelism mode (2). It goes through the network. 

#CellCluster.java    
	CPU - in contrast to WordCount reading data from s3 bucket was the shortest execution part that is why we assume that in this case the program is bounded on CPU.

### 3. Could you improve the partitioning of your data to yield better run-time?

#WordCount.java   
	No. Increasing parallelism resulted in worse runtime. Program is network bonded and parallelization in data reading by multiple workers won't help either due to communication overhead.

#CellCluster.java   
	No. Increasing parallelism resulted in worse runtime. It also made the program network bonded instead of CPU.   
	Another way in which we that could achieve better runtime might be increasing input file - berlin to germany and then increasing parallelism. We conducted such experiment and the results are similar to the expected.
	
# Setup


listing.txt

```shell
aws s3api create-bucket \
    --bucket tub-cc-assignment4-flink \
    --region us-east-1

aws emr create-default-roles
```

#ubuntu_key was created in previous assignment
```shell
aws emr create-cluster --name "Cluster with Flink" --release-label emr-5.20.0 \
    --log-uri s3://tub-cc-assignment4-flink/ \
    --applications Name=Flink --ec2-attributes KeyName=ubuntu_key \
    --instance-type m4.large --instance-count 3 --use-default-roles \
    --steps Type=CUSTOM_JAR,Jar=command-runner.jar,Name=Flink_Long_Running_Session,Args="flink-yarn-session -n 2 -d"
```

#Go through "Enable Web Connection" steps

#Hadoop web console at http://ec2-54-145-55-80.compute-1.amazonaws.com:8088/cluster   
#Flink web console at http://ip-172-31-36-21.ec2.internal:20888/proxy/application_1524164137528_0001/#/overview   

#Flink jobs:   

#WordCount.java   
`--input s3://tub-cc-assignment4-flink/tolstoy-war-and-peace.txt --output s3://tub-cc-assignment4-flink/wordcount.csv`


#CellCluster.java berlin.csv   
`--input s3://tub-cc-assignment4-flink/berlin.csv --iterations 100 --mnc 1;6;78 --k 500 --output s3://tub-cc-assignment4-flink/berlinClusters.csv`

#CellCluster.java germany.csv   
`--input s3://tub-cc-assignment4-flink/germany.csv --iterations 100 --mnc 1;6;78 --k 500 --output s3://tub-cc-assignment4-flink/clusters.csv`

To setup Flink on EMR no additional files were needed.
