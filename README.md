# market-basket-analysis
Market Basket Analysis using Hadoop MapReduce

Market Basket Analysis is a modeling technique based upon the theory that if you buy a certain group of items, you are more (or less) likely to buy another group of items.
FORMULA: 
    Total number of transactions = N
    SUPPORT=frequency (X,Y)/N
    CONFIDENCE=frequency (X,Y)/frequency(X)

DESCRIPTION OF INPUT:
    In the 'transactions' file sample transactions are given. All the items bought together are in a single transaction.
    
DESCRIPTION OF OUTPUT:
    TRANSACTIONS             {SUPPORT,CONFIDENCE}
    [apples] => [heineken] {0.104895 , 0.334395}
    [apples] => [steak,corned_b] {0.101898 , 0.324841}
    [apples] => [avocado,baguette] {0.111888 , 0.356688}
    [apples] => [hering,corned_b,olives] {0.108891 , 0.347134}
    [artichok] => [hering,avocado] {0.116883 , 0.383607}    
The support and confidence value of each transaction set is calculated.

System used:
    OS : centOS 6.5
    Hadoop 2.7.1+
    Java JDK 1.7+
    Eclipse
    
# to run the code 

 * put sample data file 'transactions' into hdfs as input directory.
 * add external jar files into eclipse projects.
 * export to jar for both the map reduce jobs(ie. mba_hadoop_1,mba_hadoop_2)
 * run the below commands in terminal to perform the 2 jobs as following:
    1) hadoop jar hadoop_1.jar mit.mba.hadoop.MBADriver [input_dir] [1st_job_putput_dir]
    2) hadoop jar hadoop_2.jar mit.mba.hadoop.MBADriver [1st_job_putput_dir] [2nd_job_putput_dir]
    
The output of the 1st job is fed to the input of the 2nd job.

# Further Improvement can be done
    * maven/gradle can be used for automatic build.
    * The two map-reduce jobs can be chained together, so that they can run one after another with a single command.
    * support and/or confidence threshold values can be set dynamically by taking command line argument.
