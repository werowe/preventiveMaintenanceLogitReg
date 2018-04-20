# preventiveMaintenanceLogitReg

This reads data and saves a logistic regression model.  The second program then creates data given the mean, stddev, max, and miean of the variables in that training set.  Then the last program runs predictions and prints out those records that are flagged with 1.  So this is a preventive maintenance application.  The data saved in the last step if folder, e.g., /maintenance/2018.04.20.16.26.53.csv/part-00000-b1d37f5f-5021-4368-86fd-d941497d8b52-c000.csv are the vehicles that need maintenance (here I have changed team and provider to me vehicle number and vehicle make, as that makes more sense.)

Data is here https://raw.githubusercontent.com/ludovicbenistant/Management-Analytics/master/Supply%20Chain/Maintenance%20(survival%20analysis)/maintenance_data.csv.

**arguments are:  csv data file from above, where to save LR model**

spark-submit \
  --verbose \
  --class com.bmc.lr.readCSV \
  --master local[*] \
   hdfs://localhost:9000/maintenance/lr-assembly-1.0.jar \
  hdfs://localhost:9000/maintenance/maintenance_data.csv \
  hdfs://localhost:9000/maintenance/maintenance_model
  
  
**argument are:  how many records to create, where is Hadoop core-site.xml, and what data to use to obtain sampling, i.e. data from above**

spark-submit \
  --class com.bmc.lr.generateData \
  --master local[*] \
 hdfs://localhost:9000/maintenance/lr-assembly-1.0.jar \
1000 \
/usr/local/sbin/hadoop-3.1.0/etc/hadoop/core-site.xml \
 hdfs://localhost:9000/maintenance/maintenance_data.csv  
  


**arguments are: what file to read, i.e., the one you just generated, and which model to use**

spark-submit \
  --class com.bmc.lr.makePrediction \
  --master local[*] \
   hdfs://localhost:9000/maintenance/lr-assembly-1.0.jar \
hdfs://localhost:9000/maintenance/2018.04.20.15.48.54.csv \
  hdfs://localhost:9000/maintenance/maintenance_model  
  



