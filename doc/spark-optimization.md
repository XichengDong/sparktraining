# Generate data

## Requirements

- Python 2.7 or above to run the scrips.
- To install the python requirements you can run:
```
sudo pip install -r requirements.txt
```

OR install the individual components:

- fake-factory (Used to generate fake data)
```
pip install fake-factory
```
- open_smart (Used to save files to S3):
```
pip install open_smart
```

## generate data using script
- go to folder script/
cd script

- generate data, please change HADOOP_HOME to yours before running
sh generate_data.sh

# Run Spark Code in Spark Shell

```
export YARN_CONF_DIR=/home/hadoop/hadoop-2.7.3/etc/hadoop
cd spark-1.6.2/
bin/spark-shell --master yarn-client
```
copy code in org.training.spark.optimize.AudienceAnalysis

# Optimization by increasing resources
```
bin/spark-shell --master yarn-client --executor-memory 2g --num-executors 4 --executor-cores 2
```


