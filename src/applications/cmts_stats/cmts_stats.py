""" Reads data from Cassandra, generate the stats out of the data and push them to kafka topic """
import sys
import datetime
import yaml
from collections import defaultdict

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import hour, sum
from kafka import KafkaProducer

from util.utils import Utils

def create_spark_sql_context(configuration):
    """Create spark sql context."""
    conf = SparkConf().setAppName(configuration.property("spark.appName"))
    spark_context = SparkContext(conf=conf)
    sql_context = SQLContext(spark_context)
    return sql_context

	
def get_mac_hour_usage_df(sql_context, network_name, table_name, keyspace):
	
    return sql_context.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table_names[0], keyspace=keyspace).load()
		
def get_cm_hour_stats_df(sql_context, network_name, table_name, keyspace):
	
    return sql_context.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table_names[1], keyspace=keyspace).load()
		
def get_mac_hour_join_cm_hour_stats_df(mac_hour_consumption_df, cm_hour_stats_df):
	
    return mac_hour_consumption_df \
        .join(cm_hour_stats_df, (mac_hour_consumption_df["mac"] == cm_hour_stats_df["mac"]) & (mac_hour_consumption_df["hour_stamp"] == cm_hour_stats_df["hour"]), 'left_outer') \
        .drop(cm_hour_stats_df["mac"]) \
        .drop(cm_hour_stats_df["hub"]) \
        .drop(cm_hour_stats_df["market"]) \
        .drop(cm_hour_stats_df["cmts_name"])
		

def get_stats_from_cassandra(configuration, sql_context, network_name, table_name):
    """Read data from cassandra and extract stats from them."""
    keyspace = configuration.property("cassandra.keyspace")

    mac_hour_consumption_df = get_mac_hour_usage_df(sql_context, network_name, table_name, keyspace)

    cm_hour_stats_df = get_cm_hour_stats_df(sql_context, network_name, table_name, keyspace)

    # Joining mac_hour_consumption_df and cm_hour_stats_df
    mac_hour_consumption_df = get_mac_hour_join_cm_hour_stats_df(mac_hour_consumption_df, cm_hour_stats_df)
    
    mac_hour_consumption_df.registerTempTable("mac_hour_consumption_df")
    
    final_df = sql_context.sql("SELECT cmts_name, hour_stamp, market as country, AVG(bytes_down) as average_bytes_down, AVG(bytes_up) as average_bytes_up, AVG(txpower_up) as average_txpower_up, AVG(rxpower_dn) as average_rxpower_dn, AVG(rxpower_up) as average_rxpower_up, AVG(snr_up) as average_snr_up,  AVG(snr_dn) as average_snr_dn FROM mac_hour_consumption_df GROUP BY cmts_name, hour_stamp, market ORDER BY cmts_name, hour_stamp")

    return final_df


def push_stats_to_kafka(configuration, data_frame):
    """Send the stats to kafka output topic."""
    options = configuration.property("kafka")

    producer = KafkaProducer(bootstrap_servers=options["bootstrap.servers"])

    # send each json element to output kafka topic
    for row in data_frame.toJSON().collect():
        producer.send(options["topic.output"], bytes(row))

    producer.close()


def get_curr_date():
    #now = datetime.datetime.now()
    now = datetime.datetime(2017, 10, 17, 0, 0)
    date_string = now.strftime('%Y%m%d')
    return date_string


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    SPARK_SQL_CONTEXT = create_spark_sql_context(configuration)

    network_config = configuration.property("network")
    tables = configuration.property('tables')
    table_dict = defaultdict(list)
    country_code = configuration.property("cassandra.network")

    date_string = get_curr_date()

    for network in network_config[country_code]:
        if network == 'pl':
            table_dict[network].append(tables[0] + "_" + date_string)
            table_dict[network].append(tables[1] + "_" + date_string)
        else:
            table_dict[network].append(tables[0] + "_" + network + "_" + date_string)
            table_dict[network].append(tables[1] + "_" + network + "_" + date_string)

    #print(table_dict)
    #table_test = {'um': ['mac_hour_consumption_rate_um_20171017', 'cm_hour_stats_um_20171017'], 'pl': ['mac_hour_consumption_rate_20171017', 'cm_hour_stats_pl_20171017']}
    for network_name, table_names in table_dict.iteritems():
    #for network_name, table_names in table_test.iteritems():
        final_data_frame = []
        final_data_frame = get_stats_from_cassandra(configuration, SPARK_SQL_CONTEXT, network_name, table_names)
        #print(final_data_frame.count())
        push_stats_to_kafka(configuration, final_data_frame)
