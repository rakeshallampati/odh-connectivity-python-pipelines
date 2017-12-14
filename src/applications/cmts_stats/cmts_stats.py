""" Reads data from Cassandra, generate the stats out of the data and push them to kafka topic """

import sys
import datetime
from datetime import timedelta
from collections import defaultdict

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from kafka import KafkaProducer

from util.utils import Utils


def create_spark_sql_context(configuration):
    """Create spark sql context."""

    conf = SparkConf().setAppName(configuration.property('spark.appName'
                                                         ))
    spark_context = SparkContext(conf=conf)
    sql_context = SQLContext(spark_context)
    return sql_context


def get_data_from_cassandra(sql_context, table_name, keyspace):
    """reading the date from cassandra"""
    return sql_context.read.format('org.apache.spark.sql.cassandra'
                                   ).options(table=table_name,
                                             keyspace=keyspace).load()


def get_mac_hour_join_cm_hour_stats_df(mac_hour_consumption_df,
                                       cm_hour_stats_df):
    """joining the mac_hour_consumption_df and cm_hour_stats_df"""

    return mac_hour_consumption_df.join(cm_hour_stats_df,
                                        (mac_hour_consumption_df['mac'] == cm_hour_stats_df['mac'])
                                        & (mac_hour_consumption_df['hour']
                                           == cm_hour_stats_df['hour']), 'outer'
                                        ).drop(cm_hour_stats_df['mac']).drop(cm_hour_stats_df['hub']) \
        .drop(cm_hour_stats_df['cmts_name']).drop(cm_hour_stats_df['hour'])


def get_mac_hour_join_attr_df(mac_hour_consumption_df, cm_attr_df):
    """joining the mac_hour_consumption_df and cm_attr_df"""

    return cm_attr_df.join(mac_hour_consumption_df,
                           mac_hour_consumption_df['mac']
                           == cm_attr_df['mac'], 'outer'
                           ).drop(cm_attr_df['mac'])


def get_stats_from_cassandra(
        configuration,
        sql_context,
        table_name,
):
    """Read data from cassandra and extract stats from them."""

    keyspace = configuration.property('cassandra.keyspace')

    mac_hour_consumption_df = get_data_from_cassandra(sql_context,
                                                      table_name[0], keyspace)
    cm_hour_stats_df = get_data_from_cassandra(sql_context,
                                               table_name[3], keyspace)
    try:
        cm_attr_df = get_data_from_cassandra(sql_context,
                                             table_name[2], keyspace)
        mac_hour_consumption_df = \
            get_mac_hour_join_attr_df(mac_hour_consumption_df,
                                      cm_attr_df)
    except:
        cm_attr_df = get_data_from_cassandra(sql_context,
                                             table_name[1], keyspace)
        mac_hour_consumption_df = \
            get_mac_hour_join_attr_df(mac_hour_consumption_df,
                                      cm_attr_df)

    # Joining mac_hour_consumption_df and cm_hour_stats_df

    mac_hour_consumption_df = \
        get_mac_hour_join_cm_hour_stats_df(mac_hour_consumption_df,
                                           cm_hour_stats_df)

    mac_hour_consumption_df.registerTempTable('mac_hour_consumption_df')

    final_df = \
        sql_context.sql('SELECT cmts_name, hour, market as country, AVG(kb_down) as average_bytes_down,'
                        ' AVG(kb_up) as average_bytes_up, AVG(txpower_up) as average_txpower_up,'
                        ' AVG(rxpower_dn) as average_rxpower_dn, AVG(rxpower_up) as average_rxpower_up,'
                        ' AVG(snr_up) as average_snr_up,  AVG(snr_dn) as average_snr_dn FROM mac_hour_consumption_df'
                        ' GROUP BY cmts_name, hour, market ORDER BY cmts_name, hour'
                        )

    return final_df


def push_stats_to_kafka(configuration, data_frame):
    """Send the stats to kafka output topic."""

    options = configuration.property('kafka')

    producer = \
        KafkaProducer(bootstrap_servers=options['bootstrap.servers'])

    # send each json element to output kafka topic

    for row in data_frame.toJSON().collect():
        producer.send(options['topic.output'], bytes(row))

    producer.close()


def get_curr_date():
    """to get the date """
    now = datetime.datetime.now() - timedelta(days=1)
    date_string = now.strftime('%Y%m%d')
    return date_string


if __name__ == '__main__':
    configuration = Utils.load_config(sys.argv[:])
    SPARK_SQL_CONTEXT = create_spark_sql_context(configuration)

    network_config = configuration.property('network')
    tables = configuration.property('tables')
    table_dict = defaultdict(list)
    country_code = configuration.property('cassandra.network')

    date_string = get_curr_date()

    for network in network_config[country_code]:
        if network == 'pl':
            table_dict[network].append(tables[0] + '_' + date_string)
            table_dict[network].append(tables[2] + '_' + date_string
                                       + '_morning')
            table_dict[network].append(tables[2] + '_' + date_string
                                       + '_afternoon')
        else:
            table_dict[network].append(tables[0] + '_' + network + '_'
                                       + date_string)
            table_dict[network].append(tables[2] + '_' + date_string
                                       + '_morning_' + network)
            table_dict[network].append(tables[2] + '_' + date_string
                                       + '_afternoon_' + network)
        table_dict[network].append(tables[1] + '_' + network + '_'
                                   + date_string)

    for (network_name, table_names) in table_dict.iteritems():
        final_data_frame = []
        final_data_frame = get_stats_from_cassandra(configuration,
                                                    SPARK_SQL_CONTEXT, table_names)
        country = final_data_frame.select('country'
                                          ).dropna().distinct().collect()
        final_data_frame = final_data_frame.fillna(country[0].country,
                                                   subset=['country'])
        push_stats_to_kafka(configuration, final_data_frame)
