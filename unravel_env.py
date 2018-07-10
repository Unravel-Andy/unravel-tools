"""
v 0.0.1
Output the following ENV variable in ENV.sh
CLUSTER_TYPE: CHD, HDP, MAPR or EMR
HADOOP_VERSION
SPARK_VERSION
HIVE_VERSION
AMBARI_VERSION
"""
import os
import re
from subprocess import Popen, PIPE


def get_cluster_type():
    global cluster_type, hadoop_version_string
    try:
        hadoop_version_string = Popen("hadoop version", stdout=PIPE, shell=True).communicate()[0]
        if re.search('CDH|cdh|cloudera', hadoop_version_string):
            cluster_type = 'CDH'
        elif re.search('HDP|hdp|ambari', hadoop_version_string):
            cluster_type = 'HDP'
        elif re.search('mapr|MAPR', hadoop_version_string):
            cluster_type = 'MAPR'
        else:
            cluster_type = 'UNKNOWN'
    except:
        cluster_type = 'UNKNOWN'
    return cluster_type


def cloudera_host():
    cdh_agent_path = '/etc/cloudera-scm-agent/config.ini'
    if not os.path.exists(cdh_agent_path):
        return 'UNKNOWN'
    agent_ini = open(cdh_agent_path, 'r').read()
    if re.search('(server_host=)(.*)', agent_ini):
        return re.search('(server_host=)(.*)', agent_ini).group(2)
    else:
        return 'UNKNOWN'


def ambari_host():
    hdp_agent_path = '/etc/ambari-agent/conf/ambari-agent.ini'
    if not os.path.exists(hdp_agent_path):
        return 'UNKNOWN'
    agent_ini = open(hdp_agent_path, 'r').read()
    if re.search('(hostname=)(.*)', agent_ini):
        return re.search('(hostname=)(.*)', agent_ini).group(2)
    else:
        return 'UNKNOWN'


def spark_version():
    global SPARK_HOME
    try:
        if cluster_type == 'CDH':
            SPARK_HOME = '/opt/cloudera/parcels/CDH/lib/spark/'
            SPARK2_HOME = '/opt/cloudera/parcels/SPARK2/'
            spark_path = '/opt/cloudera/parcels/CDH/bin/spark-submit'
            spark2_path = '/opt/cloudera/parcels/SPARK2/bin/spark2-submit'
        elif cluster_type == 'HDP':
            SPARK_HOME = '/usr/hdp/current/spark-client/'
            SPARK2_HOME = '/usr/hdp/current/spark2-client/'
            spark_path = '/usr/hdp/current/spark-client/bin/spark-submit'
            spark2_path = '/usr/hdp/current/spark2-client/bin/spark-submit'
        elif cluster_type == 'MAPR':
            get_spark_path = Popen('find /opt/mapr/spark/ -name spark-submit', stdout=PIPE,shell=True).communicate()[0].splitlines()
            spark_path = get_spark_path[0]
            SPARK_HOME = '/'.join(get_spark_path[0].split('/')[:-2])
            spark2_path = '/usr/hdp/current/spark2-client/bin/spark-submit'
            if len(get_spark_path) >= 2:
                spark2_path = get_spark_path[1]
                SPARK2_HOME = '/'.join(get_spark_path[1].split('/')[:-2])
        spark_version = \
            Popen("%s --version 2>&1 | grep -oP '.*?version\s+\K([0-9.]+)'" % spark_path, shell=True, stdout=PIPE).communicate()[0].splitlines()[0]
        if os.path.exists(spark2_path):
            SPARK_HOME += ',' + SPARK2_HOME
            spark_version += ',' + re.search('[2]\.[0-9]\.[0-9]', Popen("%s --version 2>&1" % spark2_path, shell=True, stdout=PIPE).communicate()[0]).group(0)
        return spark_version
    except:
        return None


def hadoop_version():
    try:
        return hadoop_version_string.splitlines()[0].split(' ')[1]
    except:
        return None


def hive_version():
    try:
        return Popen("$(which hive) --version 2>/dev/null | grep -Po 'Hive \K([0-9]+\.[0-9]+\.[0-9]+)'", shell=True,
              stdout=PIPE).communicate()[0].strip()
    except:
        return None


def event_log():
    unravel_properties_path = os.path.join(UNRAVEL_HOME, 'etc/unravel.properties')
    if os.path.exists(unravel_properties_path):
        properties = open(unravel_properties_path, 'r').read()
        if re.search('\s(com.unraveldata.spark.eventlog.location=)(.*)', properties):
            return re.findall('\s(com.unraveldata.spark.eventlog.location=)(.*)', properties)[-1][-1]
        else:
            print('Run setup python script to update spark envent log location')
    else:
        print('unravel.proerties file is missing')
        return None


def unravel_version():
    if os.path.exists(UNRAVEL_HOME):
        unravel_version = open(os.path.join(UNRAVEL_HOME, 'ngui/www/version.txt'), 'r').read()
        unravel_version = re.search('[0-9]{1,2}.[0-9]{1,2}.[0-9]{,2}.[0-9a-zA-Z]{,6}', unravel_version).group(0)
    else:
        unravel_version = 'UNKNOWN'
    return unravel_version


def ambari_version():
    ambari_link = os.readlink('/usr/hdp/current/hadoop-client')
    if re.search('[].[0-9]{1,2}.[0-9]', ambari_link):
        return re.search('[1-3].[0-9].[0-9].[0-9]', ambari_link).group(0)


def cloudera_version():
    cloudera_link = os.readlink('/opt/cloudera/parcels/CDH')
    if re.search('[4-6].[0-9]{1,2}.[0-9]', cloudera_link):
        return re.search('[4-6].[0-9]{1,2}.[0-9]', cloudera_link).group(0)


def mapr_version():
    mapr_rpm = Popen('rpm -qa | grep mapr-core', stdout=PIPE, shell=True).communicate()[0].splitlines()[0]
    if re.search('[5-9]\.[0-9]\.[0-9]]', mapr_rpm):
        return re.search('[5-9]\.[0-9]\.[0-9]]', mapr_rpm).group(0)


def main():
    global UNRAVEL_HOME
    UNRAVEL_HOME = '/usr/local/unravel'
    get_cluster_type()
    ENV = list()
    ENV.append('CLUSTER_TYPE=%s' % cluster_type)
    ENV.append('SPARK_VERSION=%s' % spark_version())
    ENV.append('SPARK_HOME=%s' % SPARK_HOME)
    ENV.append('HIVE_VERSION=%s' % hive_version())
    ENV.append('HADOOP_VERSION=%s' % hadoop_version())
    ENV.append('UNRAVEL_HOME_DIR=%s' %  UNRAVEL_HOME)
    ENV.append('UNRAVEL_SENSOR_DIR=/usr/local/unravel-agent,/usr/local/unravel_client')
    ENV.append('UNRAVEL_VERSION=%s' % unravel_version())
    if cluster_type == 'CDH':
        ENV.append('CLOUDERA_MANAGER_HOST=%s' % cloudera_host())
        ENV.append('CDH_VERSION=%s' % cloudera_version())
    elif cluster_type == 'HDP':
        ENV.append('AMBARI_SERVER_HOST=%s' % ambari_host())
        ENV.append('HDP_VERSION=%s' % ambari_version())
    ENV.append('SPARK_EVENT_LOG=%s' % event_log())
    ENV = '\n'.join(ENV)
    print(ENV)
    open('unravel_env.sh', 'w').write(ENV)


if __name__ == '__main__':
    main()
