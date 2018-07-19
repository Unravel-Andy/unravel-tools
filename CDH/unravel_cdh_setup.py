#!/usr/bin/python
"""
v 1.0.7
"""
import os
import re
import json
import base64
import urllib2
import argparse
from time import sleep
from subprocess import Popen, PIPE

try:
    unravel_hostname = Popen(['hostname'], stdout=PIPE).communicate()[0].strip()
except:
    unravel_hostname = None

cloudera_agent_conf_path = '/etc/cloudera-scm-agent/config.ini'
try:
    ca_conf = open(cloudera_agent_conf_path, 'r').read()
    cm_host = re.search('server_host=.*', ca_conf).group(0).split('=')[1]
except:
    cm_host = None

parser = argparse.ArgumentParser()
parser.add_argument("--spark-version", help="spark version e.g. 1.6.3 or 2.1.0", dest='spark_ver', required=True)
if unravel_hostname:
    parser.add_argument("--unravel-server", help="Unravel Server hostname/IP", dest='unravel', default=unravel_hostname)
else:
    parser.add_argument("--unravel-server", help="Unravel Server hostname/IP", dest='unravel', required=True)
if cm_host:
    parser.add_argument("--cm-server", help="Cloudera Manager Server hostname/IP", dest='cm', default=cm_host)
else:
    parser.add_argument("--cm-server", help="Cloudera Manager Server hostname/IP", dest='cm', required=True)
parser.add_argument("--cm-user", help="Cloudera Manager Server Login username", dest='cm_user', default='admin')
parser.add_argument("--cm-password", help="Cloudera Manager Server Login password", dest='cm_pass', default='admin')
parser.add_argument("--dry-run", help="Only Test but will not update anything", dest='dry_test', action='store_true')
parser.add_argument("-v", "--verbose", help="print current and suggess configuration", action='store_true')
parser.add_argument("--sensor-only", help="check/upgrade Unravel Sensor Only", dest='sensor_only', action='store_true')
parser.add_argument("--cluster-name", help="Cloudera Cluster Name default is the first cluster")
parser.add_argument("--restart-cm", help="Restart Cloudera manager stale services", dest='restart_cm', action='store_true')
argv = parser.parse_args()

sleep_time = 0.5
argv.unravel_ip = Popen(['hostname', '-i'], stdout=PIPE).communicate()[0].strip()
if len(argv.unravel.split(':')) == 2:
    argv.unravel_port = argv.unravel.split(':')[1]
    argv.unravel = argv.unravel.split(':')[0]
else:
    argv.unravel_port = 3000


class CDHSetup():
    def __init__(self):
        self.api_url = 'http://%s:7180/api/v11' % argv.cm
        self.api_clusters_url = self.api_url + '/clusters'
        self.api_cm_url = self.api_url + '/cm'
        self.spark_ver_xy = None
        self.spark2_ver_xy = None
        self.cm_cred = base64.b64encode('%s:%s' % (argv.cm_user, argv.cm_pass))
        self.hosts_list = self.get_hosts_list()
        if argv.cluster_name:
            self.cluster_name = argv.cluster_name
            self.cdh_ver_xyz = self.get_requests(self.api_clusters_url)['items'][0]['fullVersion']
        else:
            self.cluster_name = self.get_requests(self.api_clusters_url)['items'][0]['name']
            self.cdh_ver_xyz = self.get_requests(self.api_clusters_url + '/%s' % self.cluster_name)['fullVersion']
        self.hs = self.get_hive_server2_configs()
        self.yarn_gw = self.get_yarn_gw_configs()
        self.has_spark = False
        self.has_spark2 = False
        if re.search('1.[6-9]', argv.spark_ver):
            self.has_spark = True
            self.spark_gw_configs = self.get_spark_gw_configs()
            self.spark_configs = self.get_spark_configs()
        if re.search('2.[0-9]', argv.spark_ver):
            self.has_spark2 = True
            self.spark2_gw_configs = self.get_spark_gw_configs(spark_ver_x=2)
            self.spark2_configs = self.get_spark_configs(spark_ver_x=2)
        self.configs = self.generate_configs(argv.unravel)
        self.configs_ip = self.generate_configs(argv.unravel_ip)
        self.parcel_api_url = '%s/%s/parcels' % (self.api_clusters_url, self.cluster_name)
        self.parcel_installed = False

    def check_parcel_url(self):
        print('\nChecking Parcel configuration')
        sleep(1)
        cdh_ver_xy = 'cdh%s.%s' % (self.cdh_ver_xyz.split('.')[0], self.cdh_ver_xyz.split('.')[1])
        unravel_parcel_url = 'http://%s:%s/parcels/%s' % (argv.unravel, argv.unravel_port, cdh_ver_xy)
        cm_configs = self.get_requests('%s/config' % self.api_cm_url)['items']

        parcel_api_name = 'REMOTE_PARCEL_REPO_URLS'
        parcel_api_val = ''
        for config in cm_configs:
            if config['name'] == parcel_api_name:
                if not unravel_parcel_url in config['value']:
                    print('Adding Unravel Parcel Url in Cloudera Manager')
                    parcel_api_val = config['value'] + ',' + unravel_parcel_url
                    self.put_requests('%s/config' % self.api_cm_url, parcel_api_name, parcel_api_val)

    def generate_configs(self, unravel_host):
        configs = {}
        configs['hive_client_config_safety_valve'] = {
            'hive.exec.driver.run.hooks': 'com.unraveldata.dataflow.hive.hook.HiveDriverHook',
            'com.unraveldata.hive.hdfs.dir': '/user/unravel/HOOK_RESULT_DIR',
            'com.unraveldata.hive.hook.tcp': 'true',
            'com.unraveldata.host': unravel_host,
            'hive.exec.pre.hooks': 'com.unraveldata.dataflow.hive.hook.HivePreHook',
            'hive.exec.post.hooks': 'com.unraveldata.dataflow.hive.hook.HivePostHook',
            'hive.exec.failure.hooks': 'com.unraveldata.dataflow.hive.hook.HiveFailHook'
        }
        configs['hive_client_env_safety_valve'] = 'AUX_CLASSPATH=${AUX_CLASSPATH}:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/unravel_hive_hook.jar'
        if self.has_spark:
            self.spark_ver_xy = re.search('1.[6-9]', argv.spark_ver).group(0).split('.')
            configs['spark-conf/spark-defaults.conf_client_config_safety_valve'] = {
                'spark.unravel.server.hostport': '%s:4043' % unravel_host,
                'spark.driver.extraJavaOptions': '-javaagent:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/btrace-agent.jar=config=driver,libs=spark-1.6',
                'spark.executor.extraJavaOptions': '-javaagent:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/btrace-agent.jar=config=executor,libs=spark-1.6'
            }
        if self.has_spark2:
            self.spark2_ver_xy = re.search('2.[0-9]', argv.spark_ver).group(0).split('.')
            configs['spark2-conf/spark-defaults.conf_client_config_safety_valve'] = {
                'spark.unravel.server.hostport': '%s:4043' % unravel_host,
                'spark.driver.extraJavaOptions': '-javaagent:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/btrace-agent.jar=config=driver,libs=spark-%s' % re.search('2.[0-9]', argv.spark_ver).group(0),
                'spark.executor.extraJavaOptions': '-javaagent:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/btrace-agent.jar=config=executor,libs=spark-%s' % re.search('2.[0-9]', argv.spark_ver).group(0)
            }
        configs['yarn_app_mapreduce_am_command_opts'] = '-javaagent:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/btrace-agent.jar=libs=mr -Dunravel.server.hostport=%s:4043' % unravel_host
        configs['mapreduce_client_config_safety_valve'] = {
            'mapreduce.task.profile': 'true',
            'mapreduce.task.profile.maps': '0-5',
            'mapreduce.task.profile.reduces': '0-5',
            'mapreduce.task.profile.params': '-javaagent:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/btrace-agent.jar=libs=mr -Dunravel.server.hostport=%s:4043' % unravel_host,
        }
        configs['mapreduce_client_env_safety_valve'] = 'HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/unravel_hive_hook.jar'
        configs['unravel-properties'] = {
            "com.unraveldata.job.collector.done.log.base": "/user/history/done",
            "com.unraveldata.job.collector.log.aggregation.base": "/app-logs/*/logs/",
            "com.unraveldata.spark.eventlog.location": "hdfs:///user/spark/applicationHistory"
        }
        return configs

    def generate_xml_property(self, prop_name, prop_val):
        return """<property><name>%s</name><value>%s</value></property>
        """ % (prop_name, prop_val)

    def generate_regex(self, config_name, config_val='[\-0-9]{1,}|true|false|.*'):
        property_regex = '<property>\s{0,}'
        property_regex += '<name>%s<\/name>\s{0,}' % config_name
        property_regex += '<value>(%s)<\/value>\s{0,}' % config_val
        property_regex += '<\/property>'
        return property_regex

    def get_requests(self, request_url):
        request = urllib2.Request(request_url)
        request.add_header('Authorization', 'Basic %s' % self.cm_cred)
        try:
            return json.loads(urllib2.urlopen(request).read())
        except urllib2.URLError as e:
            print(e)
            if 'Name or service not known' in str(e):
                exit(1)
        except Exception as e:
            print(e)
            return {'items': []}

    def get_sensor_stat(self):
        unravel_sensor_ver = ''
        sensor_stage = ''
        for parcel in self.get_requests(self.parcel_api_url).get('items', ['None']):
            if parcel['product'] == 'UNRAVEL_SENSOR':
                if unravel_sensor_ver and parcel['version'].split('_')[0] > unravel_sensor_ver.split('_')[0]:
                    unravel_sensor_ver = parcel['version']
                    sensor_stage = parcel['stage']
                elif not unravel_sensor_ver:
                    unravel_sensor_ver = parcel['version']
                    sensor_stage = parcel['stage']
        return (unravel_sensor_ver, sensor_stage)

    # PUT request to update configuration back to Cloudera Manager
    def put_requests(self, request_url, conf_name, conf_val):
        data_format = """{"items": [{"name": "%s", "value": %s}]}""" % (conf_name, json.dumps(conf_val))
        request = urllib2.Request(request_url, data=data_format)
        request.add_header('Content-Type', 'application/json')
        request.add_header('Authorization', 'Basic %s' % self.cm_cred)
        request.get_method = lambda: 'PUT'
        try:
            return json.loads(urllib2.urlopen(request).read())
        except urllib2.URLError as e:
            print(e)
            # exit(1)
        except Exception as e:
            print(e)
            return {'items': []}

    # POST request to run cloudera manager commands
    def post_requests(self, request_url, data=None):
        request = urllib2.Request(request_url, data=data)
        request.add_header('Authorization', 'Basic %s' % self.cm_cred)
        if data:
            request.add_header('Content-type', 'application/json')
        request.get_method = lambda: 'POST'
        try:
            return json.loads(urllib2.urlopen(request).read())
        except urllib2.URLError as e:
            print(e)
            # exit(1)
        except Exception as e:
            print(e)
            return {'items': []}

    # Get hive gateway and hiveserver2 full role name and configs e.g hive-GATEWAY-BASE
    # Return dict(hs2, hive-gw) hs2[0]: full url to hiveserver2 role config, hs[1]: hiveserver2 configurations
    def get_hive_server2_configs(self):
        try:
            request_url = self.api_clusters_url + '/%s/services/hive/roleConfigGroups' % self.cluster_name
            res = self.get_requests(request_url)['items']
            for services in res:
                if services.get('roleType', None) == 'HIVESERVER2':
                    hs2_url = '%s/%s/config' % (request_url, services['name'])
                    hs2_config = services['config']['items']
                elif services.get('roleType', None) == 'GATEWAY':
                    hg_url = '%s/%s/config' % (request_url, services['name'])
                    hg_config = services['config']['items']
            return {'hs2': [hs2_url, hs2_config], 'hive-gw': [hg_url, hg_config]}
        except:
            print('Failed to get hive server2 config')

    # Return a list of hosts info
    def get_hosts_list(self):
        try:
            request_url = self.api_url + '/hosts'
            res = self.get_requests(request_url)['items']
            return res
        except:
            print('Failed to get host list')

    # Get yarn gateway full role name and configs  e.g yarn-GATEWAY-BASE
    # Return dict(yarn-gw) yarn-gw[0]: full url to yarn gateway role config, yarn-gw[1]: yarn gateway role configurations
    def get_yarn_gw_configs(self):
        try:
            request_url = self.api_clusters_url + '/%s/services/yarn/roleConfigGroups' % self.cluster_name
            res = self.get_requests(request_url)['items']
            for services in res:
                if services.get('roleType', None) == 'GATEWAY':
                    yarn_url = '%s/%s/config' % (request_url, services['name'])
                    yarn_gw_config = self.get_requests(yarn_url + '?view=full')['items']
                    return {'yarn-gw': [yarn_url, yarn_gw_config]}
        except:
            print('Failed to get yarn gateway config')

    # Get remote log dir for com.unraveldata.job.collector.log.aggregation.base
    def get_remote_log(self):
        try:
            request_url = self.api_clusters_url + '/%s/services/yarn/roleConfigGroups' % self.cluster_name
            res = self.get_requests(request_url)['items']
            for service in res:
                if service.get('roleType', None) == 'NODEMANAGER':
                    yarn_nm_url = '%s/%s/config' % (request_url, service['name'])
                    yarn_nm_config = self.get_requests(yarn_nm_url + '?view=full')['items']
                    for config in yarn_nm_config:
                        if config.get('relatedName', 'None') == 'yarn.nodemanager.remote-app-log-dir':
                            log_dir = config.get('value', '/tmp/logs')
                        elif config.get('relatedName', 'None') == 'yarn.nodemanager.remote-app-log-dir-suffix':
                            log_suffix = config.get('value', 'logs')
            return '%s/*/%s' % (log_dir, log_suffix)
        except:
            return '/tmp/logs/*/logs'

    # Get spark_on_yarn gateway full role name and configs e.g spark_on_yarn-GATEWAY-BASE
    # Input: for spark 1.X no input, for spark 2.X spark_ver_x=2
    # Return dict(spark-gw) spark-gw[0]: full url to spark_on_yarn gateway role config,
    def get_spark_gw_configs(self, spark_ver_x=''):
        try:
            if not spark_ver_x == 2:
                spark_ver_x = ''
            request_url = self.api_clusters_url + '/%s/services/spark%s_on_yarn/roleConfigGroups' % (self.cluster_name, spark_ver_x)
            res = self.get_requests(request_url)['items']
            for services in res:
                if services.get('roleType', None) == 'GATEWAY':
                    spark_gw_url = '%s/%s/config' % (request_url, services['name'])
                    spark_gw_config = services['config']['items']
                    return {'spark-gw': [spark_gw_url, spark_gw_config]}
        except:
            print('Failed to get spark%s gateway config' % spark_ver_x)

    # Get spark.eventLog.dir and update com.unraveldata.spark.eventlog.location in unravel.properties
    def get_spark_configs(self, spark_ver_x=''):
        try:
            if not spark_ver_x == 2:
                spark_ver_x = ''
            request_url = self.api_clusters_url + '/%s/services/spark%s_on_yarn/config?view=full' % (
            self.cluster_name, spark_ver_x)
            res = self.get_requests(request_url)['items']
            return res
        except:
            print('Failed to get spark%s config' % spark_ver_x)

    # Get Oozie server address that will insert into unravel.properties
    def get_oozie_server_hostname(self):
        request_url = '%s/%s/services/oozie/roleConfigGroups' % (self.api_clusters_url, self.cluster_name)
        res = self.get_requests(request_url)['items']
        for service in res:
            if service.get('roleType', 'None') == 'OOZIE_SERVER':
                oozie_role_url = request_url + '/%s/roles' % service.get('name', 'None')
                oozie_hostId = self.get_requests(oozie_role_url)['items'][0]['hostRef']['hostId']
                for config in self.get_requests(request_url + '/%s/config?view=full' % service['name'])['items']:
                    if config['name'] == 'oozie_http_port':
                        oozie_port = config.get('value', '11000')
        for host in self.hosts_list:
            if host['hostId'] == oozie_hostId:
                return host['hostname'], oozie_port
        return None, None

    # Gather the information to update unravel.properties
    # Oozie, Hive metastore, Spark eventLog dir
    def prepare_unravel_properties(self):
        unravel_spark_log = 'com.unraveldata.spark.eventlog.location'
        self.configs['unravel-properties']['com.unraveldata.job.collector.done.log.base'] = '/user/history/done'
        self.configs['unravel-properties']['com.unraveldata.job.collector.log.aggregation.base'] = self.get_remote_log()

        try:
            if self.has_spark:
                for item in self.spark_configs:
                    if item['relatedName'] == 'spark.eventLog.dir':
                        spark_eventlog = item.get('value', '/user/spark/sparkApplicationHistory')
                if not spark_eventlog in self.configs['unravel-properties']['com.unraveldata.spark.eventlog.location']:
                    self.configs['unravel-properties']['com.unraveldata.spark.eventlog.location'] = 'hdfs://' + spark_eventlog
            if self.has_spark2:
                for item in self.spark2_configs:
                    if item['relatedName'] == 'spark.eventLog.dir':
                        spark2_eventlog = item.get('value', '/user/spark/spark2ApplicationHistory')
                if not spark2_eventlog in self.configs['unravel-properties'][unravel_spark_log]:
                    if self.has_spark:
                        self.configs['unravel-properties'][unravel_spark_log] = self.configs['unravel-properties'][unravel_spark_log] + ',hdfs://' + spark2_eventlog
                    else:
                        self.configs['unravel-properties'][unravel_spark_log] = 'hdfs://' + spark2_eventlog
        except:
            pass

        # hive metastore config
        try:
            request_url = self.api_clusters_url + '/%s/services/hive/config' % self.cluster_name
            hive_configs = self.get_requests(request_url).get('items', 'None')
            if hive_configs != 'None':
                for config in hive_configs:
                    if config['name'] == 'hive_metastore_database_host': hive_host = config['value']

                    if config['name'] == 'hive_metastore_database_name': hive_db = config['value']

                    if config['name'] == 'hive_metastore_database_password': hive_pass = config['value']

                    if config['name'] == 'hive_metastore_database_port': hive_port = config['value']

                    if config['name'] == 'hive_metastore_database_type': hive_db_type = config['value']

            if hive_db_type == 'mysql':
                hive_driver = 'com.mysql.jdbc.Driver'
            else:
                hive_driver = 'org.postgresql.Driver'

            self.configs['unravel-properties']['javax.jdo.option.ConnectionURL'] = 'jdbc:%s://%s:%s/%s' % (hive_db_type, hive_host, hive_port, hive_db)
            self.configs['unravel-properties']['javax.jdo.option.ConnectionDriverName'] = hive_driver
            self.configs['unravel-properties']['javax.jdo.option.ConnectionUserName'] = 'hive'
            self.configs['unravel-properties']['javax.jdo.option.ConnectionPassword'] = hive_pass
        except:
            pass

        # Oozie server config
        try:
            oozie_hostname, oozie_port = self.get_oozie_server_hostname()
            if oozie_hostname and oozie_port:
                self.configs['unravel-properties']['oozie.server.url'] = 'http://%s:%s/oozie' % (oozie_hostname, oozie_port)
        except:
            pass

    # Consists of 3 parts:
    # hive-env: hive_client_env_safety_valve
    # hive-site: hive_client_config_safety_valve
    # hiveserver2 hive-site: hive_hs2_config_safety_valve
    def update_hive(self):
        print('\nChecking hive-env Configuration')
        self.update_hive_env()

        print('\nChecking hive-site Configuration')
        self.update_hive_site()

        print('\nChecking hiveserver2 Configuration')
        self.update_hs2_site()

    def update_hive_env(self):
        # check and update AUX_CLASSPATH in hive_client_env_safety_valve (hive-env.sh)
        try:
            hive_env_api_name = 'hive_client_env_safety_valve'
            hive_env_val = self.configs[hive_env_api_name]
            find_property = False
            cur_config_val = ''
            for cur_config in self.hs['hive-gw'][1]:
                cur_config_val = cur_config.get('value', 'None')
                cur_config_name = cur_config.get('name', 'None')
                if hive_env_val in cur_config_val and cur_config_name == hive_env_api_name:
                    print_format('AUTH_CLASSPATH', print_green("No change needed"))
                    if argv.verbose:
                        print_verbose(cur_config_val)
                    find_property = True
            if not find_property:
                print_format('AUTH_CLASSPATH', print_red("missing"))
                if not argv.dry_test:
                    print('Updating AUTH_CLASSPATH')
                    self.put_requests(self.hs['hive-gw'][0], hive_env_api_name, cur_config_val + hive_env_val)
                    sleep(1)
                    print('Update Successful!')
        except Exception as e:
            print(e)

    def update_hive_site(self):
        # check and update hive_client_config_safety_valve (hive-site.xml)
        try:
            hive_api_name = 'hive_client_config_safety_valve'
            hive_site_val = self.configs[hive_api_name]
            new_config = ''
            for config_name, config_val in hive_site_val.iteritems():
                config_ip_val = self.configs_ip[hive_api_name][config_name]
                find_property = False
                for cur_config in self.hs['hive-gw'][1]:
                    cur_config_val = cur_config.get('value', 'None')
                    cur_config_name = cur_config.get('name', 'None')
                    if (config_val in cur_config_val or config_ip_val in cur_config_val) and cur_config_name == hive_api_name:
                        print_format(config_name, print_green("No change needed"))
                        if argv.verbose:
                            print_verbose(config_val)
                        find_property = True
                    elif cur_config_name == hive_api_name:
                        if not new_config: new_config += cur_config_val
                if not find_property:
                    print_format(config_name, print_red("missing"))
                    new_config += self.generate_xml_property(config_name, config_val)
                    if argv.verbose: print_verbose('None', config_val)
                sleep(sleep_time)
            if new_config and not argv.dry_test:
                print('Updating hive-site configuration')
                self.put_requests(self.hs['hive-gw'][0], hive_api_name, new_config)
                sleep(1)
                print('Update Successful!')
        except Exception as e:
            print(e)

    def update_hs2_site(self):
        # check and update hive_hs2_config_safety_valve (hive-site.xml)
        try:
            hs2_api_name = 'hive_hs2_config_safety_valve'
            hs2_val = self.configs['hive_client_config_safety_valve']
            new_config = ''
            for config_name, config_val in hs2_val.iteritems():
                config_ip_val = self.configs_ip['hive_client_config_safety_valve'][config_name]
                find_property = False
                for cur_config in self.hs['hs2'][1]:
                    cur_config_val = cur_config.get('value', 'None')
                    cur_config_name = cur_config.get('name', 'None')
                    if (config_val in cur_config_val or config_ip_val in cur_config_val) and cur_config_name == hs2_api_name:
                        print_format(config_name, print_green("No change needed"))
                        if argv.verbose: print_verbose(config_val)
                        find_property = True
                    elif cur_config_name == hs2_api_name:
                        if not new_config: new_config += cur_config_val
                if not find_property:
                    print_format(config_name, print_red("missing"))
                    new_config += self.generate_xml_property(config_name, config_val)
                    if argv.verbose: print_verbose('None', config_val)
                sleep(sleep_time)
            if new_config and not argv.dry_test:
                print('Updating hiveserver2 configuration')
                self.put_requests(self.hs['hs2'][0], hs2_api_name, new_config)
                sleep(1)
                print('Update Successful!')
        except Exception as e:
            print(e)

    def update_spark(self, spark_ver_x=''):
        # Check spark-conf/spark-defaults.conf_client_config_safety_valve configuration and push back to cloudera
        try:
            print('\nChecking spark%s-defaults configurations' % str(spark_ver_x))
            spark_api_name = 'spark%s-conf/spark-defaults.conf_client_config_safety_valve' % str(spark_ver_x)
            spark_val = self.configs[spark_api_name]
            new_config = ''
            if spark_ver_x == 2:
                spark_ver_xy = '.'.join(self.spark2_ver_xy)
                spark_gw_configs = self.spark2_gw_configs
            else:
                spark_ver_xy = '1.6'
                spark_gw_configs = self.spark_gw_configs

            for config_name, config_val in spark_val.iteritems():
                config_ip_val = self.configs_ip[spark_api_name][config_name]
                find_property = False
                config_regex = config_name + '.*'
                for cur_config in spark_gw_configs['spark-gw'][1]:
                    cur_config_val = cur_config.get('value', 'None')
                    cur_config_name = cur_config.get('name', 'None')
                    if config_val in cur_config_val or config_ip_val in cur_config_val or re.search('%s=.*(%s).*' % (config_name, spark_ver_xy), cur_config_val):
                        print_format(config_name, print_green("No change needed"))
                        if argv.verbose: print_verbose(re.search(config_regex, cur_config_val).group(0))
                        find_property = True
                    elif cur_config_name == spark_api_name:
                        if not new_config: new_config += cur_config_val
                if not find_property:
                    if re.search(config_regex, new_config):
                        print_format(config_name, print_red("Missing value"))
                        if argv.verbose: print_verbose(re.search(config_regex, cur_config_val).group(0), config_val)
                        new_config = re.sub(config_name + '.*', '%s=%s' % (config_name, config_val), new_config)
                    else:
                        print_format(config_name, print_red("missing"))
                        if argv.verbose: print_verbose('None', config_val)
                        new_config += '%s=%s\n' % (config_name, config_val)
                sleep(sleep_time)
            if new_config and not argv.dry_test:
                print('Updating Spark%s configuration' % str(spark_ver_x))
                if spark_ver_x == 2:
                    self.put_requests(self.spark2_gw_configs['spark-gw'][0], spark_api_name, new_config)
                else:
                    self.put_requests(self.spark_gw_configs['spark-gw'][0], spark_api_name, new_config)
                sleep(1)
                print('Update Successful!')
        except Exception as e:
            print(e)

    def update_yarn(self):
        print('\nChecking hadoop-env configuration')
        self.update_hadoop_env()

        print('\nChecking mapred-site configuration')
        self.update_mapred()

        print('\nChecking ApplicationMaster Java Opts Base configuration')
        self.update_javaopts()

    def update_hadoop_env(self):
        try:
            hadoop_env_api_name = 'mapreduce_client_env_safety_valve'
            hadoop_env_val = self.configs[hadoop_env_api_name]
            find_property = False
            for cur_config in self.yarn_gw['yarn-gw'][1]:
                cur_config_val = cur_config.get('value', 'None')
                cur_config_name = cur_config.get('name', 'None')
                if hadoop_env_val in cur_config_val and cur_config_name == hadoop_env_api_name:
                    print_format(cur_config_name, print_green('No change needed'))
                    if argv.verbose: print_verbose(cur_config_val)
                    find_property = True
                    break
                elif cur_config_name == hadoop_env_api_name:
                    if cur_config_val != 'None':
                        cur_config_val += ' ' + hadoop_env_val
                    else:
                        cur_config_val = hadoop_env_val
                    break
            if not find_property:
                print_format(cur_config_name, print_red('Missing value'))
                if not argv.dry_test:
                    print('Updating hadoop-env configuration')
                    self.put_requests(self.yarn_gw['yarn-gw'][0], hadoop_env_api_name, cur_config_val)
                    sleep(1)
                    print('Update Successful!')
        except Exception as e:
            print(e)

    def update_mapred(self):
        try:
            mapred_api_name = 'mapreduce_client_config_safety_valve'
            mapred_val = self.configs[mapred_api_name]
            new_config = ''
            for config_name, config_val in mapred_val.iteritems():
                config_ip_val = self.configs_ip[mapred_api_name][config_name]
                property_ip_regex = self.generate_regex(config_name, '.*' + config_ip_val + '.*')
                property_regex = self.generate_regex(config_name, '.*' + config_val + '.*')
                property_regex_raw = self.generate_regex(config_name)
                find_property = False
                for cur_config in self.yarn_gw['yarn-gw'][1]:
                    cur_config_val = cur_config.get('value', 'None')
                    cur_config_name = cur_config.get('name', 'None')
                    if cur_config_name == mapred_api_name and (re.search(property_regex, cur_config_val) or re.search(property_ip_regex, cur_config_val)):
                        print_format(config_name, print_green("No change needed"))
                        if argv.verbose: print_verbose(re.search(property_regex_raw, cur_config_val).group(1))
                        find_property = True
                    elif cur_config_name == mapred_api_name:
                            if not new_config: new_config += cur_config_val
                if not find_property:
                    new_property = self.generate_xml_property(config_name, config_val)
                    print_format(config_name, print_red("missing"))
                    if '<name>' + config_name + '</name>' in new_config:
                        cur_val = re.search(property_regex_raw, new_config).group(1)
                        if config_name == 'mapreduce.task.profile.params':
                            new_property = self.generate_xml_property(config_name, cur_val + ' ' + config_val)
                            if argv.verbose: print_verbose(cur_val, cur_val + ' ' + config_val)
                        else:
                            new_property = self.generate_xml_property(config_name, config_val)
                            if argv.verbose: print_verbose(cur_val, config_val)
                        new_config = re.sub(property_regex_raw, new_property, new_config)
                    else:
                        new_config += new_property
                        if argv.verbose: print_verbose(cur_config_val, config_val)
                sleep(sleep_time)
            if new_config and not argv.dry_test:
                print('Updating mapred-site configuration')
                self.put_requests(self.yarn_gw['yarn-gw'][0], mapred_api_name, new_config)
                sleep(1)
                print('Update Successful!')
        except Exception as e:
            print(e)

    def update_javaopts(self):
        try:
            javaopts_api_name = 'yarn_app_mapreduce_am_command_opts'
            javaopts_val = self.configs[javaopts_api_name]
            javaopts_ip_val = self.configs_ip[javaopts_api_name]
            find_property = False
            for cur_config in self.yarn_gw['yarn-gw'][1]:
                cur_config_val = cur_config.get('value', 'None')
                cur_config_name = cur_config.get('name', 'None')
                if (javaopts_val in cur_config_val or javaopts_ip_val in cur_config_val) and cur_config_name == javaopts_api_name:
                    print_format(cur_config_name, print_green("No change needed"))
                    if argv.verbose: print_verbose(cur_config_val)
                    find_property = True
                    break
                elif cur_config_name == javaopts_api_name:
                    break
            if not find_property:
                print_format(javaopts_api_name, print_red("Missing value"))
                if cur_config_val == 'None':
                    javaopts_val = cur_config['default'] + ' ' + javaopts_val
                else:
                    javaopts_val = cur_config_val + ' ' + javaopts_val
                if argv.verbose: print_verbose(cur_config_val, javaopts_val)
                if not argv.dry_test:
                    print('Updating javaopt in Yarn')
                    self.put_requests(self.yarn_gw['yarn-gw'][0], javaopts_api_name, javaopts_val)
                    sleep(1)
                    print('Update Successful!')
        except Exception as e:
            print(e)

    def update_unravel_properties(self):
        print("\nChecking Unravel properties")
        unravel_properties_path = '/usr/local/unravel/etc/unravel.properties'
        self.prepare_unravel_properties()
        headers = "# CDH Setup\n"
        new_config = ''

        if os.path.exists(unravel_properties_path):
            try:
                with open(unravel_properties_path, 'r') as f:
                    unravel_properties = f.read()
                    f.close()
                for config, val in self.configs['unravel-properties'].iteritems():
                    find_configs = re.findall('\s' + config + '.*', unravel_properties)
                    if find_configs:
                        correct_flag = False
                        for cur_config in find_configs:
                            if val in cur_config:
                                correct_flag = True
                            else:
                                correct_flag = False
                        if not correct_flag:
                            print_format(config, print_red("Missing value"))
                            new_config += '%s=%s\n' % (config, val)
                            if argv.verbose:
                                print_verbose(cur_config.strip(), config + '=' + val)
                        else:
                            print_format(config, print_green("No change needed"))
                            if argv.verbose:
                                print_verbose(config + '=' + val)
                    else:
                        print_format(config, print_red("missing"))
                        new_config += '%s=%s\n' % (config, val)
                        if argv.verbose:
                            print_verbose(None, config + '=' + val)
                if len(new_config.split('\n')) > 1 and not argv.dry_test:
                    print('Updating Unravel Properties')
                    with open(unravel_properties_path, 'a') as f:
                        f.write(headers + new_config)
                        f.close()
                    sleep(3)
                    print('Update Successful!')
            except Exception as e:
                print(e)
                print('skip update unravel.properties')
        else:
            print("unravel.properties not found skip update unravel.properties")

    def install_parcels(self):
        try:
            self.check_parcel_url()
            sleep(10)
            sensor_ver, sensor_stage = self.get_sensor_stat()
            parcel_commands_url = '%s/products/UNRAVEL_SENSOR/versions/%s/commands' % (self.parcel_api_url, sensor_ver)
            if not argv.dry_test:
                if sensor_stage == 'AVAILABLE_REMOTELY':
                    print('Downloading Unravel Sensor Parcel')
                    self.post_requests('%s/startDownload' % parcel_commands_url)
                    while sensor_stage != 'DOWNLOADED':
                        sensor_ver, sensor_stage = self.get_sensor_stat()
                        sleep(2)
                if sensor_stage == 'DOWNLOADED':
                    print('Distributing Unravel Sensor Parcel')
                    self.post_requests('%s/startDistribution' % parcel_commands_url)
                    while sensor_stage != 'DISTRIBUTED':
                        sensor_ver, sensor_stage = self.get_sensor_stat()
                        sleep(2)
                if sensor_stage == 'DISTRIBUTED':
                    print('Activating Unravel Sensor Parcel')
                    self.post_requests('%s/activate' % parcel_commands_url)
                    sensor_ver, sensor_stage = self.get_sensor_stat()
                    while sensor_stage == 'ACTIVATING':
                        sensor_ver, sensor_stage = self.get_sensor_stat()
                        sleep(2)
                if sensor_stage == 'ACTIVATED':
                    print('Unravel Sensor Parcel Activated')
                    self.parcel_installed = True
            else:
                print('UNRAVEL_SENSOR %s %s' % (sensor_ver, sensor_stage))
                self.parcel_installed = True
        except Exception as e:
            print(e)

    def restart_cm(self):
        try:
            command_api_url = '%s/%s/commands' % (self.api_clusters_url, self.cluster_name)
            restart_api_url = command_api_url + '/restart'
            deploy_api_url = command_api_url + '/deployClientConfig'
            body = dict()
            body['restartOnlyStaleServices'] = True
            body['redeployClientConfiguration'] = True
            print('\nRestarting services')
            self.post_requests(restart_api_url, data=json.dumps(body))
        except Exception as e:
            print(e)


# --------------------------------------------- Helper functions
def print_format(config_name, content):
    print("{0} {1:>{width}}".format(config_name, content, width=80 - len(config_name)))


def print_green(input_str):
    return '\033[0;32m%s\033[0m' % input_str


def print_red(input_str):
    return '\033[0;31m%s\033[0m' % input_str


def print_yellow(input_str):
    return '\033[0;33m%s\033[0m' % input_str


def print_verbose(cur_val, sug_val=None):
    print('Current Configuration: ' + print_yellow(str(cur_val)))
    if sug_val:
        print('Suggest Configuration: ' + print_green(str(sug_val)))
# --------------------------------------------- Helper functions


def main():
    cdh_setup = CDHSetup()
    cdh_setup.install_parcels()
    if argv.sensor_only:
        exit()
    if cdh_setup.parcel_installed:
        cdh_setup.update_hive()
        cdh_setup.update_spark()
        if cdh_setup.has_spark2:
            cdh_setup.update_spark(spark_ver_x=2)
        cdh_setup.update_yarn()
        cdh_setup.update_unravel_properties()
        if argv.restart_cm:
            cdh_setup.restart_cm()
    else:
        print('Unravel Parcel did NOT install skip configuration')
    if argv.dry_test:
        print (print_yellow('\nThe script is running in dry run mode no configuration will be changed'))


if __name__ == '__main__':
    main()
