#!/usr/bin/python
# v1.0.2
import os
import re
import json
import base64
import urllib2
import zipfile
import argparse
from time import time
from subprocess import call, Popen, PIPE

parser = argparse.ArgumentParser()
parser.add_argument("--spark-version", help="spark version e.g. 1.6.3 or 2.1.0", dest='spark_ver', required=True)
parser.add_argument("--hive-version", help="hive version e.g. 1.2 or 2.1", dest='hive_ver', required=True)
parser.add_argument("--unravel-server", help="Unravel Server hostname/IP", dest='unravel', required=True)
parser.add_argument("--ambari-server", help="Ambari Server hostname/IP", dest='ambari', required=True)
parser.add_argument("--ambari-user", help="Ambari Server Login username", dest='ambari_user', default='admin')
parser.add_argument("--ambari-password", help="Ambari Server Login password", dest='ambari_pass', default='admin')
parser.add_argument("--dry-run", help="Only Test but will not update anything", dest='dry_test', action='store_true')
parser.add_argument("-v", "--verbose", help="print current and suggess configuration", action='store_true')
parser.add_argument("--sensor-only", help="check/upgrade Unravel Sensor Only", dest='sensor_only', action='store_true')
parser.add_argument("--restart_am", help="Restart Ambari Services", action='store_true')
# parser.add_argument("--ssh_user", help="SSH username for all Cluster Host")
# parser.add_argument("--ssh_password", help="SSH password for all Cluster Host")
# parser.add_argument("--ssh_key", help="SSH key full path for all Cluster Host")
argv = parser.parse_args()

argv.unravel_ip = Popen(['hostname', '-i'], stdout=PIPE).communicate()[0].strip()
if len(argv.unravel.split(':')) == 2:
    argv.unravel = argv.unravel.split(':')[0]
    argv.unravel_port = argv.unravel.split(':')[1]
else:
    argv.unravel_port = 3000


class HDPSetup:
    def __init__(self):
        self.test = None
        self.hive_version_xyz = argv.hive_ver.split('.')
        self.spark_version_xyz = argv.spark_ver.split('.')
        self.unravel_base_url = argv.unravel
        self.ambari_api_url = "http://{0}:8080/api/v1/".format(argv.ambari)
        self.configs = self.generate_configs(argv.unravel)
        self.configs_ip = self.generate_configs(argv.unravel_ip)
        self.do_hive = True
        self.do_spark = True
        self.cluster_name = self.read_configs('clusters')['items'][0]['Clusters']['cluster_name']
        self.current_config_tag = self.read_configs('clusters?fields=Clusters/desired_configs')['items'][0]['Clusters'][
            'desired_configs']
        self.configs_base_url = 'clusters/' + self.cluster_name + '/configurations'

    def generate_configs(self, unravel_host):
        configs = {}
        configs['hive-site'] = {
            'hive.exec.driver.run.hooks': 'com.unraveldata.dataflow.hive.hook.HiveDriverHook',
            'com.unraveldata.hive.hdfs.dir': '/user/unravel/HOOK_RESULT_DIR',
            'com.unraveldata.hive.hook.tcp': 'true',
            'com.unraveldata.host': unravel_host,
            'hive.exec.pre.hooks': 'com.unraveldata.dataflow.hive.hook.HivePreHook',
            'hive.exec.post.hooks': 'com.unraveldata.dataflow.hive.hook.HivePostHook',
            'hive.exec.failure.hooks': 'com.unraveldata.dataflow.hive.hook.HiveFailHook'
        }
        configs['hive-env'] = 'export AUX_CLASSPATH=${AUX_CLASSPATH}:/usr/local/unravel_client/unravel-hive-%s.%s.0-hook.jar' % (
        self.hive_version_xyz[0], self.hive_version_xyz[1])
        configs['hadoop-env'] = 'export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/usr/local/unravel_client/unravel-hive-%s.%s.0-hook.jar' % (
        self.hive_version_xyz[0], self.hive_version_xyz[1])
        if re.search('1.[6-9]', argv.spark_ver):
            configs['spark-defaults'] = {
                'spark.eventLog.dir':'hdfs:///spark-history',
                'spark.history.fs.logDirectory':'hdfs:///spark-history',
                'spark.unravel.server.hostport': unravel_host + ':4043',
                'spark.driver.extraJavaOptions': '-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=spark-%s,config=driver' % (
                    re.search('1.[0-9]', argv.spark_ver).group(0)),
                'spark.executor.extraJavaOptions': '-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=spark-%s,config=executor' % (
                    re.search('1.[0-9]', argv.spark_ver).group(0))
            }
        if re.search('2.[0-9]', argv.spark_ver):
            configs['spark2-defaults'] = {
                'spark.eventLog.dir':'hdfs:///spark-history',
                'spark.history.fs.logDirectory':'hdfs:///spark-history',
                'spark.unravel.server.hostport': unravel_host + ':4043',
                'spark.driver.extraJavaOptions': '-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=spark-%s,config=driver' % (
                    re.search('2.[0-9]', argv.spark_ver).group(0)),
                'spark.executor.extraJavaOptions': '-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=spark-%s,config=executor' % (
                    re.search('2.[0-9]', argv.spark_ver).group(0))
            }
        configs['mapred-site'] = {
            'yarn.app.mapreduce.am.command-opts': '-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=mr -Dunravel.server.hostport=%s:4043' % unravel_host,
            'mapreduce.task.profile': 'true',
            'mapreduce.task.profile.maps': '0-5',
            'mapreduce.task.profile.reduces': '0-5',
            'mapreduce.task.profile.params': '-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=mr -Dunravel.server.hostport=%s:4043' % unravel_host
        }
        configs['unravel-properties'] = {
            "com.unraveldata.job.collector.done.log.base": "/mr-history/done",
            "com.unraveldata.job.collector.log.aggregation.base": "/app-logs/*/logs/",
            "com.unraveldata.spark.eventlog.location": "hdfs:///spark-history"
        }
        configs['tez-site'] = {
            'tez.am.launch.cmd-opts': '-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=mr,config=tez -Dunravel.server.hostport=%s:4043' % unravel_host,
            'tez.task.launch.cmd-opts': '-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=mr,config=tez -Dunravel.server.hostport=%s:4043' % unravel_host
        }
        return configs

    def get_yarn_timeline(self):
        return self.read_configs(self.configs_base_url + '?type={0}&tag={1}'.format('yarn-site', self.current_config_tag['yarn-site']['tag']))['items'][0]['properties']['yarn.timeline-service.webapp.address'].split(':')

    def read_configs(self, api_path, full_path=False):
        try:
            if full_path:
                request_url = api_path
            else:
                request_url = self.ambari_api_url + api_path
                # print request_url
            request = urllib2.Request(request_url)
            base64string = base64.b64encode('%s:%s' % (argv.ambari_user, argv.ambari_pass))
            request.add_header("Authorization", "Basic %s" % base64string)
            return json.loads(urllib2.urlopen(request).read())
        except urllib2.HTTPError as e:
            if '403' in str(e):
                print("Invalid username/password combination. Please use correct Ambari Login credentials")
            else:
                print(e)
            exit()
        except Exception as e:
            print(e)
            print("Error: Unable to reach Ambari Server")
            exit()

    def restart_services(self):
        if not argv.dry_test:
            print("\nRestart Ambari Services")
            restart_command = 'curl -u {0}:\'{1}\' -i -H \'X-Requested-By: ambari\' -X POST -d \'{{\"RequestInfo\": {{\"command\":\"RESTART\",\"context\" :\"Unravel request: Restart Services\",\"operation_level\":\"host_component\"}},\"Requests/resource_filters\":[{{\"hosts_predicate\":\"HostRoles/stale_configs=true\"}}]}}\' http://{2}:8080/api/v1/clusters/{3}/requests >/tmp/Restart.out 2>/tmp/Restart.err'.format(argv.ambari_user, argv.ambari_pass, argv.ambari, self.cluster_name)
            call_result = call(restart_command, shell=True)
            # print call_result, restart_command

    def update_hive_site(self):
        try:
            print("\nChecking hive-site.xml")
            config_type = 'hive-site'
            hive_site_configs = self.read_configs(self.configs_base_url + '?type={0}&tag={1}'.format(
                config_type, self.current_config_tag[config_type]['tag']))['items'][0]['properties']
            config_changed = False

            if not self.do_hive:
                return

            for config, val in self.configs[config_type].iteritems():
                val_ip = self.configs_ip[config_type][config]
                cur_config = hive_site_configs.get(config, None)
                if cur_config is None:
                    # print("{:10} {:>{width}}".format(config, print_red("missing"), width=80 - len(config)))
                    print_format(config, print_red("missing"))
                    config_changed = True
                    hive_site_configs[config] = val
                    if argv.verbose:
                        print_verbose(cur_config, val)
                elif val in cur_config or val_ip in cur_config:
                    # print("{:10} {:>{width}}".format(config, print_green("correct"), width=80 - len(config)))
                    print_format(config, print_green("correct"))
                    if argv.verbose:
                        print_verbose(cur_config)
                elif re.match('hive.exec.(pre|post|failure).hooks', config) and val not in cur_config:
                    # print("{:10} {:>{width}}".format(config, print_red("incorrect"), width=80 - len(config)))
                    print_format(config, print_red("incorrect"))
                    hive_site_configs[config] += ',' + val
                    config_changed = True
                    if argv.verbose:
                        print_verbose(cur_config, hive_site_configs[config])
                else:
                    # print("{:10} {:>{width}}".format(config, print_red("incorrect"), width=80 - len(config)))
                    print_format(config, print_red("incorrect"))
                    hive_site_configs[config] = val
                    config_changed = True
                    if argv.verbose:
                        print_verbose(cur_config, val)

            if config_changed and not argv.dry_test:
                print("updating %s" % config_type)
                self.update_configs(config_type, hive_site_configs)
        except Exception as e:
            print("Error: " + str(e))

    def update_hive_env(self):
        print("\nChecking hive-env.sh")
        try:
            config_type = 'hive-env'
            hive_env_configs = self.read_configs(self.configs_base_url + '?type={0}&tag={1}'.format(
                config_type, self.current_config_tag[config_type]['tag']))['items'][0]['properties']

            if self.do_hive and self.configs['hive-env'].split(':')[1] in hive_env_configs['content']:
                # print("{:10} {:>{width}}".format('AUX_CLASSPATH', print_green("correct"), width=80 - len('AUX_CLASSPATH')))
                print_format('AUTH_CLASSPATH', print_green("correct"))
                if argv.verbose:
                    print_verbose(self.configs['hive-env'])
            else:
                # print("{:10} {:>{width}}".format('AUX_CLASSPATH', print_red("missing"), width=80 - len('AUX_CLASSPATH')))
                print_format('AUX_CLASSPATH', print_red("missing"))
                hive_env_configs['content'] += '\n%s\n' % self.configs[config_type]
                if argv.verbose:
                    print_verbose('', self.configs['hive-env'])
                if not argv.dry_test:
                    print("updating %s" % config_type)
                    self.update_configs(config_type, hive_env_configs)
        except Exception as e:
            print("Error: " + str(e))

    def update_spark_defaults(self, config_type, version):
        try:
            print("\nChecking %s.conf" % config_type)
            config_changed = False
            spark_defaults_configs = self.read_configs(self.configs_base_url + '?type={0}&tag={1}'.format(
                config_type, self.current_config_tag[config_type]['tag']))['items'][0]['properties']

            for config, val in self.configs[config_type].iteritems():
                val_ip = self.configs_ip[config_type][config]
                cur_config = spark_defaults_configs.get(config, None)
                if cur_config is None:
                    # print("{:10} {:>{width}}".format(config, print_red("missing"), width=80 - len(config)))
                    print_format(config, print_red("missing"))
                    config_changed = True
                    spark_defaults_configs[config] = val
                    if argv.verbose:
                        print_verbose(cur_config, val)
                elif config == 'spark.eventLog.dir' or config == 'spark.history.fs.logDirectory':
                    self.configs['spark-defaults'][config] = cur_config
                    if config == 'spark.eventLog.dir':
                        self.configs['unravel-properties']["com.unraveldata.spark.eventlog.location"] = cur_config
                elif val in cur_config or val_ip in cur_config or 'libs=spark-%s' % version in cur_config:
                    # print("{:10} {:>{width}}".format(config, print_green("correct"), width=80 - len(config)))
                    print_format(config, print_green("correct"))
                    if argv.verbose:
                        print_verbose(cur_config)
                elif config == 'spark.unravel.server.hostport':
                    # print("{:10} {:>{width}}".format(config, print_red("incorrect"), width=80 - len(config)))
                    print_format(config, print_red("incorrect"))
                    config_changed = True
                    spark_defaults_configs[config] = val
                    if argv.verbose:
                        print_verbose(cur_config, val)
                else:
                    # print("{:10} {:>{width}}".format(config, print_red("incorrect"), width=80 - len(config)))
                    print_format(config, print_red("incorrect"))
                    config_changed = True
                    orgin_regex = '-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=config=driver,libs=spark-[0-9].[0-9]'
                    if re.search(orgin_regex, spark_defaults_configs[config]):
                        spark_defaults_configs[config] = re.sub(orgin_regex, val, spark_defaults_configs[config])
                    else:
                        spark_defaults_configs[config] += ' ' + val
                    if argv.verbose:
                        print_verbose(cur_config, spark_defaults_configs[config])
            if config_changed and not argv.dry_test:
                print("updating %s" % config_type)
                self.update_configs(config_type, spark_defaults_configs)
        except Exception as e:
            print("Error: " + str(e))

    def update_hadoop_env(self):
        print("\nChecking hadoop-env.sh")
        try:
            config_type = 'hadoop-env'
            hive_env_configs = self.read_configs(self.configs_base_url + '?type={0}&tag={1}'.format(
                config_type, self.current_config_tag[config_type]['tag']))['items'][0]['properties']

            if self.do_hive and self.configs['hadoop-env'].split(':')[1] in hive_env_configs['content']:
                # print("{:10} {:>{width}}".format('HADOOP_CLASSPATH', print_green("correct"), width=80 - len('HADOOP_CLASSPATH')))
                print_format('HADOOP_CLASSPATH', print_green("correct"))
                if argv.verbose:
                    print_verbose(self.configs['hadoop-env'])
            else:
                # print("{:10} {:>{width}}".format('HADOOP_CLASSPATH', print_red("missing"), width=80 - len('HADOOP_CLASSPATH')))
                print_format('HADOOP_CLASSPATH', print_red("missing"))
                if argv.verbose:
                    print_verbose('', self.configs['hadoop-env'])
                if not argv.dry_test:
                    hive_env_configs['content'] += '\n%s\n' % self.configs[config_type]
                    print("updating %s" % config_type)
                    self.update_configs(config_type, hive_env_configs)
        except Exception as e:
            print("Error: " + str(e))

    def update_mapred_site(self):
        try:
            print("\nChecking mapred-site.xml")
            config_type = 'mapred-site'
            config_changed = False
            mapred_site_configs = self.read_configs(self.configs_base_url + '?type={0}&tag={1}'.format(
                config_type, self.current_config_tag[config_type]['tag']))['items'][0]['properties']

            for config, val in self.configs[config_type].iteritems():
                val_ip = self.configs_ip[config_type][config]
                cur_config = mapred_site_configs.get(config, None)
                if cur_config is None:
                    # print("{:10} {:>{width}}".format(config, print_red("missing"), width=80 - len(config)))
                    print_format(config, print_red("missing"))
                    config_changed = True
                    mapred_site_configs[config] = val
                    if argv.verbose:
                        print_verbose(cur_config, val)
                elif val in cur_config or val_ip in cur_config:
                    # print("{:10} {:>{width}}".format(config, print_green("correct"), width=80 - len(config)))
                    print_format(config, print_green("correct"))
                    if argv.verbose:
                        print_verbose(cur_config)
                elif not config == ('yarn.app.mapreduce.am.command-opts' or 'mapreduce.task.profile.params'):
                    # print("{:10} {:>{width}}".format(config, print_red("incorrect"), width=80 - len(config)))
                    print_format(config, print_red("incorrect"))
                    config_changed = True
                    mapred_site_configs[config] = val
                    if argv.verbose:
                        print_verbose(cur_config, val)
                else:
                    # print("{:10} {:>{width}}".format(config, print_red("incorrect"), width=80 - len(config)))
                    print_format(config, print_red("incorrect"))
                    config_changed = True
                    orgin_regex = '-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=mr -Dunravel.server.hostport=.*?:4043'
                    if re.search(orgin_regex, mapred_site_configs[config]):
                        mapred_site_configs[config] = re.sub(orgin_regex, val, mapred_site_configs[config])
                    else:
                        mapred_site_configs[config] += ' ' + val
                    if argv.verbose:
                        print_verbose(cur_config, mapred_site_configs[config])
            if config_changed and not argv.dry_test:
                print("updating %s" % config_type)
                self.update_configs(config_type, mapred_site_configs)
        except Exception as e:
            print("Error: " + str(e))

    def update_tez_site(self):
        print("\nChecking tez-site.xml")
        try:
            config_changed = False
            config_type = 'tez-site'
            tez_site_configs = self.read_configs(self.configs_base_url + '?type={0}&tag={1}'.format(
                config_type, self.current_config_tag[config_type]['tag']))['items'][0]['properties']

            for config, val in self.configs[config_type].iteritems():
                val_ip = self.configs_ip[config_type][config]
                cur_config = tez_site_configs.get(config, None)
                if cur_config is None:
                    # print("{:10} {:>{width}}".format(config, print_red("missing"), width=80 - len(config)))
                    print_format(config, print_red("missing"))
                    config_changed = True
                    tez_site_configs[config] = val
                    if argv.verbose:
                        print_verbose(cur_config, val)
                elif val in cur_config or val_ip in cur_config:
                    # print("{:10} {:>{width}}".format(config, print_green("correct"), width=80 - len(config)))
                    print_format(config, print_green("correct"))
                    if argv.verbose:
                        print_verbose(cur_config)
                else:
                    # print("{:10} {:>{width}}".format(config, print_red("incorrect"), width=80 - len(config)))
                    print_format(config, print_red("incorrect"))
                    config_changed = True
                    orgin_regex = '-javaagent:/usr/local/unravel-agent.*?:4043'
                    if re.search(orgin_regex, tez_site_configs[config]):
                        tez_site_configs[config] = re.sub(orgin_regex, val, tez_site_configs[config])
                    else:
                        tez_site_configs[config] += ' ' + val
                    if argv.verbose:
                        print_verbose(cur_config, tez_site_configs[config])
            # Insert yarn.timeline.webapp property in unravel.properties
            yarn_timeline_webapp = self.get_yarn_timeline()
            self.configs['unravel-properties']['com.unraveldata.yarn.timeline-service.webapp.address'] = 'http://%s' % yarn_timeline_webapp[0]
            self.configs['unravel-properties']['com.unraveldata.yarn.timeline-service.port'] = yarn_timeline_webapp[1]
            if config_changed and not argv.dry_test:
                self.update_configs(config_type, tez_site_configs)
                print("updating %s" % config_type)
        except Exception as e:
            print("Error: " + str(e))

    def update_unravel_properties(self):
        print("\nChecking Unravel properties")
        unravel_properties_path = '/usr/local/unravel/etc/unravel.properties'
        headers = "# HDP Setup\n"
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
                                break
                        if not correct_flag:
                            # print("{:10} {:>{width}}".format(config, print_red("incorrect"), width=80-len(config)))
                            print_format(config, print_red("incorrect"))
                            new_config += '%s=%s\n' % (config, config + '=' + val)
                            if argv.verbose:
                                print_verbose(cur_config.strip(), config + '=' + val)
                        else:
                            # print("{:10} {:>{width}}".format(config, print_green("correct"), width=80-len(config)))
                            print_format(config, print_green("correct"))
                            if argv.verbose:
                                print_verbose(config + '=' + val)
                    else:
                        # print("{:10} {:>{width}}".format(config, print_red("missing"), width=80-len(config)))
                        print_format(config, print_red("missing"))
                        new_config += '%s=%s\n' % (config, val)
                        if argv.verbose:
                            print_verbose(None, val)
                if len(new_config.split('\n')) > 1 and not argv.dry_test:
                    print('Updating Unravel Properties')
                    with open(unravel_properties_path, 'a') as f:
                        f.write(headers + new_config)
                        f.close()
                    # print('\nRestarting Unravel')
                    # print Popen(['/etc/init.d/unravel_all.sh', 'restart']).communicate()[0]
            except Exception as e:
                print(e)
                print('skip update unravel.properties')
        else:
            print("unravel.properties not found skip update unravel.properties")

    def update_configs(self, config_type, new_properties):
        try:
            new_tag = 'version' + str(int(time()))
            config_template = """{
            "Clusters" : {
                "desired_config": [{
                    "type": "%s",
                    "tag": "%s",
                    "properties": %s
                    }]
            }
            }""" % (config_type, new_tag, json.dumps(new_properties))
            request_url = self.ambari_api_url + "clusters/" + self.cluster_name
            request = urllib2.Request(request_url, data=config_template)
            base64string = base64.b64encode('%s:%s' % (argv.ambari_user, argv.ambari_pass))
            request.add_header("Authorization", "Basic %s" % base64string)
            request.add_header("X-Requested-By", "ambari")
            request.get_method = lambda: 'PUT'
            urllib2.urlopen(request)
        except Exception as e:
            print(e)
            pass

    def check_unravel_version(self):
        unravel_properties_path = '/usr/local/unravel/etc/unravel.properties'
        unravel_version_path = '/usr/local/unravel/ngui/www/version.txt'
        # update unravel properties if unravel version is 4.3.2 or above
        if os.path.exists(unravel_version_path):
            print('\nchecking Unravel version')
            with open(unravel_version_path, 'r') as f:
                version_file = f.read()
                f.close()
            if re.search('4\.[2-9]\.[1-9].*', version_file):
                print(re.search('4\.[2-9]\.[1-9].*', version_file).group(0))

            if re.search('4\.3\.[2-9]', version_file) and os.path.exists(unravel_properties_path):
                print('Unravel 4.3.2 and above detected, use jdbc maria driver')
                if not argv.dry_test:
                    file = open(unravel_properties_path, 'r').read()
                    unravel_properties = re.sub('unravel.jdbc.url=jdbc:mysql', 'unravel.jdbc.url=jdbc:mariadb', file)
                    file = open(unravel_properties_path, 'w')
                    file.write(unravel_properties)
                    file.close()
                    print('Unravel 4.3.2 detected, updating jdbc driver')


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


# Download hive-hook jar and spark sensor zip function shared in MapR and HDP
def deploy_unravel_sensor(unravel_base_url, hive_version_xyz):
    unravel_sensor_url = 'http://{unravel_base_url}:3000/hh/'.format(unravel_base_url=unravel_base_url)
    sensor_deploy_result = []
    # Download Hive Hook jar
    try:
        hive_hook_jar = 'unravel-hive-{x}.{y}.0-hook.jar'.format(x=hive_version_xyz[0], y=hive_version_xyz[1])
        save_path = '/usr/local/unravel_client/' + hive_hook_jar
        if not argv.dry_test:
            print("\nDownloading Unravel Hive Hook Sensor")
            # Create unravel_client if not exists
            if not os.path.exists(os.path.dirname(save_path)):
                print(save_path + ' not exists creating ' + save_path)
                os.makedirs(os.path.dirname(save_path))

            with open(save_path, 'wb') as f:
                f.write(urllib2.urlopen(unravel_sensor_url + hive_hook_jar).read())
                f.close()
            print(hive_hook_jar + " Download Complete!")
            print(hive_hook_jar + " Installed!")
            sensor_deploy_result.append(True)
        else:
            sensor_deploy_result.append(True)
            if os.path.exists(save_path):
                print('Unravel Hive Hook Sensor Installed\n')
            else:
                print('Unravel Hive Hook Sensor NOT Install\n')
    except Exception as e:
        print "Error:", e, unravel_sensor_url + hive_hook_jar
        print("Failed to Download Hive Hook Sensor")
        os.remove(save_path)
        sensor_deploy_result.append(False)

    # Download and unzip Spark Sensor zip
    try:
        spark_sensor_zip = 'unravel-agent-pack-bin.zip'
        save_path = '/usr/local/unravel-agent/' + spark_sensor_zip
        jar_path = '/usr/local/unravel-agent/jars/'
        if not argv.dry_test:
            print("\nDownloading Unravel Spark Sensor")
            # Create unravel_client if not exists
            if not os.path.exists(os.path.dirname(save_path)):
                print(save_path + ' not exists creating ' + save_path)
                os.makedirs(jar_path)

            with open(save_path, 'wb') as f:
                f.write(urllib2.urlopen(unravel_sensor_url + spark_sensor_zip).read())
                f.close()
            print "Download Complete!"

            if os.path.exists(save_path):
                print(spark_sensor_zip + ' Downloaded')
                zip_target = zipfile.ZipFile(save_path, 'r')
                zip_target.extractall(jar_path)
                zip_target.close()
                print('Spark Sensor Installed')
                sensor_deploy_result.append(True)
            else:
                print(spark_sensor_zip + ' Download Failed')
                sensor_deploy_result.append(False)
        else:
            sensor_deploy_result.append(True)
            if os.path.exists(jar_path):
                print('Unravel Spark Sensor Installed\n')
            else:
                print('Unravel Spark Sensor NOT Install\n')
    except Exception as e:
        print "Error: ", e, unravel_sensor_url + spark_sensor_zip
        print("Failed to Download Spark Sensor zip")
        sensor_deploy_result.append(False)
    return sensor_deploy_result


def main():
    hdp_setup = HDPSetup()

    deploy_sensor_result = deploy_unravel_sensor(hdp_setup.unravel_base_url, hdp_setup.hive_version_xyz)

    if argv.sensor_only:
        exit(0)

    # if hive sensor install succefully do instrumentation
    if deploy_sensor_result[0] or argv.dry_test:
        hdp_setup.update_hive_site()
        hdp_setup.update_hive_env()
        hdp_setup.update_hadoop_env()
    else:
        print("\nInstall Hive Hook Sensor Failed skip hive instrumentation")

    # if spark & MR sensor install succefully do instrumentation
    if deploy_sensor_result[1] or argv.dry_test:
        if re.search('2.[0-9]', argv.spark_ver):
            config_type = 'spark2-defaults'
            hdp_setup.update_spark_defaults(config_type, re.search('2.[0-9]', argv.spark_ver).group(0))
        if re.search('1.[6-9]', argv.spark_ver):
            config_type = 'spark-defaults'
            hdp_setup.update_spark_defaults(config_type, re.search('1.[6-9]', argv.spark_ver).group(0))
        hdp_setup.update_mapred_site()
        hdp_setup.update_tez_site()
    else:
        print("\nInstall Spark & MR Sensor Failed skip spark & MR instrumentation")
    hdp_setup.update_unravel_properties()
    hdp_setup.check_unravel_version()

    if argv.restart_am:
        hdp_setup.restart_services()
    if argv.dry_test:
        print (print_yellow('\nThe script is running in dry run mode no configuration will be changed'))


if __name__ == '__main__':
    main()
