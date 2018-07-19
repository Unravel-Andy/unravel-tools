#!/usr/bin/python
# v1.0.10
# - custom configuration path
import os
import re
import json
import glob
import urllib2
import zipfile
import argparse
from glob import glob
from time import sleep
from shutil import copyfile
import xml.etree.ElementTree as ET
from subprocess import Popen, PIPE

parser = argparse.ArgumentParser()
parser.add_argument("--spark-version", help="spark version e.g. 1.6.3 or 2.1.0", dest='spark_ver', required=True)
parser.add_argument("--hive-version", help="hive version e.g. 1.2 or 2.1", dest='hive_ver', required=True)
parser.add_argument("--unravel-server", help="Unravel Server hostname/IP", dest='unravel', required=True)
parser.add_argument("--dry-run", help="Only Test but will not update anything", dest='dry_test', action='store_true')
parser.add_argument("-v", "--verbose", help="print current and suggess configuration", action='store_true')
parser.add_argument("--sensor-only", help="check/upgrade Unravel Sensor Only", dest='sensor_only', action='store_true')
parser.add_argument("--hive-path", help="path to hive configuration DIR default: /opt/mapr/hive/hive-X.Y/conf", dest='hive_path')
parser.add_argument("--spark-path", help="path to spark configuration DIR default: /opt/mapr/spark/spark-X.Y.Z/conf", dest='spark_path')
parser.add_argument("--hadoop-path", help="path to hadoop configuration DIR default: /opt/mapr/hadoop/hadoop-X.Y.Z/etc/hadoop", dest='hadoop_path')
argv = parser.parse_args()

if len(argv.unravel.split(':')) == 2:
    argv.unravel_port = argv.unravel.split(':')[1]
    argv.unravel = argv.unravel.split(':')[0]
else:
    argv.unravel_port = 3000


class MaprSetup:
    def __init__(self):
        self.test = None
        self.hive_version_xyz = argv.hive_ver.split('.')
        self.spark_version_xyz = argv.spark_ver.split('.')
        self.unravel_base_url = argv.unravel
        self.base_mapr_path = '/opt/mapr/'
        self.configs = self.generate_configs()
        self.do_hive = True
        self.do_spark = True

    def generate_configs(self):
        configs = {}
        unravel_host = argv.unravel
        configs['hive-site'] = {
                                "com.unraveldata.host": [unravel_host,'Unravel hive-hook processing host'],
                                "com.unraveldata.hive.hook.tcp": ['true', ' '],
                                "com.unraveldata.hive.hdfs.dir": ["/user/unravel/HOOK_RESULT_DIR", "destination for hive-hook, Unravel log processing"],
                                "hive.exec.driver.run.hooks": ["com.unraveldata.dataflow.hive.hook.HiveDriverHook", "for Unravel, from unraveldata.com"],
                                "hive.exec.pre.hooks": ["com.unraveldata.dataflow.hive.hook.HivePreHook", "for Unravel, from unraveldata.com"],
                                "hive.exec.post.hooks": ["com.unraveldata.dataflow.hive.hook.HivePostHook", "for Unravel, from unraveldata.com"],
                                "hive.exec.failure.hooks": ["com.unraveldata.dataflow.hive.hook.HiveFailHook", "for Unravel, from unraveldata.com"]
                                }
        configs['hive-env'] = 'export AUX_CLASSPATH=${AUX_CLASSPATH}:/usr/local/unravel_client/unravel-hive-%s.%s.0-hook.jar' % (self.hive_version_xyz[0], self.hive_version_xyz[1])
        configs['hadoop-env'] = 'export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/usr/local/unravel_client/unravel-hive-%s.%s.0-hook.jar' % (self.hive_version_xyz[0], self.hive_version_xyz[1])
        configs['spark-defaults'] = {
                                    'spark.eventLog.dir': 'maprfs:///apps/spark',
                                    'spark.history.fs.logDirectory': 'maprfs:///apps/spark',
                                    'spark.unravel.server.hostport': unravel_host+':4043',
                                    'spark.driver.extraJavaOptions': '-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=spark-%s.%s,config=driver' % (self.spark_version_xyz[0], self.spark_version_xyz[1]),
                                    'spark.executor.extraJavaOptions': '-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=spark-%s.%s,config=executor' % (self.spark_version_xyz[0], self.spark_version_xyz[1])
                                    }
        configs['mapred-site'] = {
                                  'yarn.app.mapreduce.am.command-opts': ['-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=mr -Dunravel.server.hostport=%s:4043' % unravel_host, ' '],
                                  'mapreduce.task.profile': ['true', ' '],
                                  'mapreduce.task.profile.maps': ['0-5', ' '],
                                  'mapreduce.task.profile.reduces': ['0-5', ' '],
                                  'mapreduce.task.profile.params': ['-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=mr -Dunravel.server.hostport=%s:4043' % unravel_host, ' ']
                                 }
        configs['unravel-properties'] = {
                                         "com.unraveldata.job.collector.done.log.base": "/var/mapr/cluster/yarn/rm/staging/history/done",
                                         "com.unraveldata.is_mapr": "true",
                                         "fs.defaultFS": "maprfs://",
                                         "com.unraveldata.job.collector.log.aggregation.base": "/tmp/logs/*/logs/",
                                         "com.unraveldata.spark.eventlog.location": "maprfs:///apps/spark"
                                        }
        configs['yarn-site'] = {
                                "yarn.resourcemanager.webapp.address": ["%s:8088" % self.get_resourcemanager_host(), ' '],
                                "yarn.log-aggregation-enable": ["true", "For log aggregations"]
                                }

        if 'None' in configs['yarn-site']['yarn.resourcemanager.webapp.address'][0]:
            del configs['yarn-site']['yarn.resourcemanager.webapp.address']

        return configs

    def generate_xml_property(self, config, val_list, default=True):
        if default:
            return """
  <property>
    <name>%s</name>
    <value>%s</value>
    <description>%s</description>
 </property>
""" % (config, val_list[0], val_list[1])
        else:
            return """
  <property>
    <name>%s</name>
    <value>%s</value>
    <source>yarn-site.xml</source>
  </property>
""" % (config, val_list[0])

    def update_hive_site(self):
        try:
            print("\nChecking hive-site.xml")
            if argv.hive_path:
                conf_base_path = argv.hive_path
            else:
                conf_base_path = os.path.join(self.base_mapr_path, 'hive/hive-{x}.{y}/conf/'.format(x=self.hive_version_xyz[0], y=self.hive_version_xyz[1]))
            hive_site_xml = os.path.join(conf_base_path, 'hive-site.xml')
            preunravel_hive_site = os.path.join(conf_base_path, 'hive-site.xml.preunravel')

            if not os.path.exists(preunravel_hive_site) and not argv.dry_test:
                print("Backup original hive-site.xml")
                copyfile(hive_site_xml, preunravel_hive_site)

            if not self.do_hive:
                return

            tree = ET.parse(hive_site_xml)
            root = tree.getroot()
            # val_list[0] is value, val_list[1] is description
            for config, val_list in self.configs['hive-site'].iteritems():
                find_property = False
                for property in root.findall('property'):
                    property_name = property.find('name')
                    property_value = property.find('value')
                    if val_list[0] in property_value.text and property_name.text == config:
                        print("{0} {1:>{width}}".format(config, "No change needed", width=80-len(config)))
                        find_property = True
                        if argv.verbose: print_verbose(val_list[0])
                        break
                    elif config == property_name.text:
                        print("{0} {1:>{width}}".format(config, "incorrect", width=80-len(config)))
                        if argv.verbose: print_verbose(property_value.text, val_list[0])
                        if property_name.text == ('com.unraveldata.host' or 'com.unraveldata.hive.hook.tcp' or 'com.unraveldata.hive.hdfs.dir'):
                            property_value.text = val_list[0]
                        else:
                            property_value.text = property_value.text + ',' + val_list[0]
                        find_property = True

                        if not argv.dry_test: tree.write(hive_site_xml)
                        break
                if not find_property:
                    print("{0} {1:>{width}}".format(config, "missing", width=80-len(config)))
                    xml_str = self.generate_xml_property(config, val_list)
                    new_property = ET.fromstring(xml_str)
                    root.append(new_property)
                    if argv.verbose: print_verbose('None', val_list[0])
                    if not argv.dry_test: tree.write(hive_site_xml)
        except Exception as e:
            print("Error: " + str(e))
            self.do_hive = False

    def update_hive_env(self):
        print("\nChecking hive-env.sh")
        try:
            if argv.hive_path:
                conf_base_path = argv.hive_path
            else:
                conf_base_path = os.path.join(self.base_mapr_path, 'hive/hive-{x}.{y}/conf/'.format(x=self.hive_version_xyz[0], y=self.hive_version_xyz[1]))
            hive_env_sh = os.path.join(conf_base_path, 'hive-env.sh')
            preunravel_hive_env = os.path.join(conf_base_path, 'hive-env.sh.preunravel')

            if not os.path.exists(hive_env_sh):
                copyfile(os.path.join(conf_base_path, 'hive-env.sh.template'), hive_env_sh)
            elif not os.path.exists(preunravel_hive_env) and not argv.dry_test:
                print("Backup original hive-env.sh")
                copyfile(hive_env_sh, preunravel_hive_env)

            with open(hive_env_sh, 'r') as f:
                content = f.read()

            if self.do_hive and self.configs['hive-env'].split(':')[1] in content:
                print("{0} {1:>{width}}".format("AUX_CLASSPATH", "No change needed",  width=80-len('AUX_CLASSPATH')))
                if argv.verbose: print_verbose(self.configs['hive-env'])
            else:
                print("{0} {1:>{width}}".format("AUX_CLASSPATH", "Missing value",  width=80-len('AUX_CLASSPATH')))
                if argv.verbose: print_verbose('None', self.configs['hive-env'])
                if not argv.dry_test:
                    with open(hive_env_sh, 'a') as f:
                        print("appending AUX_CLASSPATH")
                        f.write('\n' + self.configs['hive-env'])
                        f.close()
        except Exception as e:
            print("Error: " + str(e))
            self.do_hive = False

    def update_spark_defaults(self):
        print("\nChecking spark-defaults.conf")
        try:
            if argv.spark_path:
                conf_base_path = argv.spark_path
            else:
                conf_base_path = os.path.join(self.base_mapr_path, 'spark/spark-{0}/conf/'.format(argv.spark_ver))
            spark_default_conf = os.path.join(conf_base_path, 'spark-defaults.conf')
            preunravel_spark_default = os.path.join(conf_base_path, 'spark-defaults.conf.preunravel')
            new_config = None

            if not os.path.exists(preunravel_spark_default) and not argv.dry_test:
                print("Backup original spark-defaults.conf")
                copyfile(spark_default_conf, preunravel_spark_default)

            with open(spark_default_conf, 'r') as f:
                content = f.read()
                f.close()

            for config, val in self.configs['spark-defaults'].iteritems():
                if config in content:
                    if config == 'spark.eventLog.dir' or config == 'spark.history.fs.logDirectory':
                        self.configs['spark-defaults'][config] = filter(None, re.split('\s', re.search(config + '.*', content).group(0)))[1]
                        if config == 'spark.eventLog.dir':
                            self.configs['unravel-properties']["com.unraveldata.spark.eventlog.location"] = self.configs['spark-defaults'][config]
                    elif val in content:
                        print("{0} {1:>{width}}".format(config, "No change needed", width=80-len(config)))
                        if argv.verbose: print_verbose(val)
                    elif not config == 'spark.unravel.server.hostport':
                        print("{0} {1:>{width}}".format(config, "Missing value", width=80-len(config)))
                        orgin_regex = config + '.*$'
                        orgin_config = re.search(orgin_regex, content).group(0)
                        new_config = orgin_config + ' ' + val
                        if argv.verbose: print_verbose(orgin_config, new_config)
                    else:
                        print("{0} {1:>{width}}".format(config, "Missing value", width=80-len(config)))
                        orgin_regex = config + '.*'
                        orgin_config = re.search(orgin_regex, content).group(0)
                        new_config = orgin_config + ' ' + val
                        new_config = re.sub(orgin_regex, new_config, content)
                        if argv.verbose: print_verbose(orgin_config, new_config)
                else:
                    print("{0} {1:>{width}}".format(config, "missing", width=80-len(config)))
                    content += '\n' + config + ' ' + val
                    new_config = content
                    if argv.verbose: print_verbose('None', config + ' ' + val)

            if new_config and not argv.dry_test:
                print('Updating spark-defaults.conf')
                with open(spark_default_conf, 'w') as f:
                    f.write(new_config)
                    f.close()
        except Exception as e:
            print("Error: " + str(e))
            self.do_spark = False

    def update_hadoop_env(self):
        print("\nChecking hadoop-env.sh")
        try:
            if argv.hadoop_path:
                conf_base_path = argv.hadoop_path
            else:
                conf_base_path = glob('/opt/mapr/hadoop/hadoop-*.*.*/etc/hadoop/')[-1]
            hadoop_env_sh = os.path.join(conf_base_path, 'hadoop-env.sh')
            preunravel_hadoop_env = os.path.join(conf_base_path, 'hadoop-env.sh.preunravel')

            if not os.path.exists(preunravel_hadoop_env) and not argv.dry_test:
                print("Backup original hadoop-env.sh")
                copyfile(hadoop_env_sh, preunravel_hadoop_env)

            with open(hadoop_env_sh, 'r') as f:
                content = f.read()
                f.close()

            if self.do_hive and self.configs['hadoop-env'].split(':')[1] in content:
                print("{0} {1:>{width}}".format("HADOOP_CLASSPATH", "No change needed", width=80-len('HADOOP_CLASSPATH')))
                if argv.verbose: print_verbose(self.configs['hadoop-env'])
            elif self.do_hive:
                print("{0} {1:>{width}}".format("HADOOP_CLASSPATH", "Missing value", width=80-len('HADOOP_CLASSPATH')))
                if argv.verbose: print_verbose('None', self.configs['hadoop-env'])
                if not argv.dry_test:
                    with open(hadoop_env_sh, 'a') as f:
                        print("appending HADOOP_CLASSPATH")
                        f.write('\n' + self.configs['hadoop-env'])
                        f.close()
            else:
                print("Skip hadoop-env.sh")
        except Exception as e:
            print("Error: " + str(e))
            self.do_hive = False

    def update_mapred_site(self):
        try:
            print("\nChecking mapred-site.xml")
            if argv.hadoop_path:
                conf_base_path = argv.hadoop_path
            else:
                conf_base_path = glob('/opt/mapr/hadoop/hadoop-*.*.*/etc/hadoop/')[-1]
            mapred_site_xml = os.path.join(conf_base_path, 'mapred-site.xml')
            preunravel_mapred_site = os.path.join(conf_base_path, 'mapred-site.xml.preunravel')

            if not self.do_spark:
                print("Skip mapred-site")
                return

            if not os.path.exists(preunravel_mapred_site) and not argv.dry_test:
                print("Backup original mapred-site.xml")
                copyfile(mapred_site_xml, preunravel_mapred_site)

            tree = ET.parse(mapred_site_xml)
            root = tree.getroot()
            # val_list[0] is value, val_list[1] is description
            for config, val_list in self.configs['mapred-site'].iteritems():
                find_property = False
                for property in root.findall('property'):
                    property_name = property.find('name')
                    property_value = property.find('value')
                    if val_list[0] in property_value.text and property_name.text == config:
                        print("{0} {1:>{width}}".format(config, "No change needed", width=80-len(config)))
                        find_property = True
                        if argv.verbose: print_verbose(val_list[0])
                        break
                    elif config == property_name.text:
                        print("{0} {1:>{width}}".format(config, "Missing value", width=80-len(config)))
                        if argv.verbose: print_verbose(property_value.text, val_list[0])
                        if not property_name.text == ('mapreduce.task.profile.params' or 'yarn.app.mapreduce.am.command-opts'):
                            property_value.text = val_list[0]
                        else:
                            property_value.text = property_value.text + ' ' + val_list[0]
                        find_property = True
                        if not argv.dry_test: tree.write(mapred_site_xml)
                        break
                if not find_property:
                    print("{0} {1:>{width}}".format(config, "missing", width=80-len(config)))
                    xml_str = self.generate_xml_property(config, val_list)
                    new_property = ET.fromstring(xml_str)
                    root.append(new_property)
                    if not argv.dry_test: tree.write(mapred_site_xml)
        except Exception as e:
            print("Error: " + str(e))

    def update_unravel_properties(self):
        print("\nChecking Unravel properties")
        unravel_properties_path = '/usr/local/unravel/etc/unravel.properties'
        headers = "# required for MapR\n"
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
                        for orgin_config in find_configs:
                            if val in orgin_config:
                                correct_flag = True
                                break
                        if correct_flag:
                            print("{0} {1:>{width}}".format(config, "No change needed", width=80 - len(config)))
                            if argv.verbose: print_verbose(orgin_config)
                        else:
                            print("{0} {1:>{width}}".format(config, "Missing value", width=80 - len(config)))
                            new_config += '%s=%s\n' % (config, val)
                            if argv.verbose: print_verbose(orgin_config, new_config)
                    else:
                        print("{0} {1:>{width}}".format(config, "missing", width=80-len(config)))
                        new_config += '%s=%s\n' % (config, val)
                        if argv.verbose: print_verbose('None', new_config)
                if len(new_config.split('\n')) > 1 and not argv.dry_test:
                    with open(unravel_properties_path, 'a') as f:
                        f.write(headers + new_config)
                        f.close()
                    sleep(3)
                    print('Update Successful!')
            except Exception as e:
                print('Error: ' + str(e))
        else:
            print("unravel.properties not found skip update unravel.properties")

    def update_yarn_site(self):
        print("\nChecking yarn-site.xml")
        try:
            if argv.hadoop_path:
                conf_base_path = argv.hadoop_path
            else:
                conf_base_path = glob('/opt/mapr/hadoop/hadoop-*.*.*/etc/hadoop/')[-1]
            yarn_site_xml = os.path.join(conf_base_path, 'yarn-site.xml')
            preunravel_yarn_site = os.path.join(conf_base_path, 'yarn-site.xml.preunravel')

            if not os.path.exists(preunravel_yarn_site) and not argv.dry_test:
                print("Backup original yarn-site.xml")
                copyfile(yarn_site_xml, preunravel_yarn_site)

            tree = ET.parse(yarn_site_xml)
            root = tree.getroot()
            for config, val_list in self.configs['yarn-site'].iteritems():
                for property in root.findall('property'):
                    find_property = False
                    property_name = property.find('name')
                    property_value = property.find('value')
                    if val_list[0] in property_value.text and property_name.text == config:
                        print("{0} {1:>{width}}".format(config, "No change needed", width=80-len(config)))
                        find_property = True
                        if argv.verbose: print_verbose(val_list[0])
                        break
                    elif config == property_name.text:
                        print("{0} {1:>{width}}".format(config, "Missing value", width=80-len(config)))
                        if argv.verbose: print_verbose(property_value.text, val_list[0])
                        property_value.text = val_list[0]
                        find_property = True
                        if not argv.dry_test: tree.write(yarn_site_xml)
                        break
                if not find_property:
                    print("{0} {1:>{width}}".format(config, "missing", width=80-len(config)))
                    if not config == 'yarn.resourcemanager.webapp.address':
                        xml_str = self.generate_xml_property(config, val_list)
                        new_property = ET.fromstring(xml_str)
                        root.append(new_property)
                        if argv.verbose: print_verbose('None', val_list[0])
                        if not argv.dry_test: tree.write(yarn_site_xml)
                    else:
                        xml_str = self.generate_xml_property(config, val_list, default=False)
                        new_property = ET.fromstring(xml_str)
                        root.append(new_property)
                        if argv.verbose: print_verbose('None', val_list[0])
                        if not argv.dry_test: tree.write(yarn_site_xml)
        except Exception as e:
            print(e)
            print("Skip yarn-site")

    def get_resourcemanager_host(self):
        try:
            result_file = '/tmp/unravel_request.log'
            Popen('maprcli node list resourcemanager -json > %s' % result_file, shell=True, stdout=PIPE)
            sleep(5)
            with open(result_file, 'r') as f:
                content = json.loads(f.read())
                f.close()
            sleep(1)
            os.remove(result_file)
            for index, content_data in enumerate(content['data']):
                if re.search('resourcemanager', content_data['service']):
                    return content_data['hostname']
                else:
                    continue
            print("Resource manager host not found")
            return 'localhost'
        except Exception as e:
            # print(e)
            print("Failed to get resource manager host")
            return 'None'

    # Get hosts list from maprcli
    def get_hosts_list(self):
        try:
            maprcli_get_hosts = Popen('maprcli node list -json', shell=True, stdout=PIPE).communicate()[0].strip()
            hosts_list = []
            for data in json.loads(maprcli_get_hosts)['data']:
                hosts_list.append(data['hostname'])
            return hosts_list
        except Exception as e:
            print(e)
            print("Failed to get node list from maprcli only current host will be configured")
            return [os.uname()[1]]

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

def print_verbose(cur_val, sug_val=None):
    print('Current Configuration: ' + str(cur_val))
    if sug_val:
        print('Suggest Configuration: ' + str(sug_val))


# Download hive-hook jar and spark sensor zip function shared in MapR and HDP
def deploy_unravel_sensor(unravel_base_url, hive_version_xyz):
    unravel_sensor_url = 'http://{unravel_base_url}:{unravel_port}/hh/'.format(unravel_base_url=unravel_base_url, unravel_port=argv.unravel_port)
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

            with open(save_path,'wb') as f:
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
    mapr_setup = MaprSetup()
    deploy_sensor_result = deploy_unravel_sensor(mapr_setup.unravel_base_url, mapr_setup.hive_version_xyz)
    if argv.sensor_only:
        exit()

    # if hive sensor install succefully do instrumentation
    if deploy_sensor_result[0]:
        mapr_setup.update_hive_site()
        mapr_setup.update_hive_env()
        mapr_setup.update_hadoop_env()
    else:
        print("\nInstall Hive Hook Sensor Failed skip hive instrumentation")

    # if spark & MR sensor install succefully do instrumentation
    if deploy_sensor_result[1]:
        mapr_setup.update_spark_defaults()
        mapr_setup.update_mapred_site()
    else:
        print("\nInstall Spark & MR Sensor Failed skip spark & MR instrumentation")

    mapr_setup.update_unravel_properties()
    mapr_setup.update_yarn_site()
    if argv.dry_test:
        print('\nThe script is running in dry run mode no configuration will be changed')
    else:
        print('\nUpdaing incorrect or missing configuration')
        sleep(5)
    # mapr_setup.get_hosts_list()


if __name__ == '__main__':
    main()
