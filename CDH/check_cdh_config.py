#!/usr/bin/python
from subprocess import call, Popen, PIPE
try:
    from cm_api.api_client import ApiResource
    from cm_api.endpoints.types import ApiClusterTemplate
    from cm_api.endpoints.cms import ClouderaManager
    from termcolor import colored
except:
    call(['sudo', 'yum' , '-y', '--enablerepo=extras', 'install', 'epel-release'])
    call(['sudo', 'yum' , '-y', 'install', 'python-pip'])
    call(['sudo', 'pip', 'install', 'cm-api'])
    call(['sudo', 'pip', 'install', 'termcolor'])
    call(['sudo', 'pip', 'install', 'requests'])
import re
import os
import pwd
import json
import argparse
import requests


parser = argparse.ArgumentParser()
parser.add_argument("--spark-version", help="spark version e.g. 1.6 or 2.2", required=True, dest='spark_ver')
parser.add_argument("--cm_host", help="hostname of CM Server, default is local host", dest='cm_hostname', required=True)
parser.add_argument("--unravel-host", help="Unravel Server hostname", dest='unravel', required=False)
parser.add_argument("-user", "--user", help="CM Username", default='admin')
parser.add_argument("-pass", "--password", help="CM Password", default='admin')
parser.add_argument("-uuser", "--unravel_username", help="Unravel UI Username", default='admin')
parser.add_argument("-upass", "--unravel_password", help="Unravel UI Password", default='unraveldata')
argv = parser.parse_args()

if not argv.cm_hostname:
    argv.cm_hostname = Popen(['hostname'], stdout=PIPE).communicate()[0].strip()

if not argv.unravel:
    argv.unravel = Popen(['hostname'], stdout=PIPE).communicate()[0].strip()
    unravel_ip = Popen(['host', argv.unravel], stdout=PIPE).communicate()[0].strip()
else:
    if re.match('[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}',argv.unravel):
        unravel_ip = argv.unravel
        try:
            unravel_hostname = Popen(['host', argv.unravel], stdout=PIPE).communicate()[0].strip().split('domain name pointer ')
            argv.unravel = unravel_hostname[1][:-1]
        except:
            pass
    else:
        unravel_ip = re.search('[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}',
                                          Popen(['host', argv.unravel], stdout=PIPE).communicate()[0].strip()).group(0)

print("Unravel Hostname: %s\n" % argv.unravel)
print("Unravel IP: %s\n" % unravel_ip)

if re.search('1.[0-9]', argv.spark_ver):
    spark_ver = re.search('1.[0-9]', argv.spark_ver).group(0)
else:
    spark_ver = None
if re.search('2.[0-9]', argv.spark_ver):
    spark2_ver = re.search('2.[0-9]', argv.spark_ver).group(0)
else:
    spark2_ver = None


def generate_configs(unravel_host):
    configs = {}
    configs['hive-env'] = 'AUX_CLASSPATH=${AUX_CLASSPATH}:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/unravel_hive_hook.jar'
    configs['mapred-site'] = '''<property><name>mapreduce.task.profile</name><value>true</value></property>
<property><name>mapreduce.task.profile.maps</name><value>0-5</value></property>
<property><name>mapreduce.task.profile.reduces</name><value>0-5</value></property>
<property><name>mapreduce.task.profile.params</name><value>-javaagent:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/btrace-agent.jar=libs=mr -Dunravel.server.hostport=%s:4043</value></property>''' % unravel_host
    configs['hadoop-env'] = 'HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/unravel_hive_hook.jar'
    configs['yarn-am'] = '-Djava.net.preferIPv4Stack=true -javaagent:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/btrace-agent.jar=libs=mr -Dunravel.server.hostport=%s:4043' % unravel_host
    configs['spark-defaults'] = '''spark.unravel.server.hostport=%s:4043
spark.driver.extraJavaOptions=-javaagent:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/btrace-agent.jar=config=driver,libs=spark-%s
spark.executor.extraJavaOptions=-javaagent:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/btrace-agent.jar=config=executor,libs=spark-%s''' % (unravel_host, spark_ver, spark_ver)
    configs['spark2-defaults'] = '''spark.unravel.server.hostport=%s:4043
spark.driver.extraJavaOptions=-javaagent:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/btrace-agent.jar=config=driver,libs=spark-%s
spark.executor.extraJavaOptions=-javaagent:/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/btrace-agent.jar=config=executor,libs=spark-%s''' % (unravel_host, spark2_ver, spark2_ver)
    configs['hive-site'] = '''<property>
  <name>com.unraveldata.host</name>
  <value>%s</value>
  <description>Unravel hive-hook processing host</description>
</property>

<property>
  <name>com.unraveldata.hive.hook.tcp</name>
  <value>true</value>
</property>

<property>
  <name>com.unraveldata.hive.hdfs.dir</name>
  <value>/user/unravel/HOOK_RESULT_DIR</value>
  <description>destination for hive-hook, Unravel log processing</description>
</property>

<property>
  <name>hive.exec.driver.run.hooks</name>
  <value>com.unraveldata.dataflow.hive.hook.HiveDriverHook</value>
  <description>for Unravel, from unraveldata.com</description>
</property>

<property>
  <name>hive.exec.pre.hooks</name>
  <value>com.unraveldata.dataflow.hive.hook.HivePreHook</value>
  <description>for Unravel, from unraveldata.com</description>
</property>

<property>
  <name>hive.exec.post.hooks</name>
  <value>com.unraveldata.dataflow.hive.hook.HivePostHook</value>
  <description>for Unravel, from unraveldata.com</description>
</property>

<property>
  <name>hive.exec.failure.hooks</name>
  <value>com.unraveldata.dataflow.hive.hook.HiveFailHook</value>
  <description>for Unravel, from unraveldata.com</description>
</property>
''' % argv.unravel
    return configs

cm_username = argv.user
cm_pass = argv.password

web_api = requests.Session()
res = web_api.post("http://%s:7180/j_spring_security_check" % argv.cm_hostname, data={'j_username':cm_username,'j_password':cm_pass})
suggest_configs = generate_configs(argv.unravel)
suggest_configs_ip = generate_configs(unravel_ip)

cdh_info = web_api.get("http://%s:7180/api/v11/clusters/" % argv.cm_hostname)
cdh_version = json.loads(cdh_info.text)['items'][0]['fullVersion'].split('.')
resource = ApiResource(argv.cm_hostname, 7180, cm_username, cm_pass, version=11)
cluster_name = json.loads(cdh_info.text)['items'][0]['displayName']
cdh_version_short = '%s.%s' % (cdh_version[0], cdh_version[1])

print("CDH Version: %s" % cdh_version_short)

cluster = resource.get_all_clusters()[0]

def check_cdh_config():

    for c in cluster.get_all_services():
        if c.type == 'HIVE':
            hive = c
        if c.type == 'YARN':
            yarn = c

    h_groups = []
    for group in cluster.get_service('hive').get_all_role_config_groups():
        # print(group.roleType)
        if group.roleType == 'GATEWAY':
            h_groups.append(group)
            # print(h_groups)
        if group.roleType == 'HIVESERVER2':
            h_groups.append(group)

    for groups in h_groups:
        for name, config in groups.get_config(view='full').items():
            # Gateway Client Environment Advanced Configuration Snippet (Safety Valve) for hive-env.sh
            try:
                if name == 'hive_client_env_safety_valve':
                    print('------------------------------------------------------------------')
                    if suggest_configs['hive-env'] == config.value.strip() or suggest_configs_ip['hive-env'] == config.value.strip():
                        print(colored('\nGateway Client Environment Advanced Configuration Snippet (Safety Valve) for hive-env.sh found\n', 'green'))
                        print(config.value)
                    else:
                        print(colored('\nGateway Client Environment Advanced Configuration Snippet (Safety Valve) for hive-env.sh found\n', 'yellow'))
                        print(colored('Current Value:\n' + config.value + '\n', 'red'))
                        print(colored('Suggest Value:\n' + suggest_configs['hive-env'], 'green', attrs=['reverse']))
            except:
                printRed('\nGateway Client Environment Advanced Configuration Snippet (Safety Valve) for hive-env.sh NOT found\n')
                print('Suggest Value:\n' + suggest_configs['hive-env'])
            # Hive Client Advanced Configuration Snippet (Safety Valve) for hive-site.xml
            try:
                if name == 'hive_client_config_safety_valve':
                    print('------------------------------------------------------------------')
                    if  argv.unravel in config.value or unravel_ip in config.value:
                        print (colored('\nHive Client Advanced Configuration Snippet (Safety Valve) for hive-site.xml found\n', 'green'))
                        # print(name,hive_site_snip)
                        print(config.value)
                    else:
                        print (colored('\nHive Client Advanced Configuration Snippet (Safety Valve) for hive-site.xml found\n', 'yellow'))
                        print(colored('Current Value:\n' + config.value + '\n', 'red'))
                        print(colored('Suggest Value:\n' + suggest_configs['hive-site'], 'green', attrs=['reverse']))
            except:
                printRed('\nHive Client Advanced Configuration Snippet (Safety Valve) for hive-site.xml NOT found\n')
                print('Suggest Value:\n' + suggest_configs['hive-site'])

            # HiveServer2 Advanced Configuration Snippet (Safety Valve) for hive-site.xml
            try:
                if name == 'hive_hs2_config_safety_valve':
                    print('------------------------------------------------------------------')
                    if argv.unravel in config.value or unravel_ip in config.value:
                        print(colored('\nFound hive server 2 found\n', 'green'))
                        print(config.value)
                    else:
                        print(colored('\nFound hive server 2 found\n', 'yellow'))
                        print(colored('Current Value:\n' + config.value + '\n', 'red'))
                        print(colored('Suggest Value:\n' + suggest_configs['hive-site'], 'green', attrs=['reverse']))
            except:
                printRed('\nHive server 2 config NOT found\n')
                print('Suggest Value:\n' + suggest_configs['hive-site'])

    y_groups = []
    for group in cluster.get_service('yarn').get_all_role_config_groups():
        if group.roleType == 'GATEWAY':
            y_groups.append(group)

    for name, config in y_groups[0].get_config(view='full').items():
        # Gateway Client Environment Advanced Configuration Snippet (Safety Valve) for hadoop-env.sh
        try:
            if name == 'mapreduce_client_env_safety_valve':
                print('------------------------------------------------------------------')
                if suggest_configs['hadoop-env'] == config.value.strip() or suggest_configs_ip['hadoop-env'] == config.value.strip():
                    print(colored('\nYarn Hook ENV found (hadoop-env)\n', 'green'))
                    print(config.value)
                else:
                    print(colored('\nYarn hadoop-env Hook ENV found (hadoop-env)\n', 'yellow'))
                    print(colored('Current Value:\n' + config.value + '\n', 'red'))
                    print(colored('Suggest Value:\n' + suggest_configs['hadoop-env'], 'green', attrs=['reverse']))
        except:
            printRed('\nYarn Hook ENV NOT found (hadoop-env)\n')
            print('Suggest Value:\n' + suggest_configs['hadoop-env'])

        # MapReduce Client Advanced Configuration Snippet (Safety Valve) for mapred-site.xml
        try:
            if name == 'mapreduce_client_config_safety_valve':
                print('------------------------------------------------------------------')
                if suggest_configs['mapred-site'].replace('\n','') == config.value.strip().replace('\n','') or suggest_configs_ip['mapred-site'].replace('\n','') == config.value.strip().replace('\n',''):
                    print(colored('\nMapReduce Client Config for mapred-site.xml\n', 'green'))
                    print(config.value)
                else:
                    print(colored('\nMapReduce Client Clinfig for mapred-site.xml\n', 'yellow'))
                    print(colored('Current Value:\n' + config.value + '\n', 'red'))
                    print(colored('Suggest Value:\n' + suggest_configs['mapred-site'], 'green', attrs=['reverse']))
        except:
            printRed('\nMapReduce Client Clinfig for mapred-site.xml NOT found\n')
            print('Suggest Value:\n' + suggest_configs['mapred-site'])

        # ApplicationMaster Java Opts Base
        try:
            if name == 'yarn_app_mapreduce_am_command_opts':
                print('------------------------------------------------------------------')
                if suggest_configs['yarn-am'] == config.value or suggest_configs_ip['yarn-am'] == config.value:
                    print(colored('\nFound Yarn Mapreduce AM command\n', 'green'))
                    print(config.value)
                else:
                    print(colored('\n Yarn Mapreduce AM command\n', 'yellow'))
                    print(colored('Current Value:\n' + config.value + '\n', 'red'))
                    print(colored('Suggest Value:\n' + suggest_configs['yarn-am'], 'green', attrs=['reverse']))
        except:
            printRed('\nYarn Mapreduce AM command NOT found\n')
            print('Suggest Value:\n' + suggest_configs['yarn-am'])


    s_groups = []
    for group in cluster.get_service('spark_on_yarn').get_all_role_config_groups():
        if group.roleType == 'GATEWAY':
            s_groups.append(group)

    if spark_ver:
        for name, config in s_groups[0].get_config(view='full').items():
            # Spark Client Advanced Configuration Snippet (Safety Valve) for spark-conf/spark-defaults.conf
            try:
                if name == 'spark-conf/spark-defaults.conf_client_config_safety_valve':
                    print('------------------------------------------------------------------')
                    if suggest_configs['spark-defaults'] == config.value.strip().replace('\n','') or suggest_configs_ip['spark-defaults'] == config.value.strip().replace('\n',''):
                        print(colored('\nSpark-defaults found\n', 'green'))
                        print(config.value)
                    else:
                        print(colored('\nSpark-defaults found\n', 'yellow'))
                        print(colored('Current Value:\n' + config.value + '\n', 'red'))
                        print(colored('Suggest Value:\n' + suggest_configs['spark-defaults'], 'green', attrs=['reverse']))
            except:
                printRed('\nSpark-defaults NOT found\n')
                print('Suggest Value:\n' + suggest_configs['spark-defaults'])

    if spark2_ver:
        s2_groups = []
        try:
            for group in cluster.get_service('spark2_on_yarn').get_all_role_config_groups():
                if group.roleType == 'GATEWAY':
                    s2_groups.append(group)

            for name, config in s2_groups[0].get_config(view='full').items():
                # Spark Client Advanced Configuration Snippet (Safety Valve) for spark-conf/spark-defaults.conf
                if name == 'spark2-conf/spark-defaults.conf_client_config_safety_valve':
                    print('------------------------------------------------------------------')
                    if suggest_configs['spark2-defaults'] == config.value.strip().replace(' ','').replace('\n','') or suggest_configs_ip['spark2-defaults'] == config.value.strip().replace(' ','').replace('\n',''):
                        print(colored('\nSpark2-defaults found\n', 'green'))
                        print(config.value)
                    else:
                        print(colored('\nSpark2-defaults found\n', 'yellow'))
                        print(colored('Current Value:\n' + config.value + '\n', 'red'))
                        print(colored('Suggest Value:\n' + suggest_configs['spark2-defaults'], 'green', attrs=['reverse']))
        except:
            printRed('\nSpark2-defaults NOT found\n')
            print('Suggest Value:\n' + suggest_configs['spark2-defaults'])


def check_parcels():
    print('------------------------------------------------------------------')
    print('\nChecking Cloudera Manager Parcels Status\n')
    found_parcel = False
    for parcel in cluster.get_all_parcels():
        if parcel.product == 'UNRAVEL_SENSOR':
            found_parcel = True
            if cdh_version_short in parcel.version and parcel.stage == 'ACTIVATED':
                printGreen(parcel.version + ' state: ' + parcel.stage)
            else:
                printYellow(parcel.version + ' state: ' + parcel.stage)
    if not found_parcel:
        printRed('Unravel Sensor Parcel not exists')


def check_unravel_properties():
    print('------------------------------------------------------------------')
    print('\n\nChecking Unravel Properties\n')
    hive_config = cluster.get_service('hive').get_config()[0]
    hive_connection = 'jdbc:{db_type}://{host}:{port}/{db_name}'.format(db_type=hive_config['hive_metastore_database_type'],
                                                        host=hive_config['hive_metastore_database_host'],
                                                        port=hive_config['hive_metastore_database_port'],
                                                        db_name=hive_config['hive_metastore_database_name'])
    hive_password = hive_config['hive_metastore_database_password']
    try:
        file_path ='/usr/local/unravel/etc/unravel.properties'
        file_stat = os.stat(file_path)
        file_owner = pwd.getpwuid(file_stat.st_uid).pw_name

        #Unravel Folder Owner
        print('------------------------------------------------------------------')
        print('Unravel Folder Owner\n')
        if file_owner == 'hdfs':
            printGreen(file_owner)
        else:
            printYellow(file_owner)
            printYellow('Unravel Folder Owner is not hdfs please run switch_to_user script')

        with open(file_path, 'r') as file:
            unravel_properties = file.read()
            file.close()

        #javax.jdo.option.ConnectionURL
        print('------------------------------------------------------------------')
        if hive_connection in unravel_properties:
            print(colored('javax.jdo.option.ConnectionURL Correct', 'green'))
            print(re.search('javax.jdo.option.ConnectionURL=.*?\n',unravel_properties).group(0))
        else:
            print(colored('javax.jdo.option.ConnectionURL Wrong', 'yellow'))
            try:
                print(colored('Current Value:\n' + re.search('javax.jdo.option.ConnectionURL=.*?\n',unravel_properties).group(0), 'red'))
            except:
                print('javax.jdo.option.ConnectionURL not in /usr/local/unravel/etc/unravel.properties')
            print(colored('Suggesst Value:\n' + 'javax.jdo.option.ConnectionURL=' + hive_connection, 'green', attrs=['reverse']))


        #javax.jdo.option.ConnectionPassword
        print('------------------------------------------------------------------')
        if hive_password in unravel_properties:
            print(colored('javax.jdo.option.ConnectionPassword', 'green'))
            print(re.search('javax.jdo.option.ConnectionPassword=.*?\n',unravel_properties).group(0))
        else:
            print(colored('javax.jdo.option.ConnectionPassword Wrong', 'yellow'))
            try:
                print(colored('Current Value:\n' + re.search('javax.jdo.option.ConnectionPassword=.*?\n',unravel_properties).group(0), 'red'))
            except:
                print('javax.jdo.option.ConnectionPassword not in /usr/local/unravel/etc/unravel.properties')
            print(colored('Suggesst Value:\n' + 'javax.jdo.option.ConnectionPassword='+ hive_password, 'green', attrs=['reverse']))
    except Exception as e:
        printRed(e)
        pass

def get_daemon_status():
    unravel_base_url = 'http://%s:3000/api/v1/' % argv.unravel
    print('------------------------------------------------------------------')
    print('\nChecking Unravel Daemon Status\n')
    try:
        login_token = json.loads(requests.post(unravel_base_url + 'signIn', data={"username": argv.unravel_username,
                                                                                  "password": argv.unravel_password}).text)['token']
        print('Unravel Sigin Token: %s\n' % login_token)

        daemon_status = json.loads(requests.get(unravel_base_url + 'manage/daemons_status',
                                                 headers = {'Authorization': 'JWT %s' % login_token}).text)


        for daemon in daemon_status.iteritems():
            if len(daemon[1]['errorMessages']) == 0 and len(daemon[1]['fatalMessages']) == 0:
                print(colored(daemon[0], 'green'))
            else:
                message = ''
                if daemon[1]['errorMessages']:
                    message += printYellow(daemon[1]['errorMessages'][0]['msg'], do_print=False)
                if daemon[1]['fatalMessages']:
                    message += printRed(daemon[1]['fatalMessages'][0]['msg'], do_print=False)
                print(daemon[0] + ': %s' % message )
    except Exception as e:
        print(e)
        printRed('\n[Error]: Couldn\'t connect to Unravel Daemons UI')
        # raise requests.exceptions.ConnectionError('Unable to connect to Unravel host: %s \nCheck Unravel Server Status or /usr/local/unravel/logs for more details' % argv.unravel)


def printGreen(print_str, do_print=True, attrs_list=None):
    if do_print:
        print(colored(print_str, 'green', attrs=attrs_list))
    else:
        return(colored(print_str, 'green', attrs=attrs_list))


def printYellow(print_str, do_print=True, attrs_list=None):
    if do_print:
        print(colored(print_str, 'yellow', attrs=attrs_list))
    else:
        return(colored(print_str, 'yellow', attrs=attrs_list))


def printRed(print_str, do_print=True, attrs_list=None):
    if do_print:
        print(colored(print_str, 'red', attrs=attrs_list))
    else:
        return(colored(print_str, 'red', attrs=attrs_list))


def main():
    check_cdh_config()
    check_parcels()
    check_unravel_properties()
    get_daemon_status()

if __name__ == '__main__':
    main()
