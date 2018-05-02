#!/usr/bin/python
# v1.0.0
import re
import os
import pwd
import json
import argparse
from termcolor import colored
import requests
from subprocess import call, Popen, PIPE

parser = argparse.ArgumentParser()
parser.add_argument("--spark-version", help="spark version e.g. 1.6 or 2.1", required=True, dest='spark_ver')
parser.add_argument("--hive-version", help="hive version e.g. 1.2", dest='hive_ver', required=True)
parser.add_argument("--am_host", help="hostname of Ambari Server, default is current host")
parser.add_argument("--username", help="Ambari Log in username, default is admin", default='admin')
parser.add_argument("--password", help="Ambari Log in password, default is admin", default='admin')
parser.add_argument("--unravel-host", help="Unravel Server hostname", dest='unravel')
parser.add_argument("-uuser", "--unravel_username", help="SSH Login for Unravel Host, default use localhost", default='admin')
parser.add_argument("-upass", "--unravel_password", help="SSH password for Unravel Host, default is local no password", default='unraveldata')
argv = parser.parse_args()

if not argv.am_host:
    argv.am_host = Popen(['hostname'], stdout=PIPE).communicate()[0].strip()

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
print('Unravel Hostname: ' + argv.unravel)
print('Unravel Host IP: ' + unravel_ip)
if not argv.hive_ver:
    argv.hive_ver = Popen('$(which hive) --version 2>/dev/null | grep -Po \'Hive \K([0-9]+\.[0-9]+\.[0-9]+)\'', shell=True, stdout=PIPE).communicate()[0].strip()
argv.hive_ver = argv.hive_ver.split('.')

session = requests.Session()
session.auth = (argv.username, argv.password)

def check_hdp_config():
    # configuration with hostname
    unravel_configs = generate_configs(argv.unravel)
    # configuration with ip address
    unravel_configs_ip = generate_configs(unravel_ip)

    # check hive-env value
    print('------------------------------------------------------------------')
    print('\nChecking hive-env\n')
    try:
        hive_env = get_config('hive-env')['content']
        if hive_env and unravel_configs['hive-env'] in hive_env:
            printGreen('AUX_CLASSPATH is in hive-env')
            print(unravel_configs['hive-env'])
        else:
            printRed('AUX_CLASSPATH is NOT in hive-env')
            printYellow('Please add the following line in hive-env: \n%s' % unravel_configs['hive-env'])
    except Exception as e:
        printRed(e)

    # check hadoop-env
    print('------------------------------------------------------------------')
    print('\nChecking hadoop-env\n')
    try:
        hadoop_env = get_config('hadoop-env')['content']
        if hadoop_env and unravel_configs['hadoop-env'] in hadoop_env:
            printGreen('HADOOP_CLASSPATH is in hadoop-env')
            print(unravel_configs['hadoop-env'])
        else:
            printRed('HADOOP_CLASSPATH is NOT in hadoop-env')
            printYellow('Please add the following line in hive-env: \n%s' % unravel_configs['hadoop-env'])
    except Exception as e:
        printRed(e)

    # check hive-site
    print('------------------------------------------------------------------')
    print('\nChecking hive-site.xml\n')
    try:
        hive_site = get_config('hive-site')
        for config, val in unravel_configs['hive-site'].iteritems():
            if val in hive_site.get(config, '') or unravel_configs_ip['hive-site'][config] in hive_site.get(config, ''):
                printGreen(config + ': ' + hive_site[config])
            else:
                printYellow(config + ': \nCurrent Value: ' + hive_site.get(config, ''))
                printYellow('Suggest Value: ' + val)
    except Exception as e:
        printRed(e)

    # check mapr-site
    print('------------------------------------------------------------------')
    print('\nChecking mapr-site.xml\n')
    try:
        mapred_site = get_config('mapred-site')
        for config, val in unravel_configs['mapred-site'].iteritems():
            if val in mapred_site.get(config, '') or unravel_configs_ip['mapred-site'][config] in mapred_site.get(config, ''):
                printGreen(config + ': ' + mapred_site[config])
            else:
                printYellow(config + ': \nCurrent Value: ' + mapred_site.get(config, ''))
                printYellow('Suggest Value: {current_val} {suggest_val}'.format(current_val=mapred_site.get(config, ''), suggest_val=val))
    except Exception as e:
        printRed(e)

    # check spark-defaults
    if re.search('1.[0-9]', argv.spark_ver):
        print('------------------------------------------------------------------')
        print('\nChecking spark-defaults\n')
        try:
            spark_ver = re.search('1.[0-9]', argv.spark_ver).group(0)
            spark_defaults = get_config('spark-defaults')
            for config, val in unravel_configs['spark-defaults'].iteritems():
                if val in spark_defaults.get(config, '') or unravel_configs_ip['spark-defaults'][config] in spark_defaults.get(config, ''):
                    printGreen(config + ': ' + spark_defaults[config])
                else:
                    printYellow(config + ': \nCurrent Value: ' + spark_defaults.get(config, ''))
                    printYellow('Suggest Value: {current_val} {suggest_val}'.format(current_val=spark_defaults.get(config, ''), suggest_val=val))
        except Exception as e:
            printRed(e)
            printRed('Spark-defaults NOT found')

    # check spark2-defaults
    if re.search('2.[0-9]', argv.spark_ver):
        print('------------------------------------------------------------------')
        print('\nChecking spark2-defaults\n')
        try:
            spark2_ver = re.search('2.[0-9]', argv.spark_ver).group(0)
            spark2_defaults = get_config('spark2-defaults')
            for config, val in unravel_configs['spark2-defaults'].iteritems():
                if val in spark2_defaults.get(config, '') or unravel_configs_ip['spark-defaults'][config] in spark2_defaults.get(config, ''):
                    printGreen(config + ': ' + spark2_defaults[config])
                else:
                    printYellow(config + ': \nCurrent Value: ' + spark2_defaults.get(config, ''))
                    printYellow('Suggest Value: {current_val} {suggest_val}'.format(current_val=spark2_defaults.get(config, ''), suggest_val=val))
        except Exception as e:
            printRed(e)
            printRed('Spark2-defaults NOT found')

    # check tez-site
    print('------------------------------------------------------------------')
    print('\nChecking tez-site.xml\n')
    try:
        tez_site = get_config('tez-site')
        for config, val in unravel_configs['tez-site'].iteritems():
            if val in tez_site.get(config, '') or unravel_configs_ip['tez-site'][config] in tez_site.get(config, ''):
                printGreen(config + ': ' + tez_site[config])
            else:
                printYellow(config + ': \nCurrent Value: ' + tez_site.get(config, ''))
                printYellow('Suggest Value: {current_val} {suggest_val}'.format(current_val=tez_site.get(config, ''), suggest_val=val))
    except Exception as e:
        printRed(e)
        printRed('Tez site NOT found')


def generate_configs(unravel_host):
    configs = {}
    configs['hive-env'] = 'export AUX_CLASSPATH=${AUX_CLASSPATH}:/usr/local/unravel_client/unravel-hive-%s.%s.0-hook.jar' % (argv.hive_ver[0],argv.hive_ver[1])
    configs['hadoop-env'] = 'export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/usr/local/unravel_client/unravel-hive-%s.%s.0-hook.jar' % (argv.hive_ver[0],argv.hive_ver[1])
    configs['hive-site'] = {
                            'hive.exec.driver.run.hooks': 'com.unraveldata.dataflow.hive.hook.HiveDriverHook',
                            'com.unraveldata.hive.hdfs.dir': '/user/unravel/HOOK_RESULT_DIR',
                            'com.unraveldata.hive.hook.tcp': 'true',
                            'com.unraveldata.host':unravel_host,
                            'hive.exec.pre.hooks': 'com.unraveldata.dataflow.hive.hook.HivePreHook',
                            'hive.exec.post.hooks': 'com.unraveldata.dataflow.hive.hook.HivePostHook',
                            'hive.exec.failure.hooks': 'com.unraveldata.dataflow.hive.hook.HiveFailHook'
                           }
    if re.search('1.[0-9]', argv.spark_ver):
        configs['spark-defaults'] = {
                                     # 'spark.eventLog.dir':'hdfs:///spark-history',
                                     # 'spark.history.fs.logDirectory':'hdfs:///spark-history',
                                     'spark.unravel.server.hostport':unravel_host+':4043',
                                     'spark.driver.extraJavaOptions':'-Dcom.unraveldata.client.rest.shutdown.ms=300 -javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=spark-%s,config=driver' % (re.search('1.[0-9]', argv.spark_ver).group(0)),
                                     'spark.executor.extraJavaOptions':'-Dcom.unraveldata.client.rest.shutdown.ms=300 -javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=spark-%s,config=executor' % (re.search('1.[0-9]', argv.spark_ver).group(0))
                                    }
    if re.search('2.[0-9]', argv.spark_ver):
        configs['spark2-defaults'] = {
                                     # 'spark.eventLog.dir':'hdfs:///spark-history',
                                     # 'spark.history.fs.logDirectory':'hdfs:///spark-history',
                                     'spark.unravel.server.hostport':unravel_host+':4043',
                                     'spark.driver.extraJavaOptions':'-Dcom.unraveldata.client.rest.shutdown.ms=300 -javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=spark-%s,config=driver' % (re.search('2.[0-9]', argv.spark_ver).group(0)),
                                     'spark.executor.extraJavaOptions':'-Dcom.unraveldata.client.rest.shutdown.ms=300 -javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=spark-%s,config=executor' % (re.search('2.[0-9]', argv.spark_ver).group(0))
                                    }
    configs['mapred-site'] = {
                              'yarn.app.mapreduce.am.command-opts':'-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=mr -Dunravel.server.hostport=%s:4043' % unravel_host,
                              'mapreduce.task.profile':'true',
                              'mapreduce.task.profile.maps':'0-5',
                              'mapreduce.task.profile.reduces':'0-5',
                              'mapreduce.task.profile.params':'-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=mr -Dunravel.server.hostport=%s:4043' % unravel_host
                             }
    configs['tez-site'] = {
                            'tez.am.launch.cmd-opts':'-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=mr,config=tez -Dunravel.server.hostport=%s:4043' % unravel_host,
                            'tez.task.launch.cmd-opts':'-javaagent:/usr/local/unravel-agent/jars/btrace-agent.jar=libs=mr,config=tez -Dunravel.server.hostport=%s:4043' % unravel_host
                          }
    return configs

# Get Configuration from ambari api
def get_config(config_name):
    try:
        base_ambari_url = 'http://%s:8080/api/v1/' % argv.am_host
        cluster_name = json.loads(session.get(base_ambari_url + 'clusters').text)['items'][0]['Clusters']['cluster_name']
        config_latest_version_url = json.loads(session.get(base_ambari_url + 'clusters/{cluster_name}/configurations?type={config_name}'.format(host=argv.am_host, cluster_name=cluster_name, config_name=config_name)).text)['items'][-1]['href']
        config = json.loads(session.get(config_latest_version_url).text)
        return config['items'][0]['properties']
    except Exception as e:
        printRed(e)
        return None


def get_hosts_list():
    try:
        base_ambari_url = 'http://%s:8080/api/v1/' % argv.am_host
        ambari_base_info = json.loads(session.get(base_ambari_url + 'clusters').text)['items'][0]
        cluster_name = ambari_base_info['Clusters']['cluster_name']
        print('Ambari Version: ' + ambari_base_info['Clusters']['version'])
        hosts_info = json.loads(session.get(base_ambari_url + 'clusters/{cluster_name}/hosts'.format(cluster_name=cluster_name)).text)['items']
        hosts_list = []
        for host in hosts_info:
            hosts_list.append(host['Hosts']['host_name'])
        return(hosts_list)
    except Exception as e:
        printRed(e)
        return []


def check_unravel_properties():
    print('------------------------------------------------------------------')
    print('\n\nChecking Unravel Properties\n')

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

        #com.unraveldata.spark.eventlog.location
        if re.search('1.[0-9]', argv.spark_ver):
            spark_default = get_config('spark-defaults')
        elif re.search('2.[0-9]', argv.spark_ver):
            spark_default = get_config('spark2-defaults')
        print('------------------------------------------------------------------')
        if spark_default['spark.eventLog.dir'] in unravel_properties:
            print(colored('com.unraveldata.spark.eventlog.location Correct', 'green'))
            printGreen(re.search('com.unraveldata.spark.eventlog.location=.*?\n', spark_default['spark.eventLog.dir']).group(0))
        else:
            print(colored('com.unraveldata.spark.eventlog.location Wrong', 'yellow'))
            try:
                print(colored('Current Value:\n' + re.search('com.unraveldata.spark.eventlog.location=.*?\n', unravel_properties).group(0), 'red'))
            except:
                print('com.unraveldata.spark.eventlog.location not in /usr/local/unravel/etc/unravel.properties')
            print(colored('Suggesst Value:\n' + 'com.unraveldata.spark.eventlog.location=' + spark_default['spark.eventLog.dir'], 'green', attrs=['reverse']))
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
        if requests.get(unravel_base_url + 'clusters').status_code == 200:
            printRed('\nCheck Unravel Login credentials')
        else:
            printRed('\n[Error]: Couldn\'t connect to Unravel Daemons UI\nPlease Check /usr/local/unravel/logs/ and /var/log/ for unravel_*.log')
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
    get_hosts_list()
    check_hdp_config()
    check_unravel_properties()
    get_daemon_status()


if __name__ == '__main__':
    main()
