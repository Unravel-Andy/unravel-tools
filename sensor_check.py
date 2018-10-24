#! /usr/bin/python
import os
import re
try:
    import urllib2
except:
    import urllib
import base64
import json
import argparse
from subprocess import PIPE, Popen

parser = argparse.ArgumentParser()
parser.add_argument('--username', default='admin', help='username used to log in Ambari or CM default is admin')
parser.add_argument('--password', default='admin', help='password used to log in Ambari or CM default is admin')
parser.add_argument('--port', help='default server port 8080 for HDP or 7180 for CDH')
parser.add_argument('--https', action='store_true', default=False)
argv = parser.parse_args()

# check unravel sensor version across the cluster
class SensorCheck:
    def __init__(self):
        self.cluster_type = self.get_cluster_type()
        self.hosts_list = self.get_cluster_hosts()

    def get_cluster_type(self):
        hadoop_popen = Popen('hadoop version', stderr=PIPE, stdout=PIPE, shell=True)
        hadoop_version = hadoop_popen.communicate()[0]
        cluster_type = 'UNKNOWN'
        if hadoop_popen.returncode == 0:
            if 'cloudera' in hadoop_version:
                cluster_type = 'CDH'
            elif 'hortonworks' in hadoop_version:
                cluster_type = 'HDP'
            elif 'mapr' in hadoop_version:
                cluster_type = 'MAPR'
        return cluster_type

    def get_cluster_hosts(self):
        if self.cluster_type == 'CDH':
            return self.get_cdh_hosts()
        elif self.cluster_type == 'HDP':
            return self.get_hdp_hosts()
        elif self.cluster_type == 'MAPR':
            return self.get_mapr_hosts()
        else:
            return []

    def get_request(self, hostname, port, api, https=False):
        if https:
            req_url = 'https://{}:{}/{}'.format(hostname, port, api)
        else:
            req_url = 'http://{}:{}/{}'.format(hostname, port, api)

        request = urllib2.Request(req_url)
        base64string = base64.b64encode('%s:%s' % (argv.username, argv.password))
        request.add_header("Authorization", "Basic %s" % base64string)
        try:
            res = urllib2.urlopen(request, timeout=10)
            return json.loads(res.read())
        except Exception as e:
            print(e)
            print(req_url)
            return {}

    def get_cdh_hosts(self):
        agent_ini = '/etc/cloudera-scm-agent/config.ini'
        hosts_list = []
        if os.path.exists(agent_ini):
            host_name = re.search('\s(server_host=)(.*)', open(agent_ini, 'r').read()).group(2)
            if argv.port:
                res = self.get_request(host_name, argv.port, 'api/v11/hosts', https=argv.https)
            else:
                res = self.get_request(host_name, 7180, 'api/v11/hosts')
            for host in res['items']:
                hosts_list.append(host['hostname'])
            return(hosts_list)
        else:
            print('%s not exists' % agent_ini)
        return hosts_list

    def get_hdp_hosts(self):
        agent_ini = '/etc/ambari-agent/conf/ambari-agent.ini'
        hosts_list = []
        if os.path.exists(agent_ini):
            host_name = re.search('\s(hostname=)(.*)', open(agent_ini, 'r').read()).group(2)
            if argv.port:
                res = self.get_request(host_name, argv.port, 'api/v1/hosts', https=argv.https)
            else:
                res = self.get_request(host_name, 8080, 'api/v1/hosts')
            for host in res['items']:
                hosts_list.append(host['Hosts']['host_name'])
            return (hosts_list)
        else:
            print('%s not exists' % agent_ini)
        return hosts_list

    def get_mapr_hosts(self):
        mapr_popen = Popen('maprcli node list -json', shell=True, stdout=PIPE, stderr=PIPE)
        popen_result = mapr_popen.communicate()
        hosts_list = []
        if mapr_popen.returncode == 0:
            for node in json.loads(popen_result[0])['data']:
                hosts_list.append(node['hostname'])
        else:
            print(popen_result[1])
        return hosts_list

    def get_sensor_version(self):
        for host in self.hosts_list:
            ssh_result = self.ssh_command(host)
            if not ssh_result == 'None':
                sensor_version = re.search('(Unravel Version:)(.*)', ssh_result).group(2)
                md5sum = ssh_result.split('\n')[-2]
                print('{}: {}'.format(host, sensor_version))
                print('hive hook md5sum: {}\n'.format(md5sum))

    def ssh_command(self, host_name, ssh_user='root'):
        if self.cluster_type == 'MAPR' or self.cluster_type == 'HDP':
            version_path = '/usr/local/unravel-agent/jars/version.txt'
            spark_sensor_path = '/usr/local/unravel-agent/jars/'
            hive_sensor_path = '/usr/local/unravel_client/*'
            ssh_popen = Popen('ssh {0}@{1} \'cat {2}; md5sum {3}\''.format(ssh_user, host_name, version_path, hive_sensor_path), shell=True, stdout=PIPE, stderr=PIPE)
        else:
            version_path = '/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/version.txt'
            spark_sensor_path = '/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/'
            hive_sensor_path = '/opt/cloudera/parcels/UNRAVEL_SENSOR/lib/java/unravel_hive_hook.jar'
            ssh_popen = Popen(
                'ssh {0}@{1} \'cat {2}; md5sum {2}\''.format(ssh_user, host_name, version_path, hive_sensor_path), shell=True,
                stdout=PIPE, stderr=PIPE)
        ssh_result = ssh_popen.communicate()
        if ssh_popen.returncode == 0:
            return ssh_result[0]
        else:
            print(host_name)
            print(ssh_result[1])
            return 'None'


def get_installed_unravel_version():
    unravel_ver_file = '/usr/local/unravel/ngui/www/version.txt'
    if os.path.exists(unravel_ver_file):
        unravel_ver = re.search('(UNRAVEL_VERSION=)(.*)', open(unravel_ver_file, 'r').read()).group(2)
        return unravel_ver
    else:
        return 'None'


main = SensorCheck()
print('Getting Unravel Sensor Version')
print('Cluster type: %s' % main.cluster_type)
print('Installed Unravel Version: %s' % get_installed_unravel_version())
main.get_sensor_version()