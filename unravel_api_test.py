"""
v 0.1.0
"""
import requests
from termcolor import colored
import argparse
import json
import re

parser = argparse.ArgumentParser()
parser.add_argument('-host',"--unravel-host", help="Unravel Server hostname", dest='unravel', required=True)
parser.add_argument('-user','--username', default="admin")
parser.add_argument('-pass','--password', default="unraveldata")
argv = parser.parse_args()

unravel_api = "http://"+argv.unravel+":3000/api/v1"
web_api = requests.Session()
web_api.headers = {'Content-Type':'application/x-www-form-urlencoded'}

def send_res(api_name, action='get', data_in=None, print_res=True):
    try:
        if action == 'get':
            res = web_api.get(unravel_api + api_name)
        elif action == 'post':
            res = web_api.get(unravel_api + api_name, data=data_in)
        if res.status_code == 200:
            print(colored(api_name + " Pass",'green'))
            if print_res:
                print(res.text)
                # print(json.dumps(json.loads(res.text), indent=2))
        else:
            print(colored(api_name + " Warning",'yellow'))
            print(res.status_code)
            print(res.text)
    except:
        print(colored(api_name + " Fail",'red'))
    print('')

# Get Token / login
def test_login():
    global web_api
    try:
        res = web_api.post(unravel_api+"/signIn", data={"username":argv.username, "password":argv.password})

        if res.status_code == 200:
            print(colored("/signIn Pass",'green'))
        token = json.loads(res.text)['token']
        print(token)
        web_api.headers.update({'Authorization':'JWT %s' % token})
    except:
        print((colored("/signIn Fail",'red')))
    print('')


def main():
    res = web_api.get("http://"+argv.unravel+":3000/version.txt")
    try:
        print(colored(re.search('[0-9].[0-9].[0-9]{0,4}[0-9b.]{0,7}',res.text).group(0), "green"))
        print('')
    except:
        print(colored('No Unvravel Version Found','red'))
        print('')
    # test Log in
    test_login()

    # test clusters
    send_res('/clusters')
    send_res("/clusters/nodes")
    send_res("/clusters/resources/cpu/allocated")
    send_res("/clusters/resources/memory/allocated")
    send_res("/clusters/resources/cpu/total")
    send_res("/clusters/resources/memory/total")
    send_res("/clusters/default/nodes")

    # test apps
    send_res("/apps/search",action='post', data_in={"appStatus":["R"],
                                                    "from":0,
                                                    "appTypes":["mr","hive","spark","cascading","pig"]
                                                    })
    send_res("/apps/events/inefficient_apps")
    # send_res("/apps/status/")
    send_res("/apps/status/running/")
    send_res("/apps/status/finished/")
    send_res("/apps/resources/cpu/allocated")
    send_res("/apps/resources/memory/allocated")


    # test workflows
    send_res("/workflows")
    send_res("/workflows/missing_sla")

    # test autoactions
    send_res("/autoactions")
    send_res("/autoactions/recent_violations")

    # test reports
    ## test charge back
    send_res("/search/cb/appt/queue?from=2018-03-21T13:14:36%2B05:30&to=2018-03-28T13:00:56%2B05:30")
    ## test summary and compare
    send_res("/manage/cluster_stats?et=1522268681000&mode=user&st=1521663869000",print_res=True)
    send_res("/manage/cluster_stats?et=1522268681000&mode=applications&st=1521663869000",print_res=True)
    send_res("/manage/cluster_stats?et=1522268681000&mode=queue&st=1521663869000",print_res=True)
    ## test data insight overview
    send_res("/reports/data/kpis?numDays=7")
    send_res('/hive/tables/', print_res=False)
    send_res('/hive/tables/default.drivers/detail')

    #daemon status
    send_res("/manage/daemons_status", print_res=False)

if __name__ == '__main__':
    main()
