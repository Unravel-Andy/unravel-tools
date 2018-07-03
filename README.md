# unravel-tools
## unravel_api_test.py

Required Packages:

`requests` and `termcolor`

Usage: python unravel_api_test.py --unravel-host <Unravel HOSTNAME> [-user] [-pass]

To add new api end point open the file and append a line 

For example if the new unravel end point is: http://unraveldata.com:3000/api/v1/clusters/default/nodes

`send_res("/clusters/default/nodes")`

`send_res("/clusters/default/nodes", print_res=False)` will not print the response body

For POST request:

`send_res("/apps/search",action='post', data_in={"appStatus":["R"],
                                                    "from":0,
                                                    "appTypes":["mr","hive","spark","cascading","pig"]
                                                    })`
    