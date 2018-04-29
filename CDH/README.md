### check_cdh_config.py

sample usage: `./check_cdh_config.py --spark-version 1.6,2.2 --cm_host congo21.unraveldata.com --unravel-host congo24.unraveldata.com`

```
required arguments:

  --spark-version SPARK_VER
                        spark version e.g. 1.6 or 2.2
  --cm_host CM_HOSTNAME
                        hostname of CM Server, default is local host

optional arguments:

  --unravel-host UNRAVEL
                        Unravel Server hostname or IP address
  -user USER, --user USER
                        CM Username, default: admin
  -pass PASSWORD, --password PASSWORD
                        CM Password, default: admin
  -uuser UNRAVEL_USERNAME, --unravel_username UNRAVEL_USERNAME
                        Unravel UI Username, default: admin
  -upass UNRAVEL_PASSWORD, --unravel_password UNRAVEL_PASSWORD
                        Unravel UI Password, default: unraveldata
  -h, --help            show this help message and exit
 ```

sample output:
![img1](screenshot/20180428-201238.png)
![img2](screenshot/20180428-201352.png)
![img3](screenshot/20180428-201418.png)
![img4](screenshot/20180428-201510.png)
![img5](screenshot/20180428-201535.png)
