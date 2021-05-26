# ma-rmv-stats
Query Massachusetts RMV wait times and publish to Stackdriver as custom metrics. 

To run this script on a GCP instance:
* Create a new GCP instance.
  * All GCP instances, given default permissions, can write custom metrics.
  * If you would like to be able to run the example query to verify that everything
    is working end-to-end, add the "Full" scope for the "Stackdriver Monitoring API".
    The default scope does not allow querying the MonitoringApi.ListTimeSeries
    endpoint.
* `ssh` into the instance.
* Execute the following:
  ```  
  $ sudo apt-get -y install git python3-pip python3-venv
  $ git clone https://github.com/premoxios/ma-rmv-stats.git
  $ cd ma-rmv-stats
  $ python3 -m venv ./virtualenv
  $ source ./virtualenv/bin/activate
  $ python3 -m pip install wheel
  $ python3 -m pip install -r requirements.txt
  $ python3 ma-rmv-stats.py --project_id [PROJECT_ID]
  ```

To collect data on a regular schedule via cron, add a crontab entry as, e.g.,
```
$ sudo sh -c 'echo "2,7,12,17,22,27,32,37,42,47,52,57 * * * * root \
  . /[PATH]/ma-rmv-stats/virtualenv/bin/activate && \
  python /[PATH]/ma-rmv-stats/ma_rmv_stats.py --project_id [PROJECT_ID] >> \
  /var/log/rmv.log" >> /etc/crontab'
```
