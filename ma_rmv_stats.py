import argparse
import requests
import time
import xml.etree.ElementTree as ET
from string import split

from apiclient import discovery
from apiclient import errors
from oauth2client.client import GoogleCredentials
import strict_rfc3339

#
# Query the MA RMV for branch wait times and post as custom metrics.
#
# Run this via cron with the follow crontab entry:
#   2,7,12,17,22,27,32,37,42,47,52,57 * * * * python /full/path/to/ma_rmv_metric_gen.py > /var/log/rmv.log
#

MA_RMV_WAIT_TIME_CUSTOM_METRIC_TYPE = "custom.googleapis.com/ma_rmv_wait_time"
MA_RMV_WAIT_TIME_API = "http://www.massdot.state.ma.us/feeds/qmaticxml/qmaticXML.aspx"

def get_client():
    """Builds an http client authenticated with the service account credentials."""
    credentials = GoogleCredentials.get_application_default()
    api_client = discovery.build('monitoring', 'v3', credentials=credentials)
    return api_client

def get_metric_descriptor(api_client, project_id):
    """Fetch the metric descriptor for the wait_time custom metric."""
    try:
        request = api_client.projects().metricDescriptors().get(
            name="projects/%s/metricDescriptors/%s" %
            (project_id, MA_RMV_WAIT_TIME_CUSTOM_METRIC_TYPE))
        response = request.execute()
        return response
    except errors.HttpError as e:
        print("Failed to get metric descriptor: %s" % e)
        return None

def create_metric_descriptor(api_client, project_id):
    """Create the metric descriptor for the wait_time custom metric."""
    md_definition = {
        "type": MA_RMV_WAIT_TIME_CUSTOM_METRIC_TYPE,
        "labels": [
            {
                "key": "branch",
                "value_type": "STRING",
                "description": "The RMV branch providing service."
            },
            {
                "key": "service",
                "value_type": "STRING",
                "description": "The type of RMV service.",
            },
            ],
        "metric_kind": "GAUGE",
        "value_type": "DOUBLE",
        "unit": "min",
        "display_name": "Massachusetts RMV Wait Time",
        "description": "The amount of time to wait for a RMV service by "
            "service type and branch.",
        }
    try:
        print "Creating metric descriptor."
        request = api_client.projects().metricDescriptors().create(
            name="projects/%s" % project_id, body=md_definition)
        response = request.execute()
        # It can take a few seconds for a new MetricDescriptor to propagate
        # through the system.
        time.sleep(5)
        return response  # response is a MetricDescriptor
    except errors.HttpError as e:
        print("Failed to create metric descriptor: %s" % e)
        return None

def write_data_point(api_client, project_id,
                     branch, service, timestamp, wait_time):
    """Write a data point to the wait_time custom metric."""
    data_point_definition = {
        "time_series": [
            {
            "metric": {
                "type": MA_RMV_WAIT_TIME_CUSTOM_METRIC_TYPE,
                "labels": {
                    "branch": branch,
                    "service": service
                    },
                },
            "resource": {
                "type": "global",
                "labels": {
                    "project_id": project_id,
                    },
                },
            "points": [{
                "interval": {
                    "end_time": timestamp,
                    },
                "value": {
                    "double_value": wait_time,
                    },
                }],
            }]
         }

    try:
        request = api_client.projects().timeSeries().create(
            name="projects/%s" % project_id, body=data_point_definition)
        _ = request.execute()
    except errors.HttpError as e:
        print("Write failed: branch=%s, service=%s, error=%s" %
              (branch, service, e))

def query_wait_time(api_client, project_id, branch=None, service=None):
    """Query the last hour of data for a branch and service."""
    query_filter = 'resource.type="global" AND metric.type="%s"' % (
        MA_RMV_WAIT_TIME_CUSTOM_METRIC_TYPE)
    if branch is not None:
        query_filter = '%s AND metric.label.branch="%s"' % (query_filter, branch)
    if service is not None:
        query_filter = '%s AND metric.label.service="%s"' % (query_filter, service)

    now = int(time.time())
    end_rfc3339 = strict_rfc3339.timestamp_to_rfc3339_utcoffset(now) 
    start_rfc3339 = strict_rfc3339.timestamp_to_rfc3339_utcoffset(now-60*60)

    request = api_client.projects().timeSeries().list(
        name="projects/%s" % project_id, filter=query_filter,
        interval_endTime=end_rfc3339, interval_startTime=start_rfc3339)
    response = request.execute()
    print "Query result: %s" % response

def get_ma_rmv_wait_times():
    """Queries MA RMV wait time API and parses results.

    Returns a dict[<town>][<service_type>] = <time> with <time> reported
    in minutes as a float.
    """
    def parse_wait_time(time_val):
        if time_val in ['Error', 'Closed']:
            return 0
        vals = split(time_val, ':')
        return int(vals[0])*60 + int(vals[1]) + float(vals[2])/60

    r = requests.get(MA_RMV_WAIT_TIME_API)

    rmv_waits = {}
    branches = ET.fromstring(r.text)
    for branch in branches:
        branch_info = {}
        for attr in branch:
            attr_value = attr.text
            if attr.tag in ["licensing", "registration", ]:
              attr_value = parse_wait_time(attr.text)
            branch_info[attr.tag] = attr_value
        rmv_waits[branch_info['town']] = branch_info
    return rmv_waits


def write_ma_rmv_wait_times(api_client, project_id,
                            ma_rmv_wait_times, timestamp):
    for branch, branch_info in ma_rmv_wait_times.iteritems():
        for service in ["licensing", "registration", ]:
            write_data_point(api_client, project_id, branch, service,
                             timestamp, branch_info[service])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project_id", help="Project ID you want to access.", required=True)
    args = parser.parse_args()

    now = int(time.time())
    now_rfc3339 = strict_rfc3339.timestamp_to_rfc3339_utcoffset(now) 
    
    print "Running: time=%s" % now_rfc3339
    ma_rmv_wait_times = get_ma_rmv_wait_times()

    api_client = get_client()
    md = get_metric_descriptor(api_client, args.project_id)
    if md is None:
        print("Creating custom metric descriptor for MA RMV wait time.")
        md = create_metric_descriptor(api_client, args.project_id)
    write_ma_rmv_wait_times(api_client, args.project_id,
                            ma_rmv_wait_times, now_rfc3339)
    query_wait_time(api_client, args.project_id,
                    branch="Boston",  service="registration")
