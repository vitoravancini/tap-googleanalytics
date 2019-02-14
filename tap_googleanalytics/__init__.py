#!/usr/bin/env python3
import pprint
import os
import json
import singer
from singer import utils, metadata
from singer import (transform,
                    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    Transformer)
import singer
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import google.oauth2.credentials
from datetime import date, timedelta, datetime
import time

REQUIRED_CONFIG_KEYS = ["start_date", "view_id", "developer_token", "oauth_client_id", "oauth_client_secret", "refresh_token"]
LOGGER = singer.get_logger()
management = []

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas

def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        stream_key_properties = []

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : [],
            'key_properties': []
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog.streams:
        stream_metadata = metadata.to_map(stream.metadata)
        # stream metadata will have an empty breadcrumb
        if metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream.tap_stream_id)

    return selected_streams


def sync(config, state, catalog, analytics, management):

    selected_stream_ids = get_selected_streams(catalog)
    if 'end_date' in config:
        end_date = datetime.strptime(config['end_date'], "%Y-%m-%d")
    else:
        end_date = datetime.today()
    
    start_date = datetime.strptime(config['start_date'], "%Y-%m-%d")
    current_date = datetime.strptime(config['start_date'], "%Y-%m-%d")
    
    if end_date < start_date:
        LOGGER.error("end_date: {} < start_Date: {}, exiting".format(end_date, start_date))
        exit(1)

    # Loop over streams in catalog
    for stream in catalog.streams:
        stream_id = stream.stream

        if stream_id in selected_stream_ids:
            while (current_date <= end_date):
                metrics = get_metrics_from_schema(stream.schema, stream.metadata)
                dimensions = get_dimensions_from_schema(stream.schema, stream.metadata)
                
                if stream_id == 'ga-basic-report' or stream_id == 'ga-adwords-report':
               
                    report = get_report(analytics, metrics, dimensions, config, current_date)
                    LOGGER.info('Syncing stream:' + stream_id)
                    sync_report(report, stream)

                elif stream_id == 'ga-goals-report':
                    LOGGER.info('Syncing stream:' + stream_id)

                    goals = list_goals(config, stream, management)
                    LOGGER.info('Goals:' + str(goals))
                    LOGGER.info('Date:' + str(current_date))

                    reports = get_goals_reports(config, goals, stream, analytics, metrics, dimensions, current_date)
                    for report in reports:
                        #pp.pprint(report)
                        sync_report(report, stream)
                current_date += timedelta(days=1)
                LOGGER.info("Waiting 2s for next")
                time.sleep(2)
    return


def get_dimensions_from_schema(stream_schema, stream_metadata):
    #only flat metrics for now
    schema_dict = stream_schema.to_dict()
    metadata_dict = metadata.to_map(stream_metadata)
       
    metrics = [prop for prop in schema_dict['properties'] if metadata.get(metadata_dict, ("properties", prop), "dimension") is True]
    
    return metrics


def get_metrics_from_schema(stream_schema, stream_metadata):
    #only flat metrics for now
    schema_dict = stream_schema.to_dict()
    metadata_dict = metadata.to_map(stream_metadata)
    
    def is_metric(prop):
        return metadata.get(metadata_dict, ("properties", prop), "dimension") is not True and prop != "date"
    
    metrics = [prop for prop in schema_dict['properties'] if is_metric(prop)]
    
    LOGGER.info(metrics)
    return metrics


def get_goals_reports(config, goals, stream, analytics, metrics, dimensions, current_date):
    reports = []
    for goal in goals:
        report = get_goal_report(analytics, goal['id'], metrics, dimensions, config, current_date)
        reports.append(report)
    return reports


def sync_report(reports, stream):
    stream_metadata = stream.metadata
    stream_schema = stream.schema
    stream_name = stream.stream

    metrics = get_metrics_from_schema(stream_schema, stream_metadata)
    dimensions = get_dimensions_from_schema(stream_schema, stream_metadata)
    for report in reports["reports"]:
        LOGGER.info("Report")
        LOGGER.info(report)

        if 'rows' in report['data']: 
            for line in report['data']['rows']:
                metric_line = {}

                for i, dimension in enumerate(dimensions):
                    metric_line[dimension] = line['dimensions'][i]

                for i, metric in enumerate(metrics):
                    metric_line[metric] = line['metrics'][0]['values'][i]

                singer_line = None
                
                if "ga:date" in metric_line:
                    gaDate = datetime.strptime(metric_line['ga:date'], "%Y%m%d")
                    singerDate = gaDate.strftime("%Y-%m-%d")
                    metric_line['date'] = singerDate

                with Transformer(singer.UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
                    singer_lines = bumble_bee.transform(metric_line, stream_schema.to_dict())

                singer.write_record(stream_name, singer_lines)
               


def initialize_analytics_reporting(config):
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  _GOOGLE_OAUTH2_ENDPOINT = 'https://accounts.google.com/o/oauth2/token'
  
  creds = google.oauth2.credentials.Credentials(
        config['developer_token'], refresh_token=config['refresh_token'],
        client_id=config['oauth_client_id'], 
        client_secret=config['oauth_client_secret'],
        token_uri=_GOOGLE_OAUTH2_ENDPOINT)

  analytics = build('analyticsreporting', 'v4', credentials=creds)

  return analytics


def initialize_analytics_management(config):
  """Initializes an Analytics MGMT API V3 service object.

  Returns:
    An authorized Analytics MGMT API V3 service object.
  """
  _GOOGLE_OAUTH2_ENDPOINT = 'https://accounts.google.com/o/oauth2/token'
  creds = google.oauth2.credentials.Credentials(
        config['developer_token'], refresh_token=config['refresh_token'],
        client_id=config['oauth_client_id'], 
        client_secret=config['oauth_client_secret'],
        token_uri=_GOOGLE_OAUTH2_ENDPOINT)

  analytics = build('analytics', 'v3', credentials=creds)

  return analytics


def get_goal_report(analytics, goal_id, metrics, dimensions, config, current_date):
  """Queries the Analytics Reporting API V4 for the goal metrics

  Args:
    analytics: An authorized Analytics Management API V3 service object.
    goal_id: The id of the goal to return the metrics
  Returns:
    The Analytics Reporting API V4 response.
  """ 
    
  def to_ga_metric(metric_name):
    if "XX" in metric_name:
        metric_name = metric_name.replace("XX", goal_id)
    return {"expression" : metric_name }
    
  def to_ga_dimension(dimension):
        return {"name" : dimension }

  metrics_for_ga = [to_ga_metric(metric) for metric in metrics]
  dimensions_for_ga = [to_ga_dimension(dimension) for dimension in dimensions]

  return analytics.reports().batchGet(
      body={
          'reportRequests': [
          {
            'viewId': config['view_id'],
            'dateRanges': [{'startDate': current_date.date().isoformat(), 'endDate':current_date.date().isoformat()}],
            'metrics': metrics_for_ga,
            'dimensions': dimensions_for_ga
          }]
      }
  ).execute() 


def get_report(analytics, metrics, dimensions, config, current_date):
  """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """

  def to_ga_metric(metric_name):
      return {"expression" : metric_name }
  
  def to_ga_dimension(dimension):
      return {"name" : dimension }

  metrics_for_ga = [to_ga_metric(metric) for metric in metrics]
  dimensions_for_ga = [to_ga_dimension(dimension) for dimension in dimensions]
  
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': config['view_id'],
          'dateRanges': [{'startDate': current_date.date().isoformat(), 'endDate':current_date.date().isoformat()}],
          'metrics': metrics_for_ga,
          'dimensions': dimensions_for_ga,
          'includeEmptyRows': True
        }]
      }
  ).execute()


def list_goals(config, stream, management):
    result = management.management().goals().list(
        accountId=config['account_id'],
        webPropertyId=config['web_property_id'],
        profileId=config['view_id']).execute()

    return result['items']


@utils.handle_top_exception(LOGGER)
def main():
    
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(catalog)
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog =  discover()

        management = initialize_analytics_management(args.config)
        analytics = initialize_analytics_reporting(args.config)

        sync(args.config, args.state, catalog, analytics, management)


if __name__ == "__main__":
    main()
