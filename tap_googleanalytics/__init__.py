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
from datetime import date

REQUIRED_CONFIG_KEYS = ["start_date", "view_id", "developer_token", "oauth_client_id", "oauth_client_secret", "refresh_token"]
LOGGER = singer.get_logger()

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


def sync(config, state, catalog, analytics):

    selected_stream_ids = get_selected_streams(catalog)

    # Loop over streams in catalog
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id

        if stream_id in selected_stream_ids:
            metrics = get_metrics_from_schema(stream.schema, stream.metadata)
            dimensions = get_dimensions_from_schema(stream.schema, stream.metadata)
                
            if stream_id == 'ga-basic-report':         
                report = get_report(analytics, metrics, dimensions, config)
                LOGGER.info('Syncing stream:' + stream_id)
                sync_report(report, stream)

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
       
    metrics = [prop for prop in schema_dict['properties'] if metadata.get(metadata_dict, ("properties", prop), "dimension") is not True]
    
    return metrics


def sync_report(reports, stream):
    stream_metadata = stream.metadata
    stream_schema = stream.schema
    stream_name = stream.tap_stream_id

    metrics = get_metrics_from_schema(stream_schema, stream_metadata)
    dimensions = get_dimensions_from_schema(stream_schema, stream_metadata)

    for report in reports["reports"]:
        if 'rows' in report['data']: 
            for line in report['data']['rows']:
                metric_line = {}

                for i, dimension in enumerate(dimensions):
                    metric_line[dimension] = line['dimensions'][i]

                for i, metric in enumerate(metrics):
                    metric_line[metric] = line['metrics'][0]['values'][i]

                singer_line = None
                
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


def get_report(analytics, metrics, dimensions, config):
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
  
  if 'end_date' in config:
    end_date = config['end_date']
  else:
    end_date = date.today().isoformat()

  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': config['view_id'],
          'dateRanges': [{'startDate': config['start_date'], 'endDate':end_date }],
          'metrics': metrics_for_ga,
          'dimensions': dimensions_for_ga
        }]
      }
  ).execute()


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

        analytics = initialize_analytics_reporting(args.config)
        sync(args.config, args.state, catalog, analytics)



if __name__ == "__main__":
    main()