import time
from functools import partial

import pendulum
import singer
from singer import metadata
from singer.bookmarks import write_bookmark, reset_stream
from ratelimit import limits, sleep_and_retry, RateLimitException
from backoff import on_exception, expo, constant

from .schemas import (
    IDS,
    get_contacts_raw_fields,
    normalize_fieldname,
    METRICS_AVAILABLE
)
from .http import MetricsRateLimitException

LOGGER = singer.get_logger()

MAX_METRIC_JOB_TIME = 1800
METRIC_JOB_POLL_SLEEP = 1

def count(tap_stream_id, records):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))

def write_records(tap_stream_id, records):
    singer.write_records(tap_stream_id, records)
    count(tap_stream_id, records)

def get_date_and_integer_fields(stream):
    date_fields = []
    integer_fields = []
    for prop, json_schema in stream.schema.properties.items():
        _type = json_schema.type
        if isinstance(_type, list) and 'integer' in _type or \
           _type == 'integer':
           integer_fields.append(prop)
        elif json_schema.format == 'date-time':
            date_fields.append(prop)
    return date_fields, integer_fields

def base_transform(date_fields, integer_fields, obj):
    new_obj = {}
    for field, value in obj.items():
        if value == '':
            value = None
        elif field in integer_fields and value is not None:
            value = int(value)
        elif field in date_fields and value is not None:
            value = pendulum.parse(value).isoformat()
        new_obj[field] = value
    return new_obj

def select_fields(mdata, obj):
    new_obj = {}
    for key, value in obj.items():
        field_metadata = mdata.get(('properties', key))
        if field_metadata and \
           (field_metadata.get('selected') is True or \
            field_metadata.get('inclusion') == 'automatic'):
            new_obj[key] = value
    return new_obj

@on_exception(constant, MetricsRateLimitException, max_tries=5, interval=60)
@on_exception(expo, RateLimitException, max_tries=5)
@sleep_and_retry
@limits(calls=1, period=61) # 60 seconds needed to be padded by 1 second to work
def post_metric(atx, metric, start_date, end_date, campaign_id):
    LOGGER.info('Metrics query - metric: {} start_date: {} end_date: {} campaign_id: {}'.format(
        metric,
        start_date,
        end_date,
        campaign_id))
    return atx.client.post(
        '/email/responses',
        {
            'type': metric,
            'start_date': start_date,
            'end_date': end_date,
            'campaign_id': campaign_id
        },
        endpoint='metrics_job')

def sync_metric(atx, campaign_id, metric, start_date, end_date):
    with singer.metrics.job_timer('daily_aggregated_metric'):
        job = post_metric(atx,
                          metric,
                          start_date.to_date_string(),
                          end_date.to_date_string(),
                          campaign_id)

        LOGGER.info('Metrics query job - {}'.format(job['id']))

        start = time.monotonic()
        while True:
            if (time.monotonic() - start) >= MAX_METRIC_JOB_TIME:
                raise Exception('Metric job timeout ({} secs)'.format(
                    MAX_METRIC_JOB_TIME))
            LOGGER.info('Polling metrics query job - {}'.format(job['id']))
            data = atx.client.get('/email/{}/responses'.format(job['id']), endpoint='metrics')
            if data != '':
                break
            else:
                time.sleep(METRIC_JOB_POLL_SLEEP)

    if len(data['contact_ids']) == 1 and data['contact_ids'][0] == '':
        return

    data_rows = []
    metric_date = start_date.isoformat()
    for contact_id in data['contact_ids']:
        data_rows.append({
            'date': metric_date,
            'metric': metric,
            'contact_id': contact_id,
            'campaign_id': campaign_id
        })

    write_records('metrics', data_rows)

def write_metrics_state(atx, campaigns_to_resume, metrics_to_resume, date_to_resume):
    write_bookmark(atx.state, 'metrics', 'campaigns_to_resume', campaigns_to_resume)
    write_bookmark(atx.state, 'metrics', 'metrics_to_resume', metrics_to_resume)
    write_bookmark(atx.state, 'metrics', 'date_to_resume', date_to_resume.to_date_string())
    atx.write_state()

def sync_metrics(atx,metric):
    test_mode = atx.config.get('test_mode')
    stream = atx.catalog.get_stream('metrics')
    bookmark = atx.state.get('bookmarks', {}).get('metrics', {})

    mdata = metadata.to_map(stream.metadata)

    start_date = pendulum.parse(atx.config.get('start_date', 'now'))
    end_date = pendulum.parse(atx.config.get('end_date', 'now'))

    start_date = bookmark.get('last_metric_date', start_date)

    campaigns_to_resume = bookmark.get('campaigns_to_resume')
    if campaigns_to_resume:
        campaign_ids = campaigns_to_resume
        last_metrics = bookmark.get('metrics_to_resume')
        last_date = bookmark.get('date_to_resume')
        if last_date:
            last_date = pendulum.parse(last_date)
    else:
        campaign_ids = (
            list(map(lambda x: x['id'],
                     filter(lambda x: x['deleted'] is None,
                            campaigns)))
        )
        metrics_to_resume = metrics_selected
        last_date = None
        last_metrics = None

    current_date = last_date or start_date

    if test_mode:
        end_date = current_date
        metrics_selected = metrics_selected[:2]

    while current_date <= end_date:
        next_date = current_date.add(days=1)
        campaigns_to_resume = campaign_ids.copy()
        for campaign_id in campaign_ids:
            campaign_metrics = last_metrics or metrics_selected
            last_metrics = None
            metrics_to_resume = campaign_metrics.copy()
            for metric in campaign_metrics:
                sync_metric(atx,
                            campaign_id,
                            metric,
                            current_date,
                            next_date)
                write_metrics_state(atx, campaigns_to_resume, metrics_to_resume, current_date)
                metrics_to_resume.remove(metric)
            campaigns_to_resume.remove(campaign_id)
        current_date = next_date

    reset_stream(atx.state, 'metrics')
    write_bookmark(atx.state, 'metrics', 'last_metric_date', end_date.to_date_string())
    atx.write_state()

def sync_selected_streams(atx):
    selected_streams = atx.selected_stream_ids
    last_synced_stream = atx.state.get('last_synced_stream')

    if IDS.TEAM_TABLE in selected_streams and last_synced_stream != IDS.TEAM_TABLE:
        sync_metrics(atx,'team_table')
        atx.state['last_synced_stream'] = IDS.TEAM_TABLE
        atx.write_state()

    # add additional analytics here

    atx.state['last_synced_stream'] = None
    atx.write_state()
