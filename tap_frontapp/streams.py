import time

import pendulum
import singer
from singer import metadata
from singer.bookmarks import write_bookmark, reset_stream
from ratelimit import limits, sleep_and_retry, RateLimitException
from backoff import on_exception, expo, constant

from .schemas import (
    IDS
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
def get_metric(atx, metric, start_date, end_date):
    LOGGER.info('Metrics query - metric: {} start_date: {} end_date: {} '.format(
        metric,
        start_date,
        end_date))
    return atx.client.get('/analytics', params={'start': start_date, \
            'end': end_date, 'metrics[]':metric}, endpoint='analytics')

def sync_metric(atx, metric, incremental_range, start_date, end_date):
    with singer.metrics.job_timer('daily_aggregated_metric'):
        start = time.monotonic()
        # we've really moved this functionality to the request in the http script
        #so we don't expect that this will actually have to run mult times
        while True:
            if (time.monotonic() - start) >= MAX_METRIC_JOB_TIME:
                raise Exception('Metric job timeout ({} secs)'.format(
                    MAX_METRIC_JOB_TIME))
            data = get_metric(atx, metric, start_date, end_date)
            if data != '':
                break
            else:
                time.sleep(METRIC_JOB_POLL_SLEEP)

    data_rows = []
    #metric_date = start_date.isoformat()
    # transform the team_table data
    if metric == 'team_table':
        rnum = 0
        for row in data:
            rnum += 1
            if rnum == 1:
                data_rows.append({
                    "analytics_date": start_date,
                    "analytics_range": incremental_range,
                    "teammate_v": row[0]['v'],
                    "teammate_url": "",
                    "teammate_id": 0,
                    "teammate_p": row[0]['p'],
                    "num_conversations_v": row[1]['v'],
                    "num_conversations_p": row[1]['p'],
                    "avg_message_conversations_v": row[2]['v'],
                    "avg_message_conversations_p": row[2]['p'],
                    "avg_reaction_time_v": row[3]['v'],
                    "avg_reaction_time_p": row[3]['p'],
                    "avg_first_reaction_time_v": row[4]['v'],
                    "avg_first_reaction_time_p": row[4]['p'],
                    "num_messages_v": row[5]['v'],
                    "num_messages_p": row[5]['p'],
                    "num_sent_v": row[6]['v'],
                    "num_sent_p": row[6]['p'],
                    "num_replied_v": row[7]['v'],
                    "num_replied_p": row[7]['p'],
                    "num_composed_v": row[8]['v'],
                    "num_composed_p": row[8]['p']
                    })
            else:
                data_rows.append({
                    "analytics_date": start_date,
                    "analytics_range": incremental_range,
                    "teammate_v": row[0]['v'],
                    "teammate_url": row[0]['url'],
                    "teammate_id": row[0]['id'],
                    "teammate_p": row[0]['v'],
                    "num_conversations_v": row[1]['v'],
                    "num_conversations_p": row[1]['p'],
                    "avg_message_conversations_v": row[2]['v'],
                    "avg_message_conversations_p": row[2]['p'],
                    "avg_reaction_time_v": row[3]['v'],
                    "avg_reaction_time_p": row[3]['p'],
                    "avg_first_reaction_time_v": row[4]['v'],
                    "avg_first_reaction_time_p": row[4]['p'],
                    "num_messages_v": row[5]['v'],
                    "num_messages_p": row[5]['p'],
                    "num_sent_v": row[6]['v'],
                    "num_sent_p": row[6]['p'],
                    "num_replied_v": row[7]['v'],
                    "num_replied_p": row[7]['p'],
                    "num_composed_v": row[8]['v'],
                    "num_composed_p": row[8]['p']
                    })

    write_records(metric, data_rows)

def write_metrics_state(atx, metric, date_to_resume):
    write_bookmark(atx.state, metric, 'date_to_resume', date_to_resume.to_date_string())
    atx.write_state()

def sync_metrics(atx, metric):
    test_mode = atx.config.get('test_mode')
    incremental_range = atx.config.get('incremental_range')
    stream = atx.catalog.get_stream(metric)
    bookmark = atx.state.get('bookmarks', {}).get(metric, {})

    mdata = metadata.to_map(stream.metadata)

    start_date = pendulum.parse(atx.config.get('start_date', 'now'))
    #print("start_date=",start_date)
    end_date = pendulum.parse(atx.config.get('end_date', 'now'))

    start_date = bookmark.get('last_metric_date', start_date)
    #print("start_date=",start_date)

    last_date = bookmark.get('date_to_resume', end_date)
    #print("last_date=",last_date)

    # no real reason to assign this other than the naming
    # makes better sense once we go into the loop
    current_date = last_date or start_date
    #print("current_date=",current_date)

    while current_date <= end_date:
        if incremental_range == "daily":
            next_date = current_date.add(days=1)
        elif incremental_range == "hourly":
            next_date = current_date.add(hours=1)

        ut_current_date = int(current_date.timestamp())
        ut_next_date = int(next_date.timestamp())
        sync_metric(atx, metric, incremental_range, ut_current_date, ut_next_date)
        write_metrics_state(atx, metric, current_date)
        current_date = next_date

    reset_stream(atx.state, metric)
    write_bookmark(atx.state, metric, 'last_metric_date', end_date.to_date_string())
    atx.write_state()

def sync_selected_streams(atx):
    selected_streams = atx.selected_stream_ids
    last_synced_stream = atx.state.get('last_synced_stream')

    # last synced stream = None
# mike had to change this to get it to work though don't know why
    #if IDS.TEAM_TABLE in selected_streams and last_synced_stream != IDS.TEAM_TABLE:
    if IDS.TEAM_TABLE in selected_streams:
        sync_metrics(atx, 'team_table')
        atx.state['last_synced_stream'] = IDS.TEAM_TABLE
        atx.write_state()

    # add additional analytics here
