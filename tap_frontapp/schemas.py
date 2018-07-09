import os
import re

import singer
from singer import utils
from singer.catalog import Schema

class IDS(object): # pylint: disable=too-few-public-methods
    TEAM_TABLE = 'team_table'

STATIC_SCHEMA_STREAM_IDS = [
    IDS.TEAM_TABLE
]

PK_FIELDS = {
    IDS.TEAM_TABLE: ['analytics_date', 'analytics_range', 'teammate_v']
}

def normalize_fieldname(fieldname):
    fieldname = fieldname.lower()
    fieldname = re.sub(r'[\s\-]', '_', fieldname)
    return re.sub(r'[^a-z0-9_]', '', fieldname)


# the problem with the schema we see coming from team_table is that it's a little inconsistent:
# {"t":"str","v":"All","p":"All"},{"t":"num","v":306,"p":465},{"t":"num","v":2.65,"p":2.39},{"t":"dur","v":193.5,"p":271.7},{"t":"dur","v":257.57,"p":348.04},{"t":"num","v":1484,"p":1413},{"t":"num","v":659,"p":677},{"t":"num","v":440,"p":341},{"t":"num","v":219,"p":336}]
# ,[{"t":"teammate","v":"Andrew Keprta","url":"/api/1/companies/theguild_co/team/andrew","id":253419}
# so we see that the type = teammate is different when it covers All team members
# also we see the schema where type = num or dur is actually a triplet of type, value, and previous 
# so it looks like we need to hardcode those anomalies into this file


def get_contact_json_schema(raw_field_type):
    if raw_field_type == 'date':
        return {
            'type': ['null', 'string'],
            'format': 'date-time'
        }
    if raw_field_type == 'numeric':
        return {
            'type': ['null', 'number']
        }
    if raw_field_type == 'multichoice':
        return {
            'type': ['null', 'array'],
            'items': {
                'type': 'integer'
            }
        }
    return {
        'type': ['null', 'string']
    }

def get_contacts_raw_fields(atx):
    return atx.client.get('/field', endpoint='contact_fields')

def get_contacts_schema(atx):
    raw_fields = get_contacts_raw_fields(atx)
    properties = {}
    metadata = []
    for raw_field in raw_fields:
        json_schema = get_contact_json_schema(raw_field['application_type'])
        field_name = normalize_fieldname(raw_field['name'])
        properties[field_name] = json_schema
        metadata.append({
            'metadata': {
                'inclusion': 'available'
            },
            'breadcrumb': ['properties', field_name]
        })

    for field_name in ['id', 'uid']:
        properties[field_name] = {'type': ['string']}
        metadata.append({
            'metadata': {
                'inclusion': 'automatic'
            },
            'breadcrumb': ['properties', field_name]
        })

    schema = {
        'type': ['object'],
        'additionalProperties': False,
        'properties': properties
    }

    return Schema.from_dict(schema), metadata




def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(tap_stream_id):
    path = 'schemas/{}.json'.format(tap_stream_id)
    return utils.load_json(get_abs_path(path))

def load_and_write_schema(tap_stream_id):
    schema = load_schema(tap_stream_id)
    singer.write_schema(tap_stream_id, schema, PK_FIELDS[tap_stream_id])
