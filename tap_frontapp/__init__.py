#!/usr/bin/env python3

import os
import sys
import json

import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams
from .context import Context
from . import schemas

REQUIRED_CONFIG_KEYS = ["token", "metric"]

LOGGER = singer.get_logger()

#def check_authorization(atx):
#    atx.client.get('/settings')


## with tap-emarsys, they do it this way where the catalog is read in from a call to the api 
#def discover(atx):
#    check_authorization(atx)
#    catalog = Catalog([])
#    for tap_stream_id in schemas.STATIC_SCHEMA_STREAM_IDS:
#        schema = Schema.from_dict(schemas.load_schema(tap_stream_id))
#        metadata = []
#        # what is ROOT_METADATA?  
#        if tap_stream_id in schemas.ROOT_METADATA:
#            metadata.append(schemas.ROOT_METADATA[tap_stream_id])
#        for field_name in schema.properties.keys():
#            if field_name in schemas.PK_FIELDS[tap_stream_id]:
#                inclusion = 'automatic'
#            else:
#                inclusion = 'available'
#            metadata.append({
#                'metadata': {
#                    'inclusion': inclusion
#                },
#                'breadcrumb': ['properties', field_name]
#            })
#        catalog.streams.append(CatalogEntry(
#            stream=tap_stream_id,
#            tap_stream_id=tap_stream_id,
#            key_properties=schemas.PK_FIELDS[tap_stream_id],
#            schema=schema,
#            metadata=metadata
#        ))
## mike so here they do it again.  i do think it's an added table that theyre pulling out from streams
#    contacts_schema, contact_metadata = schemas.get_contacts_schema(atx)
#    catalog.streams.append(CatalogEntry(
#        stream='contacts',
#        tap_stream_id='contacts',
#        key_properties=schemas.PK_FIELDS['contacts'],
#        schema=contacts_schema,
#        metadata=contact_metadata
#    ))
#
#    return catalog


# with tap-xero, they just define the schema via the json files:
def load_schema(tap_stream_id):
   path = "schemas/{}.json".format(tap_stream_id)
   schema = utils.load_json(get_abs_path(path))
   dependencies = schema.pop("tap_schema_dependencies", [])
   refs = {}
   for sub_stream_id in dependencies:
    refs[sub_stream_id] = load_schema(sub_stream_id)
   if refs:
    singer.resolve_schema_references(schema, refs)
   return schema


# not sure i need this
#def ensure_credentials_are_valid(config):
#   XeroClient(config).filter("currencies")

def discover(atx):
#   ensure_credentials_are_valid(atx)
   catalog = Catalog([])
   for stream in streams_.all_streams:
    schema = Schema.from_dict(load_schema(stream.tap_stream_id),
    inclusion="automatic")
    catalog.streams.append(CatalogEntry(
    stream=stream.tap_stream_id,
    tap_stream_id=stream.tap_stream_id,
    key_properties=stream.pk_fields,
    schema=schema,
   ))
   return catalog




def sync(atx):
    for tap_stream_id in schemas.STATIC_SCHEMA_STREAM_IDS:
        schemas.load_and_write_schema(tap_stream_id)

# mike not sure why they do this but i don't thin we need it.  maybe they're pulling out an extra table that 
#    contacts_schema, _ = schemas.get_contacts_schema(atx)
#    singer.write_schema('contacts',
#                        contacts_schema.to_dict(),
#                        schemas.PK_FIELDS['contacts'])

    streams.sync_selected_streams(atx)
    atx.write_state()

@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    atx = Context(args.config, args.state)
    if args.discover:
        catalog = discover(atx)
        json.dump(catalog.to_dict(), sys.stdout)
    else:
        atx.catalog = Catalog.from_dict(args.properties) \
            if args.properties else discover(atx)
        sync(atx)

if __name__ == "__main__":
    main()
