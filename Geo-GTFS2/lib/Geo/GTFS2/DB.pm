package Geo::GTFS2::DB;
use warnings;
use strict;

use DBI;
use File::Basename qw(dirname basename);
use File::Path qw(make_path);
use HTTP::Date qw(str2time);
use POSIX qw(strftime floor);
use Data::Dumper;

use fields qw(dir
	      sqlite_filename
              verbose
              no_auto_update
	      dbh);

use vars qw($TABLES $INDEXES);

BEGIN {
    $TABLES = [
        {
            "name" => "geo_gtfs",
            "columns" => [
                { "name" => "name",  "type" => "varchar", "size" => 32, "nullable" => 0, "primary_key" => 1 },
                { "name" => "value", "type" => "text",                  "nullable" => 1 },
            ],
            # delete from geo_gtfs where name = 'geo_gtfs.db.version';
            # insert into geo_gtfs (name, value) values('geo_gtfs.db.version', '0.1');
        },
        {
            "name" => "geo_gtfs_agency",
            "columns" => [
                { "name" => "id",   "type" => "integer", "nullable" => 0, "primary_key" => 1, "auto_increment" => 1 },

                # preferably the transit agency's domain name, without a
                # "www." prefix.  examples: 'ridetarc.org', 'ttc.ca'
                { "name" => "name", "type" => "varchar", "size" => 64, "nullable" => 0 },
            ],
            "indexes" => [
                { "name" => "geo_gtfs_agency_01", "columns" => [ "name" ] },
            ]
        },
        {
            "name" => "geo_gtfs_feed",
            "columns" => [
                { "name" => "id",                 "type" => "integer", "nullable" => 0, "primary_key" => 1, "auto_increment" => 1 },
                { "name" => "geo_gtfs_agency_id", "type" => "integer", "nullable" => 0, "references" => { "table" => "geo_gtfs_agency", "column" => "id" } },
                { "name" => "url",                "type" => "text",    "nullable" => 0 },

                # updated when feeds added, removed, I guess.
                { "name" => "is_active",          "type" => "integer", "nullable" => 0, "default" => 1 },
            ],
            "indexes" => [
                { "name" => "geo_gtfs_feed_01", "columns" => [ "is_active" ] },
            ]
        },
        {
            "name" => "geo_gtfs_feed_instance",
            "columns" => [
                { "name" => "id",               "type" => "integer", "nullable" => 0, "primary_key" => 1, "auto_increment" => 1 },
                { "name" => "geo_gtfs_feed_id", "type" => "integer", "nullable" => 0, "references" => { "table" => "geo_gtfs_feed", "column" => "id" } },
                { "name" => "filename",         "type" => "text",    "nullable" => 0 },
                { "name" => "retrieved",        "type" => "integer", "nullable" => 0 },

                # SHOULD be specified, but some servers omit.
                { "name" => "last_modified",    "type" => "integer", "nullable" => 1 },

                { "name" => "is_latest",        "type" => "integer", "nullable" => 0, "default" => 1 },
            ],
            "indexes" => [
                { "name" => "geo_gtfs_feed_instance_01", "columns" => [ "is_latest" ] },
            ]
        },
        {
            "name" => "geo_gtfs_realtime_feed",
            "columns" => [
                { "name" => "id",                 "type" => "integer",               "nullable" => 0, "primary_key" => 1 },
                { "name" => "geo_gtfs_agency_id", "type" => "integer",               "nullable" => 0, "references" => { "table" => "geo_gtfs_agency", "column" => "id" } },
                { "name" => "url",                "type" => "text",                  "nullable" => 0 },

                # 'updates', 'positions', 'alerts', 'all'
                { "name" => "feed_type",          "type" => "varchar", "size" => 16, "nullable" => 0 },

                # updated when feeds added, removed
                { "name" => "is_active",          "type" => "integer",               "nullable" => 0, "default" => 1 },
            ],
            "indexes" => [
                { "name" => "geo_gtfs_realtime_feed_01", "columns" => [ "feed_type" ] },
                { "name" => "geo_gtfs_realtime_feed_02", "columns" => [ "is_active" ] },
            ]
        },
        {
            "name" => "geo_gtfs_realtime_feed_instance",
            "columns" => [
                { "name" => "id",                        "type" => "integer", "nullable" => 0, "primary_key" => 1 },
                { "name" => "geo_gtfs_realtime_feed_id", "type" => "integer", "nullable" => 0, "references" => { "table" => "geo_gtfs_realtime_feed", "column" => "id" } },
                { "name" => "filename",                  "type" => "text",    "nullable" => 0 },
                { "name" => "retrieved",                 "type" => "integer", "nullable" => 0 },
                { "name" => "last_modified",             "type" => "integer", "nullable" => 1 },
                { "name" => "header_timestamp",          "type" => "integer", "nullable" => 1 },
                { "name" => "is_latest",                 "type" => "integer", "nullable" => 0, "default" => 1 },
            ],
            "indexes" => [
                { "name" => "geo_gtfs_realtime_feed_instance_01", "columns" => [ "is_latest" ] },
            ]
        },
        {
            "name" => "gtfs_agency",
            "gtfs_required" => 1,
            "columns" => [
                { "name" => "geo_gtfs_feed_instance_id", "type" => "integer",              "nullable" => 0, "references" => { "table" => "geo_gtfs_feed", "column" => "id" } },

                # for feeds containing only one agency, this can be NULL.
                { "name" => "agency_id",                 "type" => "text",                 "nullable" => 1 },
                { "name" => "agency_name",               "type" => "text",                 "nullable" => 0 },
                { "name" => "agency_url",                "type" => "text",                 "nullable" => 0 },
                { "name" => "agency_timezone",           "type" => "text",                 "nullable" => 0 },
                { "name" => "agency_lang",               "type" => "varchar", "size" => 2, "nullable" => 1 },
                { "name" => "agency_phone",              "type" => "text",                 "nullable" => 1 },
                { "name" => "agency_fare_url",           "type" => "text",                 "nullable" => 1 },
                { "name" => "agency_email",              "type" => "text",                 "nullable" => 1 }, # new in 2018
            ],
            "indexes" => [
                { "name" => "gtfs_agency_01", "columns" => [ "geo_gtfs_feed_instance_id", "agency_id" ], "unique" => 1 },
            ]
        },
        {
            "name" => "gtfs_stops",
            "gtfs_required" => 1,
            "columns" => [
                { "name" => "geo_gtfs_feed_instance_id", "type" => "integer", "nullable" => 0, "references" => { "table" => "geo_gtfs_feed", "column" => "id" } },
                { "name" => "stop_id",                   "type" => "text",    "nullable" => 0 },
                { "name" => "stop_code",                 "type" => "text",    "nullable" => 1 },
                { "name" => "stop_name",                 "type" => "text",    "nullable" => 0 },
                { "name" => "stop_desc",                 "type" => "text",    "nullable" => 1 },
                { "name" => "stop_lat",                  "type" => "numeric", "nullable" => 0 },
                { "name" => "stop_lon",                  "type" => "numeric", "nullable" => 0 },
                { "name" => "zone_id",                   "type" => "text",    "nullable" => 1 },
                { "name" => "stop_url",                  "type" => "text",    "nullable" => 1 },
                { "name" => "location_type",             "type" => "integer", "nullable" => 1 },
                { "name" => "parent_station",            "type" => "text",    "nullable" => 1 },
                { "name" => "stop_timezone",             "type" => "text",    "nullable" => 1 },
                { "name" => "wheelchair_boarding",       "type" => "integer", "nullable" => 1 },
            ],
            "indexes" => [
                { "name" => "gtfs_stops_01", "columns" => [ "geo_gtfs_feed_instance_id", "stop_id" ], "unique" => 1 },
                { "name" => "gtfs_stops_02", "columns" => [ "geo_gtfs_feed_instance_id", "zone_id" ] },
                { "name" => "gtfs_stops_03", "columns" => [ "geo_gtfs_feed_instance_id", "location_type" ] },
                { "name" => "gtfs_stops_04", "columns" => [ "geo_gtfs_feed_instance_id", "parent_station" ] },
                { "name" => "gtfs_stops_05", "columns" => [ "geo_gtfs_feed_instance_id", "wheelchair_boarding" ] },
            ]
        },
        {
            "name" => "gtfs_routes",
            "gtfs_required" => 1,
            "columns" => [
                { "name" => "geo_gtfs_feed_instance_id", "type" => "integer",              "nullable" => 0, "references" => { "table" => "geo_gtfs_feed", "column" => "id" } },
                { "name" => "route_id",                  "type" => "text",                 "nullable" => 0 },
                { "name" => "agency_id",                 "type" => "text",                 "nullable" => 1, "references" => { "table" => "gtfs_agency", "column" => "id" } },
                { "name" => "route_short_name",          "type" => "text",                 "nullable" => 0 },
                { "name" => "route_long_name",           "type" => "text",                 "nullable" => 0 },
                { "name" => "route_desc",                "type" => "text",                 "nullable" => 1 },
                { "name" => "route_type",                "type" => "integer",              "nullable" => 0 },
                { "name" => "route_url",                 "type" => "text",                 "nullable" => 1 },
                { "name" => "route_color",               "type" => "varchar", "size" => 6, "nullable" => 1 },
                { "name" => "route_text_color",          "type" => "varchar", "size" => 6, "nullable" => 1 },
                { "name" => "route_sort_order",          "type" => "integer",              "nullable" => 1 }, # new for 2018
            ],
            "indexes" => [
                { "name" => "gtfs_routes_01", "columns" => [ "geo_gtfs_feed_instance_id", "route_id", "agency_id" ], "unique" => 1 },
                { "name" => "gtfs_routes_02", "columns" => [ "geo_gtfs_feed_instance_id", "agency_id" ] },
                { "name" => "gtfs_routes_03", "columns" => [ "geo_gtfs_feed_instance_id", "route_id" ] },
                { "name" => "gtfs_routes_04", "columns" => [ "geo_gtfs_feed_instance_id", "route_type" ] },
                { "name" => "gtfs_routes_05", "columns" => [ "geo_gtfs_feed_instance_id", "route_sort_order" ] }, # new for 2018
            ]
        },
        {
            "name" => "gtfs_trips",
            "gtfs_required" => 1,
            "columns" => [
                { "name" => "geo_gtfs_feed_instance_id", "type" => "integer", "nullable" => 0, "references" => { "table" => "geo_gtfs_feed", "column" => "id" } },
                { "name" => "route_id",                  "type" => "text",    "nullable" => 0, "references" => { "table" => "gtfs_routes", "column" => "id" } },
                { "name" => "service_id",                "type" => "text",    "nullable" => 0 },
                { "name" => "trip_id",                   "type" => "text",    "nullable" => 0 },
                { "name" => "trip_headsign",             "type" => "text",    "nullable" => 1 },
                { "name" => "trip_short_name",           "type" => "text",    "nullable" => 1 },
                { "name" => "direction_id",              "type" => "integer", "nullable" => 1 },
                { "name" => "block_id",                  "type" => "text",    "nullable" => 1 },
                { "name" => "shape_id",                  "type" => "text",    "nullable" => 1, "references" => { "table" => "gtfs_shapes", "column" => "id" } },
                { "name" => "wheelchair_accessible",     "type" => "integer", "nullable" => 1 },
                { "name" => "bikes_allowed",             "type" => "integer", "nullable" => 1 },
            ],
            "indexes" => [
                { "name" => "gtfs_trips_01", "columns" => [ "geo_gtfs_feed_instance_id", "trip_id" ], "unique" => 1 },
                { "name" => "gtfs_trips_02", "columns" => [ "geo_gtfs_feed_instance_id", "route_id" ] },
                { "name" => "gtfs_trips_03", "columns" => [ "geo_gtfs_feed_instance_id", "service_id" ] },
                { "name" => "gtfs_trips_04", "columns" => [ "geo_gtfs_feed_instance_id", "direction_id" ] },
                { "name" => "gtfs_trips_05", "columns" => [ "geo_gtfs_feed_instance_id", "block_id" ] },
                { "name" => "gtfs_trips_06", "columns" => [ "geo_gtfs_feed_instance_id", "shape_id" ] },
            ]
        },
        {
            "name" => "gtfs_stop_times",
            "gtfs_required" => 1,
            "columns" => [
                { "name" => "geo_gtfs_feed_instance_id", "type" => "integer",              "nullable" => 0, "references" => { "table" => "geo_gtfs_feed", "column" => "id" } },
                { "name" => "trip_id",                   "type" => "text",                 "nullable" => 0, "references" => { "table" => "gtfs_trips", "column" => "id" } },
                { "name" => "arrival_time",              "type" => "varchar", "size" => 8, "nullable" => 0 },
                { "name" => "departure_time",            "type" => "varchar", "size" => 8, "nullable" => 0 },
                { "name" => "stop_id",                   "type" => "text",                 "nullable" => 0, "references" => { "table" => "gtfs_stops", "column" => "id" } },
                { "name" => "stop_sequence",             "type" => "integer",              "nullable" => 0 },
                { "name" => "stop_headsign",             "type" => "text",                 "nullable" => 1 },
                { "name" => "pickup_type",               "type" => "integer",              "nullable" => 1 },
                { "name" => "drop_off_type",             "type" => "integer",              "nullable" => 1 },
                { "name" => "shape_dist_traveled",       "type" => "numeric",              "nullable" => 1 },
                { "name" => "timepoint",                 "type" => "integer",              "nullable" => 1 }, # new for 2018
            ],
            "indexes" => [
                { "name" => "gtfs_stop_times_01", "columns" => [ "geo_gtfs_feed_instance_id", "stop_id" ] },
                { "name" => "gtfs_stop_times_02", "columns" => [ "geo_gtfs_feed_instance_id", "trip_id" ] },
                { "name" => "gtfs_stop_times_03", "columns" => [ "geo_gtfs_feed_instance_id", "stop_sequence" ] },
                { "name" => "gtfs_stop_times_04", "columns" => [ "geo_gtfs_feed_instance_id", "trip_id", "stop_id" ], "unique" => 1 },
                { "name" => "gtfs_stop_times_05", "columns" => [ "geo_gtfs_feed_instance_id", "timepoint" ] }, # new for 2018
            ]
        },
        {
            "name" => "gtfs_calendar",
            "gtfs_required" => 1,
            "columns" => [
                { "name" => "geo_gtfs_feed_instance_id", "type" => "integer",              "nullable" => 0, "references" => { "table" => "geo_gtfs_feed", "column" => "id" } },
                { "name" => "service_id",                "type" => "text",                 "nullable" => 0 },
                { "name" => "monday",                    "type" => "integer",              "nullable" => 0 },
                { "name" => "tuesday",                   "type" => "integer",              "nullable" => 0 },
                { "name" => "wednesday",                 "type" => "integer",              "nullable" => 0 },
                { "name" => "thursday",                  "type" => "integer",              "nullable" => 0 },
                { "name" => "friday",                    "type" => "integer",              "nullable" => 0 },
                { "name" => "saturday",                  "type" => "integer",              "nullable" => 0 },
                { "name" => "sunday",                    "type" => "integer",              "nullable" => 0 },
                { "name" => "start_date",                "type" => "varchar", "size" => 8, "nullable" => 0 },
                { "name" => "end_date",                  "type" => "varchar", "size" => 8, "nullable" => 0 },
            ],
            "indexes" => [
                { "name" => "gtfs_calendar_01", "columns" => [ "geo_gtfs_feed_instance_id", "service_id" ] },
                { "name" => "gtfs_calendar_02", "columns" => [ "geo_gtfs_feed_instance_id", "monday" ] },
                { "name" => "gtfs_calendar_03", "columns" => [ "geo_gtfs_feed_instance_id", "tuesday" ] },
                { "name" => "gtfs_calendar_04", "columns" => [ "geo_gtfs_feed_instance_id", "wednesday" ] },
                { "name" => "gtfs_calendar_05", "columns" => [ "geo_gtfs_feed_instance_id", "thursday" ] },
                { "name" => "gtfs_calendar_06", "columns" => [ "geo_gtfs_feed_instance_id", "friday" ] },
                { "name" => "gtfs_calendar_07", "columns" => [ "geo_gtfs_feed_instance_id", "saturday" ] },
                { "name" => "gtfs_calendar_08", "columns" => [ "geo_gtfs_feed_instance_id", "sunday" ] },
                { "name" => "gtfs_calendar_09", "columns" => [ "geo_gtfs_feed_instance_id", "start_date" ] },
                { "name" => "gtfs_calendar_10", "columns" => [ "geo_gtfs_feed_instance_id", "end_date" ] },
            ]
        },
        {
            "name" => "gtfs_calendar_dates",
            "columns" => [
                { "name" => "geo_gtfs_feed_instance_id", "type" => "integer",              "nullable" => 0, "references" => { "table" => "geo_gtfs_feed", "column" => "id" } },
                { "name" => "service_id",                "type" => "text",                 "nullable" => 0 },
                { "name" => "date",                      "type" => "varchar", "size" => 8, "nullable" => 0 },
                { "name" => "exception_type",            "type" => "integer",              "nullable" => 0 },
            ],
            "indexes" => [
                { "name" => "gtfs_calendar_dates_01", "columns" => [ "geo_gtfs_feed_instance_id", "service_id" ] },
                { "name" => "gtfs_calendar_dates_02", "columns" => [ "geo_gtfs_feed_instance_id", "date" ] },
                { "name" => "gtfs_calendar_dates_03", "columns" => [ "geo_gtfs_feed_instance_id", "exception_type" ] },
            ]
        },
        {
            "name" => "gtfs_fare_attributes",
            "columns" => [
                { "name" => "geo_gtfs_feed_instance_id", "type" => "integer", "nullable" => 0, "references" => { "table" => "geo_gtfs_feed", "column" => "id" } },
                { "name" => "fare_id",                   "type" => "text",    "nullable" => 0 },
                { "name" => "price",                     "type" => "numeric", "nullable" => 0 },
                { "name" => "currency_type",             "type" => "text",    "nullable" => 0 },
                { "name" => "payment_method",            "type" => "integer", "nullable" => 0 },
                { "name" => "transfers",                 "type" => "integer", "nullable" => 0 },
                { "name" => "agency_id",                 "type" => "text",    "nullable" => 1 },
                { "name" => "transfer_duration",         "type" => "integer", "nullable" => 1 },
            ],
            "indexes" => [
                { "name" => "gtfs_fare_attributes_01", "columns" => [ "geo_gtfs_feed_instance_id", "fare_id" ] },
            ]
        },
        {
            "name" => "gtfs_fare_rules",
            "columns" => [
                { "name" => "geo_gtfs_feed_instance_id", "type" => "integer", "nullable" => 0, "references" => { "table" => "geo_gtfs_feed", "column" => "id" } },
                { "name" => "fare_id",                   "type" => "text",    "nullable" => 0, "references" => { "table" => "gtfs_fare_attributes", "column" => "fare_id" } },
                { "name" => "route_id",                  "type" => "text",    "nullable" => 1, "references" => { "table" => "gtfs_routes", "column" => "id" } },
                { "name" => "origin_id",                 "type" => "text",    "nullable" => 1 },
                { "name" => "destination_id",            "type" => "text",    "nullable" => 1 },
                { "name" => "contains_id",               "type" => "text",    "nullable" => 1 },
            ],
            "indexes" => [
                { "name" => "gtfs_fare_rules_01", "columns" => [ "geo_gtfs_feed_instance_id", "fare_id" ] },
                { "name" => "gtfs_fare_rules_02", "columns" => [ "geo_gtfs_feed_instance_id", "route_id" ] },
                { "name" => "gtfs_fare_rules_03", "columns" => [ "geo_gtfs_feed_instance_id", "origin_id" ] },
                { "name" => "gtfs_fare_rules_04", "columns" => [ "geo_gtfs_feed_instance_id", "destination_id" ] },
                { "name" => "gtfs_fare_rules_05", "columns" => [ "geo_gtfs_feed_instance_id", "contains_id" ] },
            ]
        },
        {
            "name" => "gtfs_shapes",
            "columns" => [
                { "name" => "geo_gtfs_feed_instance_id", "type" => "integer", "nullable" => 0, "references" => { "table" => "geo_gtfs_feed", "column" => "id" } },
                { "name" => "shape_id",                  "type" => "text",    "nullable" => 0 },
                { "name" => "shape_pt_lat",              "type" => "numeric", "nullable" => 0 },
                { "name" => "shape_pt_lon",              "type" => "numeric", "nullable" => 0 },
                { "name" => "shape_pt_sequence",         "type" => "integer", "nullable" => 0 },
                { "name" => "shape_dist_traveled",       "type" => "numeric", "nullable" => 1 },
            ],
            "indexes" => [
                { "name" => "gtfs_shapes_01", "columns" => [ "geo_gtfs_feed_instance_id", "shape_id" ] },
                { "name" => "gtfs_shapes_02", "columns" => [ "geo_gtfs_feed_instance_id", "shape_id", "shape_pt_sequence" ] },
            ]
        },
        {
            "name" => "gtfs_frequencies",
            "columns" => [
                { "name" => "geo_gtfs_feed_instance_id", "type" => "integer",              "nullable" => 0, "references" => { "table" => "geo_gtfs_feed", "column" => "id" } },
                { "name" => "trip_id",                   "type" => "text",                 "nullable" => 1, "references" => { "table" => "gtfs_trips", "column" => "id" } },
                { "name" => "start_time",                "type" => "varchar", "size" => 8, "nullable" => 1 },
                { "name" => "end_time",                  "type" => "varchar", "size" => 8, "nullable" => 1 },
                { "name" => "headway_secs",              "type" => "integer",              "nullable" => 1 },
                { "name" => "exact_times",               "type" => "integer",              "nullable" => 1 },
            ],
            "indexes" => [
                { "name" => "gtfs_frequencies_01", "columns" => [ "geo_gtfs_feed_instance_id", "trip_id" ] },
                { "name" => "gtfs_frequencies_02", "columns" => [ "geo_gtfs_feed_instance_id", "start_time" ] },
                { "name" => "gtfs_frequencies_03", "columns" => [ "geo_gtfs_feed_instance_id", "end_time" ] },
            ]
        },
        {
            "name" => "gtfs_transfers",
            "columns" => [
                { "name" => "geo_gtfs_feed_instance_id",       "type" => "integer",         "nullable" => 0,        "references" => { "table" => "geo_gtfs_feed", "column" => "id" } },
                { "name" => "from_stop_id",                    "type" => "text",            "nullable" => 0,        "references" => { "table" => "gtfs_stops", "column" => "id" } },
                { "name" => "to_stop_id",                      "type" => "text",            "nullable" => 0,        "references" => { "table" => "gtfs_stops", "column" => "id" } },
                { "name" => "transfer_type",                   "type" => "integer",         "nullable" => 0 },
                { "name" => "min_transfer_time",               "type" => "integer",         "nullable" => 1 },
            ],
            "indexes" => [
                { "name" => "gtfs_transfers_01", "columns" => [ "geo_gtfs_feed_instance_id", "from_stop_id" ] },
                { "name" => "gtfs_transfers_02", "columns" => [ "geo_gtfs_feed_instance_id", "to_stop_id" ] },
            ]
        },
        {
            "name" => "gtfs_feed_info",
            "columns" => [
                { "name" => "geo_gtfs_feed_instance_id", "type" => "integer",              "nullable" => 0, "references" => { "table" => "geo_gtfs_feed", "column" => "id" } },
                { "name" => "feed_publisher_name",       "type" => "text",                 "nullable" => 0 },
                { "name" => "feed_publisher_url",        "type" => "text",                 "nullable" => 0 },
                { "name" => "feed_lang",                 "type" => "text",                 "nullable" => 0 },
                { "name" => "feed_start_date",           "type" => "varchar", "size" => 8, "nullable" => 1 },
                { "name" => "feed_end_date",             "type" => "varchar", "size" => 8, "nullable" => 1 },
                { "name" => "feed_version",              "type" => "text",                 "nullable" => 1 },
            ]
        },
    ];
    $INDEXES = [
        { "name" => "geo_gtfs_agency_00",          "table" => "gtfs_agency",          "columns" => [ "geo_gtfs_feed_instance_id" ] },
        { "name" => "geo_gtfs_stops_00",           "table" => "gtfs_stops",           "columns" => [ "geo_gtfs_feed_instance_id" ] },
        { "name" => "geo_gtfs_routes_00",          "table" => "gtfs_routes",          "columns" => [ "geo_gtfs_feed_instance_id" ] },
        { "name" => "geo_gtfs_trips_00",           "table" => "gtfs_trips",           "columns" => [ "geo_gtfs_feed_instance_id" ] },
        { "name" => "geo_gtfs_stop_times_00",      "table" => "gtfs_stop_times",      "columns" => [ "geo_gtfs_feed_instance_id" ] },
        { "name" => "geo_gtfs_calendar_00",        "table" => "gtfs_calendar",        "columns" => [ "geo_gtfs_feed_instance_id" ] },
        { "name" => "geo_gtfs_calendar_dates_00",  "table" => "gtfs_calendar_dates",  "columns" => [ "geo_gtfs_feed_instance_id" ] },
        { "name" => "geo_gtfs_fare_attributes_00", "table" => "gtfs_fare_attributes", "columns" => [ "geo_gtfs_feed_instance_id" ] },
        { "name" => "geo_gtfs_fare_rules_00",      "table" => "gtfs_fare_rules",      "columns" => [ "geo_gtfs_feed_instance_id" ] },
        { "name" => "geo_gtfs_shapes_00",          "table" => "gtfs_shapes",          "columns" => [ "geo_gtfs_feed_instance_id" ] },
        { "name" => "geo_gtfs_frequencies_00",     "table" => "gtfs_frequencies",     "columns" => [ "geo_gtfs_feed_instance_id" ] },
        { "name" => "geo_gtfs_transfers_00",       "table" => "gtfs_transfers",       "columns" => [ "geo_gtfs_feed_instance_id" ] },
        { "name" => "geo_gtfs_feed_info_00",       "table" => "gtfs_feed_info",       "columns" => [ "geo_gtfs_feed_instance_id" ] },
    ];
}

sub new {
    my ($class, %args) = @_;
    my $self = fields::new($class);
    $self->init(%args);
    return $self;
}

sub init {
    my ($self, %args) = @_;
    my @pwent = getpwuid($>);
    while (my ($k, $v) = each(%args)) {
	$self->{$k} = $v;
    }
    my $dir;
    my $username = $pwent[0];
    if ($username eq "_www") { # special os x user
	$dir = $self->{dir} //= "/Users/_www/.geo-gtfs2";
    } else {
	my $HOME = $ENV{HOME} // $pwent[7];
	$dir = $self->{dir} //= "$HOME/.geo-gtfs2";
    }
    my $dbfile = $self->{sqlite_filename} //= "$dir/google_transit.sqlite";
}

our $CONNECTIONS;
BEGIN {
    $CONNECTIONS = {};
}

sub dbh {
    my ($self) = @_;
    return $CONNECTIONS->{$$}->{dbh} if eval { $CONNECTIONS->{$$}->{dbh}; };

    my $dbfile = $self->{sqlite_filename};
    make_path(dirname($dbfile));

    # delete database connections in case they were created before a
    # fork.
    my @pids = keys %$CONNECTIONS;
    my @other_pids = grep { $_ ne $$ } @pids;
    foreach my $pid (@other_pids) {
        delete $CONNECTIONS->{$pid};
    }

    $CONNECTIONS->{$$} = {};
    warn(sprintf("Connecting to %s ...\n", $dbfile)) if $self->{verbose} || -t 2;
    my $dbh = DBI->connect("dbi:SQLite:$dbfile", "", "",
                           { RaiseError => 1, AutoCommit => 0 });
    warn(sprintf("... connected!\n")) if $self->{verbose} || -t 2;
    $dbh->sqlite_busy_timeout(5000);
    $CONNECTIONS->{$$}->{dbh} = $dbh;
    if (!$self->{no_auto_update}) {
        if (!$CONNECTIONS->{$$}->{tables_created}) {
            $self->create_tables();
            $CONNECTIONS->{$$}->{tables_created} = 1;
        }
    }
    return $dbh;
}

sub close_dbh {
    my ($self) = @_;
    eval { $CONNECTIONS->{$$}->{dbh}->rollback(); };
    eval { delete $CONNECTIONS->{$$}->{dbh}; };
    eval { delete $CONNECTIONS->{$$}; };
}

sub select_or_insert_id {
    my ($self, %args) = @_;
    my $table_name = $args{table_name};
    my $id_name = $args{id_name};

    my %key_fields  = eval { %{$args{key_fields}} };
    my @key_names   = keys(%key_fields);
    my @key_values  = map { $key_fields{$_} } @key_names;
    my $key_where   = join(" and ", map { "($_ = ?)" } @key_names);

    my $sth;
    my $sql;

    $sql = "select $id_name from $table_name where $key_where";
    $sth = $self->dbh->prepare($sql);
    $sth->execute(@key_values);
    my ($id) = $sth->fetchrow_array();
    $sth->finish();
    if (defined $id) {
	$self->dbh->rollback();
	return $id;
    }

    if ($args{before_insert}) {
	my $sql = $args{before_insert}{sql};
	my @bind_values = eval { @{$args{before_insert}{bind_values}} };
	$sth = $self->dbh->prepare($sql);
	$sth->execute(@bind_values);
	$sth->finish();
    }

    my %more_fields  = eval { %{$args{more_fields}} };
    my %insert_fields = (%key_fields, %more_fields);
    my @insert_names  = keys(%insert_fields);
    my @insert_values = map { $insert_fields{$_} } @insert_names;

    my $insert_field_names  = join(", ", @insert_names);
    my $insert_placeholders = join(", ", ("?") x scalar(@insert_names));

    $sql = "insert into $table_name($insert_field_names) values($insert_placeholders)";
    $sth = $self->dbh->prepare($sql);
    $sth->execute(@insert_values);
    $sth->finish();

    $id = $self->dbh->last_insert_id("", "", "", "");

    $self->dbh->commit();

    if (defined $id) {
	$self->dbh->rollback();
	return $id;
    }
}

sub drop_tables {
    my ($self) = @_;
    my $dbh = $self->dbh;
    foreach my $sql ($self->sql_to_drop_tables) {
        warn($sql) if $self->{verbose} || -t 2;
        $self->dbh->do($sql);
    }
    warn("Committing...\n") if $self->{verbose} || -t 2;
    $self->dbh->commit();
    warn("...done!\n") if $self->{verbose} || -t 2;
}

sub create_tables {
    my ($self) = @_;
    my $dbh = $self->dbh;
    foreach my $sql ($self->sql_to_create_tables) {
        warn($sql) if $self->{verbose} || -t 2;
        $self->dbh->do($sql);
    }
    warn("Committing...\n") if $self->{verbose} || -t 2;
    $self->dbh->commit();
    warn("...done!\n") if $self->{verbose} || -t 2;
}

sub update_tables {
    my ($self) = @_;
    my $dbh = $self->dbh;
    foreach my $sql ($self->sql_to_update_tables) {
        warn($sql) if $self->{verbose} || -t 2;
        $self->dbh->do($sql);
    }
    warn("Committing...\n") if $self->{verbose} || -t 2;
    $self->dbh->commit();
    warn("...done!\n") if $self->{verbose} || -t 2;
}

use Carp qw();

sub execute_multiple_sql_queries {
    my ($self, $sql) = @_;
    # This function makes a couple of gratuitous assumtions:
    # 1. that "--" does not occur in a string
    # 2. that ; at the end of a line is a statement separator
    $sql =~ s{--.*?$}{}gsm;            # Assumption 1.
    my @sql = split(qr{;\s*$}m, $sql); # Assumption 2.
    foreach my $sql (@sql) {
	next unless $sql =~ m{\S}; # ignore blank strings.
	local $SIG{__DIE__} = sub { Carp::confess(@_); };
        $self->dbh->do($sql);
    }
    $self->dbh->commit();
}

###############################################################################
# GTFS-REALTIME
###############################################################################

sub get_geo_gtfs_realtime_feeds {
    my ($self, $geo_gtfs_agency_id) = @_;
    my $sth = $self->dbh->prepare("select * from geo_gtfs_realtime_feed where geo_gtfs_agency_id = ?");
    $sth->execute($geo_gtfs_agency_id);
    my @rows;
    while (my $row = $sth->fetchrow_hashref()) {
	push(@rows, $row);
    }
    return @rows;
}

sub get_geo_gtfs_realtime_feed_by_type {
    my ($self, $geo_gtfs_agency_id, $feed_type) = @_;
    my $sth = $self->dbh->prepare("select * from geo_gtfs_realtime_feed where geo_gtfs_agency_id = ? and feed_type = ?");
    $sth->execute($geo_gtfs_agency_id, $feed_type);
    my $row = $sth->fetchrow_hashref();
    if (!$row) {
	die("No $feed_type feed for agency id $geo_gtfs_agency_id.\n");
    }
    return $row;
}

sub get_latest_geo_gtfs_realtime_feed_instances {
    my ($self, $geo_gtfs_agency_id) = @_;
    my $sql = <<"END";
	select	i.*, f.feed_type
	from	geo_gtfs_realtime_feed_instance i
		join geo_gtfs_realtime_feed f on i.geo_gtfs_realtime_feed_id = f.id
       		where f.geo_gtfs_agency_id = ? and i.is_latest
END
    my $sth = $self->dbh->prepare($sql);
    $sth->execute($geo_gtfs_agency_id);
    my @rows;
    while (my $row = $sth->fetchrow_hashref()) {
	push(@rows, $row);
    }
    return @rows;
}

sub select_or_insert_geo_gtfs_realtime_feed_id {
    my ($self, $geo_gtfs_agency_id, $url, $feed_type) = @_;
    return $self->select_or_insert_id("table_name" => "geo_gtfs_realtime_feed",
				      "id_name" => "id",
				      "key_fields" => { "geo_gtfs_agency_id" => $geo_gtfs_agency_id,
							"url"                => $url,
							"feed_type"          => $feed_type });
}

sub select_or_insert_geo_gtfs_realtime_feed_instance_id {
    my ($self,
	$geo_gtfs_realtime_feed_id,
	$rel_filename,
	$retrieved,
	$last_modified,
	$header_timestamp) = @_;

    # NOTE: if last_modified is undefined, nothing gets replaced
    # because anything = NULL returns false.
    return $self->select_or_insert_id("table_name" => "geo_gtfs_realtime_feed_instance",
				      "id_name" => "id",
				      "key_fields" => { "geo_gtfs_realtime_feed_id" => $geo_gtfs_realtime_feed_id,
							"last_modified"             => $last_modified,
							"header_timestamp"          => $header_timestamp },
				      "more_fields" => { "filename"  => $rel_filename,
							 "retrieved" => $retrieved },
				      "before_insert" => { sql => "update geo_gtfs_realtime_feed_instance set is_latest = 0 " .
							     "where geo_gtfs_realtime_feed_id = ?",
							   bind_values => [$geo_gtfs_realtime_feed_id] },
				     );
}

###############################################################################
# GTFS
###############################################################################

sub get_gtfs_route {
    my ($self, $geo_gtfs_feed_instance_id, $route_id) = @_;
    my $sql = <<"END";
	select *
	from gtfs_routes
	where geo_gtfs_feed_instance_id = ? and route_id = ?
END
    my $sth = $self->dbh->prepare($sql);
    $sth->execute($geo_gtfs_feed_instance_id, $route_id);
    my $result = $sth->fetchrow_hashref();
    return $result;
}

sub get_gtfs_trip {
    my ($self, $geo_gtfs_feed_instance_id, $trip_id, $route_id, $service_id) = @_;
    if (defined $route_id && defined $service_id) {
	my $sql = <<"END";
		select *
		from gtfs_trips
		where geo_gtfs_feed_instance_id = ? and route_id = ? and service_id = ? and trip_id = ?
END
	my $sth = $self->dbh->prepare($sql);
	$sth->execute($geo_gtfs_feed_instance_id, $route_id, $service_id, $trip_id);
	my $result = $sth->fetchrow_hashref();
	return $result;
    } else {
	my $sql = <<"END";
		select *
		from gtfs_trips
		where geo_gtfs_feed_instance_id = ? and trip_id = ?
END
	my $sth = $self->dbh->prepare($sql);
	$sth->execute($geo_gtfs_feed_instance_id, $trip_id);
	my $result = $sth->fetchrow_hashref();
	return $result;
    }
}

sub get_gtfs_stop {
    my ($self, $geo_gtfs_feed_instance_id, $stop_id) = @_;
    my $sql = <<"END";
	select *
	from gtfs_stops
	where geo_gtfs_feed_instance_id = ? and stop_id = ?
END
    my $sth = $self->dbh->prepare($sql);
    $sth->execute($geo_gtfs_feed_instance_id, $stop_id);
    my $result = $sth->fetchrow_hashref();
    return $result;
}

sub get_gtfs_stop_time {
    my ($self, $geo_gtfs_feed_instance_id, $stop_id, $trip_id) = @_;
    my $sql = <<"END";
	select *
	from gtfs_stop_times
	where geo_gtfs_feed_instance_id = ? and stop_id = ? and trip_id = ?
END
    my $sth = $self->dbh->prepare($sql);
    $sth->execute($geo_gtfs_feed_instance_id, $stop_id, $trip_id);
    my $result = $sth->fetchrow_hashref();
    return $result;
}

sub select_or_insert_geo_gtfs_feed_id {
    my ($self, $geo_gtfs_agency_id, $url) = @_;
    return $self->select_or_insert_id("table_name" => "geo_gtfs_feed",
				      "id_name" => "id",
				      "key_fields" => { "geo_gtfs_agency_id" => $geo_gtfs_agency_id,
							"url"                => $url });
}

sub select_or_insert_geo_gtfs_feed_instance_id {
    my ($self,
	$geo_gtfs_feed_id,
	$rel_filename,
	$retrieved,
	$last_modified,
	$header_timestamp) = @_;

    # NOTE: if last_modified is undefined, nothing gets replaced
    # because anything = NULL returns false.
    return $self->select_or_insert_id("table_name" => "geo_gtfs_feed_instance",
				      "id_name" => "id",
				      "key_fields" => { "geo_gtfs_feed_id" => $geo_gtfs_feed_id,
							"last_modified"    => $last_modified },
				      "more_fields" => { "filename"  => $rel_filename,
							 "retrieved" => $retrieved },
				      "before_insert" => { sql => "update geo_gtfs_feed_instance set is_latest = 0 " .
							     "where geo_gtfs_feed_id = ?",
							   bind_values => [$geo_gtfs_feed_id] },
				     );
}

use vars qw(@GTFS_CALENDAR_WDAY_COLUMN);
BEGIN {
    @GTFS_CALENDAR_WDAY_COLUMN = qw(sunday monday tuesday wednesday thursday friday saturday);
}

sub get_geo_gtfs_feed_instance_id {
    my ($self, $geo_gtfs_agency_id, $date) = @_;
    
    if ($date =~ m{^(\d{4})(\d{2})(\d{2})$}) {
	$date = "$1-$2-$3";
    }
    my $time_t = str2time($date);
    my @time_t = localtime($time_t);
    my $yyyymmdd = strftime("%Y%m%d", @time_t);
    my $wday = $time_t[6];	# sunday is zero
    my $wday_column = $GTFS_CALENDAR_WDAY_COLUMN[$wday];

    my $sql = <<"END";
	select geo_gtfs_feed_instance_id, service_id
	from gtfs_calendar c
          join geo_gtfs_feed_instance i on c.geo_gtfs_feed_instance_id = i.id
          join geo_gtfs_feed f          on i.geo_gtfs_feed_id = f.id
        where $wday_column and ? between start_date and end_date
          and geo_gtfs_agency_id = ?
        order by start_date desc, end_date asc
END
    my $sth = $self->dbh->prepare($sql);
    $sth->execute($yyyymmdd, $geo_gtfs_agency_id);
    my @rows;
    my $row = $sth->fetchrow_hashref();
    if (!$row) {
	die(sprintf("No GTFS feed data available on %s.",
		    scalar(localtime(@time_t))));
    }
    return ($row->{geo_gtfs_feed_instance_id},
	    $row->{service_id});
}

sub get_geo_gtfs_feed_instance_id_and_service_id {
    my ($self, $geo_gtfs_agency_id, $date) = @_;
    
    if ($date =~ m{^(\d{4})(\d{2})(\d{2})$}) {
	$date = "$1-$2-$3";
    }
    my $time_t = str2time($date);
    my @time_t = localtime($time_t);
    my $yyyymmdd = strftime("%Y%m%d", @time_t);
    my $wday = $time_t[6];	# sunday is zero
    my $wday_column = $GTFS_CALENDAR_WDAY_COLUMN[$wday];

    my $sql = <<"END";
	select geo_gtfs_feed_instance_id, service_id
	from gtfs_calendar c
          join geo_gtfs_feed_instance i on c.geo_gtfs_feed_instance_id = i.id
          join geo_gtfs_feed f          on i.geo_gtfs_feed_id = f.id
        where $wday_column and ? between start_date and end_date
          and geo_gtfs_agency_id = ?
        order by start_date desc, end_date asc
END
    my $sth = $self->dbh->prepare($sql);
    $sth->execute($yyyymmdd, $geo_gtfs_agency_id);
    my @rows;
    my $row = $sth->fetchrow_hashref();
    if (!$row) {
	die(sprintf("No GTFS feed data available on %s.",
		    scalar(localtime(@time_t))));
    }
    return ($row->{geo_gtfs_feed_instance_id},
	    $row->{service_id});
}

#------------------------------------------------------------------------------

use POSIX qw(strftime);
use Time::ParseDate;

sub get_list_of_current_trips {
    my ($self, $geo_gtfs_feed_instance_id, $time_t) = @_;
    $time_t //= time();

    my @localtime = localtime($time_t);
    my ($hh, $mm, $ss) = @localtime[2, 1, 0];
    my $hhmmss    = sprintf("%02d:%02d:%02d", $hh, $mm, $ss);
    my $hhmmss_xm = sprintf("%02d:%02d:%02d", $hh + 24, $mm, $ss);

    my $service_id    = $self->get_current_day_service_id($geo_gtfs_feed_instance_id, $time_t);
    my $service_id_xm = $self->get_previous_day_service_id($geo_gtfs_feed_instance_id, $time_t);

    my $sql = "
	select   t.trip_id as trip_id,
                 min(st.departure_time) as trip_departure_time,
                 max(st.arrival_time) as trip_arrival_time,
		 t.trip_headsign as trip_headsign,
		 t.trip_short_name as trip_short_name,
		 t.direction_id as direction_id,
		 t.block_id as block_id,
		 r.route_id as route_id,
		 r.route_short_name as route_short_name,
		 r.route_long_name as route_long_name
        from     gtfs_stop_times st
                 join gtfs_trips t
                         on st.trip_id = t.trip_id
                            and st.geo_gtfs_feed_instance_id = t.geo_gtfs_feed_instance_id
		 join gtfs_routes r
                         on t.route_id = r.route_id
                            and t.geo_gtfs_feed_instance_id = r.geo_gtfs_feed_instance_id
        where    t.service_id = ?
	         and t.geo_gtfs_feed_instance_id = ?
        group by t.trip_id
	having   trip_departure_time <= ? and ? < trip_arrival_time
	order by r.route_id, trip_departure_time
    ";

    my $sth = $self->dbh->prepare($sql);
    my @rows;
    $sth->execute($service_id_xm, $hhmmss_xm, $hhmmss_xm);
    while (my $row = $sth->fetchrow_hashref()) {
	push(@rows, $row);
    }
    $sth->execute($service_id, $hhmmss, $hhmmss);
    while (my $row = $sth->fetchrow_hashref()) {
	push(@rows, $row);
    }
    return @rows;
}

sub get_list_of_current_trips_2 {
    my ($self, $geo_gtfs_feed_instance_id, $time_t) = @_;
    $time_t //= time();

    my @localtime = localtime($time_t);
    my ($hh, $mm, $ss) = @localtime[2, 1, 0];
    my $hhmmss    = sprintf("%02d:%02d:%02d", $hh, $mm, $ss);
    my $hhmmss_xm = sprintf("%02d:%02d:%02d", $hh + 24, $mm, $ss);

    my $service_id    = $self->get_current_day_service_id($geo_gtfs_feed_instance_id, $time_t);
    my $service_id_xm = $self->get_previous_day_service_id($geo_gtfs_feed_instance_id, $time_t);

    my $sql1 = "
        select   t.trip_id as trip_id,
		 t.trip_headsign as trip_headsign,
		 t.trip_short_name as trip_short_name,
		 t.direction_id as direction_id,
		 t.block_id as block_id,
		 r.route_id as route_id,
		 r.route_short_name as route_short_name,
		 r.route_long_name as route_long_name
        from     gtfs_trips t
                 join gtfs_routes r
                   on t.route_id = r.route_id
                      and t.geo_gtfs_feed_instance_id = r.geo_gtfs_feed_instance_id
        where    t.service_id = ?
                 and t.geo_gtfs_feed_instance_id = ?
        order by r.route_id, t.trip_id
        ;
    ";
    my $sth1 = $self->dbh->prepare($sql1);
    $sth1->execute($service_id_xm, $geo_gtfs_feed_instance_id);
    my @trips_xm;
    while (my $row = $sth1->fetchrow_hashref()) {
	push(@trips_xm, $row);
    }
    $sth1->execute($service_id, $geo_gtfs_feed_instance_id);
    my @trips;
    while (my $row = $sth1->fetchrow_hashref()) {
	push(@trips, $row);
    }

    my $sql2 = "
        select   min(departure_time) as trip_departure_time, max(arrival_time) as trip_arrival_time
        from     gtfs_stop_times st
        where    st.trip_id = ?
                   and t.geo_gtfs_feed_instance_id = ?
        group by st.trip_id
        ;
    ";
    my $sth2 = $self->dbh->prepare($sql2);
    foreach my $t (@trips, @trips_xm) {
	$sth2->execute($t->{trip_id}, $geo_gtfs_feed_instance_id);
	my $row = $sth2->fetchrow_hashref();
	if ($row) {
	    $t->{trip_departure_time} = $row->{trip_departure_time};
	    $t->{trip_arrival_time}   = $row->{trip_arrival_time};
	}
    }

    return (@trips_xm, @trips);
}

sub get_current_day_service_id {
    my ($self, $geo_gtfs_feed_instance_id, $time_t) = @_;
    $time_t //= time();

    return $self->get_service_id_by_date($time_t);
}

sub get_previous_day_service_id {
    my ($self, $geo_gtfs_feed_instance_id, $time_t) = @_;
    $time_t //= time();

    my $yesterday = parsedate("yesterday", NOW => $time_t);
    return $self->get_service_id_by_date($yesterday);
}

our @GTFS_CALENDAR_COLUMN_NAMES;
BEGIN {
    @GTFS_CALENDAR_COLUMN_NAMES = qw(sunday monday tuesday wednesday
				     thursday friday saturday);
}

sub get_service_id_by_date {
    my ($self, $geo_gtfs_feed_instance_id, $time_t) = @_;
    $time_t //= time();

    my @localtime = localtime($time_t);
    my $yyyymmdd = strftime("%Y%m%d", @localtime);
    my $wday = $localtime[6];
    my $wday_column_name = $GTFS_CALENDAR_COLUMN_NAMES[$wday];

    my $sql2 = "
        select  service_id
        from    gtfs_calendar_dates
        where   geo_gtfs_feed_instance_id = ?
                and exception_type = 2
                and `date` = ?
    ";
    my $sth2 = $self->dbh->prepare($sql2);
    $sth2->execute($geo_gtfs_feed_instance_id, $yyyymmdd);
    if (my $row = $sth2->fetchrow_array()) {
	return $row->{service_id};
    }

    my $sql1 = "
        select  service_id
        from    gtfs_calendar
        where   geo_gtfs_feed_instance_id = ?
                and $wday_column_name
                and ? between start_date and end_date
    ";
    my $sth1 = $self->dbh->prepare($sql1);
    $sth1->execute($geo_gtfs_feed_instance_id, $yyyymmdd);
    if (my $row = $sth1->fetchrow_array()) {
	return $row->{service_id};
    }
}

###############################################################################
# AGENCIES
###############################################################################

sub select_geo_gtfs_agency_by_id {
    my ($self, $geo_gtfs_agency_id) = @_;
    my $sth = $self->dbh->prepare("select * from geo_gtfs_agency where id = ?");
    $sth->execute($geo_gtfs_agency_id);
    my $row = $sth->fetchrow_hashref();
    if (!$row) {
	die("No agency with id: $geo_gtfs_agency_id\n");
    }
    return $row;
}

sub select_or_insert_geo_gtfs_agency_id {
    my ($self, $geo_gtfs_agency_name) = @_;
    return $geo_gtfs_agency_name if defined $geo_gtfs_agency_name && $geo_gtfs_agency_name =~ m{^\d+$};
    return $self->select_or_insert_id("table_name" => "geo_gtfs_agency",
				      "id_name" => "id",
				      "key_fields" => { "name" => $geo_gtfs_agency_name });
}

###############################################################################
# DATABASE
###############################################################################

sub sql_to_update_tables {
    my ($self) = @_;
    my @result;
    foreach my $table (@{$TABLES}) {
        my $sth = $self->dbh->table_info(undef, undef, $table->{name}, "TABLE");
        my $row = $sth->fetchrow_hashref;
        if (!$row) {
            push(@result, $self->sql_to_create_table($table));
        } else {
            push(@result, $self->sql_to_alter_table($table));
        }
    }
    foreach my $index (@{$INDEXES}) {
        if (!$self->get_index_info(undef, $index, 1)) {
            push(@result, $self->sql_to_create_index($index));
        }
    }
    return @result if wantarray;
    return \@result;
}

sub sql_to_alter_table {
    my ($self, $table) = @_;
    my @result;
    foreach my $column (@{$table->{columns}}) {
        my $sth = $self->dbh->column_info(undef, undef, $table->{name}, $column->{name});
        my $row = $sth->fetchrow_hashref;
        if (!$row) {
            push(@result, $self->sql_to_alter_table_add_column($table, $column));
        }
    }
  index:
    foreach my $index (@{$table->{indexes}}) {
        my $sth = $self->dbh->statistics_info(undef, undef, $table->{name}, 0, 0);
        if (!$self->get_index_info($table, $index, 1)) {
            push(@result, $self->sql_to_create_index($index));
        }
    }
    return @result if wantarray;
    return \@result;
}

sub get_index_info {
    my ($self, $table, $index, $wantflag) = @_;
    my @result;
    my $sth = $self->dbh->statistics_info(undef, undef, $index->{table} // $table->{name}, 0, 0);
    while (my $row = $sth->fetchrow_hashref) {
        if ($row->{INDEX_NAME} eq $index->{name}) {
            return 1 if $wantflag;
            push(@result, $row);
        }
    }
    return 0 if $wantflag;
    return @result if wantarray;
    return \@result;
}

sub sql_to_alter_table_add_column {
    my ($self, $table, $column) = @_;
    my $sql = sprintf("alter table %s add column %s;\n",
                      $self->dbh->quote_identifier($table->{name}),
                      $self->sql_to_specify_table_column($column));
    return $sql;
}

sub sql_to_drop_tables {
    my ($self) = @_;
    my @result;
    foreach my $table (@{$TABLES}) {
        push(@result, $self->sql_to_drop_table($table));
    }
    return @result if wantarray;
    return \@result;
}

sub sql_to_create_tables {
    my ($self) = @_;
    my @result;
    foreach my $table (@{$TABLES}) {
        push(@result, $self->sql_to_create_table($table));
    }
    push(@result, $self->sql_to_create_indexes);
    return @result if wantarray;
    return \@result;
}

sub sql_to_drop_table {
    my ($self, $table) = @_;
    my $sql = sprintf("drop table if exists %s;\n",
                      $self->quote_table_name($table->{name}));
    return $sql;
}

sub sql_to_create_table {
    my ($self, $table) = @_;
    my @result;

    my $sql;
    $sql = sprintf("create table if not exists %s (\n",
                   $self->quote_table_name($table->{name}));
    my @spec = map { $self->sql_to_specify_table_column($_) }
        @{$table->{columns}};
    if (scalar @spec) {
        $sql .= "    " . join(",\n    ", @spec);
        $sql .= "\n";
    }
    $sql .= ");\n";
    push(@result, $sql);

    if (defined $table->{indexes}) {
        my @indexes = @{$table->{indexes}};
        foreach my $index (@indexes) {
            my %index = %$index;
            $index{table} = $table->{name};
            push(@result, $self->sql_to_create_index(\%index));
        }
    }

    return @result if wantarray;
    return \@result;
}

# column definition
sub sql_to_specify_table_column {
    my ($self, $column) = @_;
    my $spec = sprintf("%s", $self->quote_column_name($column->{name}));
    $spec .= sprintf(" %s", $column->{type});
    $spec .= sprintf("(%d)", $column->{size}) if defined $column->{size};
    if (defined $column->{nullable} and not $column->{primary_key}) { # primary_key implies not null
        if ($column->{nullable}) {
            $spec .= " null";
        } else {
            $spec .= " not null";
        }
    }
    $spec .= " primary key" if $column->{primary_key};
    $spec .= " autoincrement" if $column->{auto_increment};
    $spec .= sprintf(" references %s(%s)",
                     $self->quote_table_name($column->{references}->{table}),
                     $self->quote_column_name($column->{references}->{column}))
        if defined $column->{references};
    if (defined $column->{default}) {
        if ($column->{type} eq "integer" || $column->{type} eq "numeric") {
            $spec .= sprintf(" default %d", $column->{default});
        } elsif ($column->{type} eq "varchar" || $column->{type} eq "text") {
            $spec .= sprintf(" default %s", $self->dbh->quote($column->{default}));
        }
    }
    return $spec;
}

sub sql_to_create_indexes {
    my ($self) = @_;
    my @result;
    foreach my $index (@{$INDEXES}) {
        push(@result, $self->sql_to_create_index($index));
    }
    return @result if wantarray;
    return \@result;
}

sub sql_to_create_index {
    my ($self, $index) = @_;
    my $sql = "create";
    $sql .= " unique" if $index->{unique};
    $sql .= " index";
    $sql .= " if not exists";
    my $name = $index->{name} // sprintf("%s__idx__%s",
                                         $index->{table},
                                         join("__", @{$index->{columns}}));
    $sql .= sprintf(" %s", $self->quote_index_name($name));
    $sql .= sprintf(" on %s(%s);\n",
                    $self->quote_table_name($index->{table}),
                    join(", ", map { $self->quote_column_name($_) } @{$index->{columns}}));
    return $sql;
}

sub quote_table_name {
    my ($self, $table_name) = @_;
    return $self->dbh->quote_identifier($table_name);
}

sub quote_column_name {
    my ($self, $column_name) = @_;
    return $self->dbh->quote_identifier($column_name);
}

sub quote_index_name {
    my ($self, $index_name) = @_;
    return $self->dbh->quote_identifier($index_name);
}

###############################################################################
# MISC.
###############################################################################

sub DESTROY {
    my ($self) = @_;
    $self->close_dbh();
}

=head1 NAME

Geo::GTFS2::DB - Database query handling for Geo::GTFS2

=head1 DESCRIPTION

Database handling routines.  Internally used for Geo::GTFS2, primarily.

=head1 METHODS

=head2 Constructor

    my $db = Geo::GTFS2::DB->new();

=head2 dbh

    my $dbh = $db->dbh();

Returns a DBI database connection handle.  Creates one first, if one
has not yet been created.

=head2 select_or_insert_id

    my $id = $db->select_or_insert_id(
        table_name => "table",
        id_name    => "id",
        key_fields => {
                      },
        before_insert => {
                             sql         => ...,
                             bind_values => [...],
                         },
        more_fields => {
                       },
    );

=head2 execute_multiple_sql_queries

    $db->execute_multiple_sql_queries(<<"END");
        create table A (...);
        create table B (...);
        create table C (...);
    END

Each statement must be terminated by a semicolon followed by a newline.



p=cut

1;
