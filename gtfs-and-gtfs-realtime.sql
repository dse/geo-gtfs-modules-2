drop table if exists geo_gtfs;
create table geo_gtfs (				name				varchar(32)	not null	primary key,
						value				text		null
);
delete from geo_gtfs where name = 'geo_gtfs.db.version';
insert into geo_gtfs (name, value) values('geo_gtfs.db.version', '0.1');

drop table if exists geo_gtfs_agency;
create table geo_gtfs_agency (			id				integer				primary key autoincrement,
						name				varchar(64)	not null	-- preferably the transit agency's domain name, without a www. prefix. - examples: 'ridetarc.org', 'ttc.ca'
);
create index geo_gtfs_agency_01 on geo_gtfs_agency(name);

drop table if exists geo_gtfs_feed;
create table geo_gtfs_feed (			id				integer				primary key autoincrement,
						geo_gtfs_agency_id		integer		not null	references geo_gtfs_agency(id),
						url				text		not null,
						is_active			integer		not null	-- updated when feeds added, removed
);
create index geo_gtfs_feed_01 on geo_gtfs_feed(is_active);

drop table if exists geo_gtfs_feed_instance;
create table geo_gtfs_feed_instance (		id				integer				primary key autoincrement,
						geo_gtfs_feed_id		integer		not null	references geo_gtfs_feed(id),
						filename			text		not null,
						retrieved			integer		not null,
						last_modified			integer		null,		-- SHOULD be specified, but some servers omit.
						is_latest			integer		not null	
);
create index geo_gtfs_feed_instance_01 on geo_gtfs_feed_instance(is_latest);

drop table if exists geo_gtfs_realtime_feed;
create table geo_gtfs_realtime_feed (		id				integer				primary key autoincrement,
						geo_gtfs_agency_id		integer		not null	references geo_gtfs_agency(id),
						url				text		not null,
						feed_type			varchar(16)	not null,	-- 'updates', 'positions', 'alerts', 'all'
						is_active			integer		not null	-- updated when feeds added, removed
);
create index geo_gtfs_realtime_feed_01 on geo_gtfs_realtime_feed(feed_type);
create index geo_gtfs_realtime_feed_02 on geo_gtfs_realtime_feed(is_active);

drop table if exists geo_gtfs_realtime_feed_instance;
create table geo_gtfs_realtime_feed_instance (	id				integer				primary key autoincrement,
						geo_gtfs_feed_id		integer		not null	references geo_gtfs_realtime_feed(id),
						filename			text		not null,
						retrieved			integer		not null,
						last_modified			integer		null,
						is_latest			integer		not null
);
create index geo_gtfs_realtime_feed_instance_01 on geo_gtfs_realtime_feed_instance(is_latest);
-------------------------------------------------------------------------------
drop table if exists gtfs_agency;
create table gtfs_agency (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						agency_id			text		null,		-- indexed -- for feeds containing only one agency, this can be NULL.
						agency_name			text		not null,
						agency_url			text		not null,
						agency_timezone			text		not null,
						agency_lang			varchar(2)	null,
						agency_phone			text		null,
						agency_fare_url			text		null
);
create unique index gtfs_agency_01 on gtfs_agency(geo_gtfs_feed_instance_id, agency_id);

drop table if exists gtfs_stops;
create table gtfs_stops (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						stop_id				text		not null,	-- indexed --
						stop_code			text		null,
						stop_name			text		not null,
						stop_desc			text		null,
						stop_lat			numeric		not null,
						stop_lon			numeric		not null,
						zone_id				text		null,		-- indexed --
						stop_url			text		null,
						location_type			integer		null,
						parent_station			text		null,
						stop_timezone			text		null,
						wheelchair_boarding		integer		null
);
create unique index gtfs_stops_01 on gtfs_stops(geo_gtfs_feed_instance_id, stop_id);
create        index gtfs_stops_02 on gtfs_stops(geo_gtfs_feed_instance_id, zone_id);
create        index gtfs_stops_03 on gtfs_stops(geo_gtfs_feed_instance_id, location_type);
create        index gtfs_stops_04 on gtfs_stops(geo_gtfs_feed_instance_id, parent_station);
create        index gtfs_stops_05 on gtfs_stops(geo_gtfs_feed_instance_id, wheelchair_boarding);

drop table if exists gtfs_routes;
create table gtfs_routes (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						route_id			text		not null,	-- indexed --
						agency_id			text		null		references gtfs_agency(id),
						route_short_name		text		not null,
						route_long_name			text		not null,
						route_desc			text		null,
						route_type			integer		not null,
						route_url			text		null,
						route_color			varchar(6)	null,
						route_text_color		varchar(6)	null
);
create unique index gtfs_routes_01 on gtfs_routes (geo_gtfs_feed_instance_id, route_id, agency_id);
create        index gtfs_routes_02 on gtfs_routes (geo_gtfs_feed_instance_id, agency_id);
create        index gtfs_routes_03 on gtfs_routes (geo_gtfs_feed_instance_id, route_id);
create        index gtfs_routes_04 on gtfs_routes (geo_gtfs_feed_instance_id, route_type);

drop table if exists gtfs_trips;
create table gtfs_trips (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						route_id			text		not null	references gtfs_routes(id),
						service_id			text		not null,	-- indexed --
						trip_id				text		not null,	-- indexed --
						trip_headsign			text		null,
						trip_short_name			text		null,
						direction_id			integer		null,		-- indexed --
						block_id			text		null,		-- indexed --
						shape_id			text		null		references gtfs_shapes(id),
						wheelchair_accessible		integer		null,
						bikes_allowed			integer		null
);
create unique index gtfs_trips_01 on gtfs_trips (geo_gtfs_feed_instance_id, trip_id);
create        index gtfs_trips_02 on gtfs_trips (geo_gtfs_feed_instance_id, route_id);
create        index gtfs_trips_03 on gtfs_trips (geo_gtfs_feed_instance_id, service_id);
create        index gtfs_trips_04 on gtfs_trips (geo_gtfs_feed_instance_id, direction_id);
create        index gtfs_trips_05 on gtfs_trips (geo_gtfs_feed_instance_id, block_id);
create        index gtfs_trips_06 on gtfs_trips (geo_gtfs_feed_instance_id, shape_id);

drop table if exists gtfs_stop_times;
create table gtfs_stop_times (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						trip_id				text		not null	references gtfs_trips(id),
						arrival_time			varchar(8)	not null,
						departure_time			varchar(8)	not null,
						stop_id				text		not null	references gtfs_stops(id),
						stop_sequence			integer		not null,
						stop_headsign			text		null,
						pickup_type			integer		null,
						drop_off_type			integer		null,
						shape_dist_traveled		numeric		null
);
create unique index gtfs_stop_times_01 on gtfs_stop_times (geo_gtfs_feed_instance_id, stop_id);
create        index gtfs_stop_times_02 on gtfs_stop_times (geo_gtfs_feed_instance_id, trip_id);
create        index gtfs_stop_times_03 on gtfs_stop_times (geo_gtfs_feed_instance_id, stop_sequence);

drop table if exists gtfs_calendar;
create table gtfs_calendar (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						service_id			text		not null,	-- indexed --
						monday				integer		not null,
						tuesday				integer		not null,
						wednesday			integer		not null,
						thursday			integer		not null,
						friday				integer		not null,
						saturday			integer		not null,
						sunday				integer		not null,
						start_date			varchar(8)	not null,
						end_date			varchar(8)	not null
);
create        index gtfs_calendar_01 on gtfs_calendar(geo_gtfs_feed_instance_id, service_id);
create        index gtfs_calendar_02 on gtfs_calendar(geo_gtfs_feed_instance_id, monday);
create        index gtfs_calendar_03 on gtfs_calendar(geo_gtfs_feed_instance_id, tuesday);
create        index gtfs_calendar_04 on gtfs_calendar(geo_gtfs_feed_instance_id, wednesday);
create        index gtfs_calendar_05 on gtfs_calendar(geo_gtfs_feed_instance_id, thursday);
create        index gtfs_calendar_06 on gtfs_calendar(geo_gtfs_feed_instance_id, friday);
create        index gtfs_calendar_07 on gtfs_calendar(geo_gtfs_feed_instance_id, saturday);
create        index gtfs_calendar_08 on gtfs_calendar(geo_gtfs_feed_instance_id, sunday);
create        index gtfs_calendar_09 on gtfs_calendar(geo_gtfs_feed_instance_id, start_date);
create        index gtfs_calendar_10 on gtfs_calendar(geo_gtfs_feed_instance_id, end_date);

drop table if exists gtfs_calendar_dates;
create table gtfs_calendar_dates (		geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						service_id			text		not null,	-- indexed --
						`date`				varchar(8)	not null,
						exception_type			integer		not null
);
create        index gtfs_calendar_dates_01 on gtfs_calendar_dates(geo_gtfs_feed_instance_id, service_id);
create        index gtfs_calendar_dates_02 on gtfs_calendar_dates(geo_gtfs_feed_instance_id, `date`);
create        index gtfs_calendar_dates_03 on gtfs_calendar_dates(geo_gtfs_feed_instance_id, exception_type);

drop table if exists gtfs_fare_attributes;
create table gtfs_fare_attributes (		geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						fare_id				text		not null,	-- indexed --
						price				numeric		not null,
						currency_type			text		not null,
						payment_method			integer		not null,
						transfers			integer		not null,
						transfer_duration		integer		null
);
create        index gtfs_fare_attributes_01 on gtfs_fare_attributes(geo_gtfs_feed_instance_id, fare_id);

drop table if exists gtfs_fare_rules;
create table gtfs_fare_rules (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						fare_id				text		not null	references gtfs_fare_attributes(fare_id),
						route_id			text		null		references gtfs_routes(id),
						origin_id			text		null,		-- indexed --
						destination_id			text		null,		-- indexed --
						contains_id			text		null		-- indexed --
);
create        index gtfs_fare_rules_01 on gtfs_fare_rules(geo_gtfs_feed_instance_id, fare_id);
create        index gtfs_fare_rules_02 on gtfs_fare_rules(geo_gtfs_feed_instance_id, route_id);
create        index gtfs_fare_rules_03 on gtfs_fare_rules(geo_gtfs_feed_instance_id, origin_id);
create        index gtfs_fare_rules_04 on gtfs_fare_rules(geo_gtfs_feed_instance_id, destination_id);
create        index gtfs_fare_rules_05 on gtfs_fare_rules(geo_gtfs_feed_instance_id, contains_id);

drop table if exists gtfs_shapes;
create table gtfs_shapes (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						shape_id			text		not null,	-- indexed --
						shape_pt_lat			numeric		not null,
						shape_pt_lon			numeric		not null,
						shape_pt_sequence		integer		not null,	-- indexed --
						shape_dist_traveled		numeric		null
);
create        index gtfs_shapes_01 on gtfs_shapes(geo_gtfs_feed_instance_id, shape_id);
create        index gtfs_shapes_02 on gtfs_shapes(geo_gtfs_feed_instance_id, shape_id, shape_pt_sequence);

drop table if exists gtfs_frequencies;
create table gtfs_frequencies (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						trip_id				text		null		references gtfs_trips(id),
						start_time			varchar(8)	null, --indexed
						end_time			varchar(8)	null, --indexed
						headway_secs			integer		null,
						exact_times			integer		null
);
create        index gtfs_frequencies_01 on gtfs_frequencies(geo_gtfs_feed_instance_id, trip_id);
create        index gtfs_frequencies_02 on gtfs_frequencies(geo_gtfs_feed_instance_id, start_time);
create        index gtfs_frequencies_03 on gtfs_frequencies(geo_gtfs_feed_instance_id, end_time);

drop table if exists gtfs_transfers;
create table gtfs_transfers (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						from_stop_id			text		not null	references gtfs_stops(id),
						to_stop_id			text		not null	references gtfs_stops(id),
						transfer_type			integer		not null,
						min_transfer_time		integer		null
);
create        index gtfs_transfers_01 on gtfs_transfers(from_stop_id);
create        index gtfs_transfers_02 on gtfs_transfers(to_stop_id);

drop table if exists gtfs_feed_info;
create table gtfs_feed_info (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						feed_publisher_name		text		not null,
						feed_publisher_url		text		not null,
						feed_lang			text		not null,
						feed_start_date			varchar(8)	null,
						feed_end_date			varchar(8)	null,
						feed_version			text		null
);

create index geo_gtfs_agency_00			on gtfs_agency		(geo_gtfs_feed_instance_id);
create index geo_gtfs_stops_00			on gtfs_stops		(geo_gtfs_feed_instance_id);
create index geo_gtfs_routes_00			on gtfs_routes		(geo_gtfs_feed_instance_id);
create index geo_gtfs_trips_00			on gtfs_trips		(geo_gtfs_feed_instance_id);
create index geo_gtfs_stop_times_00		on gtfs_stop_times	(geo_gtfs_feed_instance_id);
create index geo_gtfs_calendar_00		on gtfs_calendar	(geo_gtfs_feed_instance_id);
create index geo_gtfs_calendar_dates_00		on gtfs_calendar_dates	(geo_gtfs_feed_instance_id);
create index geo_gtfs_fare_attributes_00	on gtfs_fare_attributes	(geo_gtfs_feed_instance_id);
create index geo_gtfs_fare_rules_00		on gtfs_fare_rules	(geo_gtfs_feed_instance_id);
create index geo_gtfs_shapes_00			on gtfs_shapes		(geo_gtfs_feed_instance_id);
create index geo_gtfs_frequencies_00		on gtfs_frequencies	(geo_gtfs_feed_instance_id);
create index geo_gtfs_transfers_00		on gtfs_transfers	(geo_gtfs_feed_instance_id);
create index geo_gtfs_feed_info_00		on gtfs_feed_info	(geo_gtfs_feed_instance_id);

