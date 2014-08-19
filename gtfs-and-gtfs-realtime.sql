create table geo_gtfs (				name			varchar(32)	not null	primary key,
						value			text		null
);
delete from geo_gtfs where name = 'geo_gtfs.db.version';
insert into geo_gtfs (name, value) values('geo_gtfs.db.version', '0.1');
create table geo_gtfs_agency (			id			integer				primary key autoincrement,
						name			varchar(64)	not null	-- preferably the transit agency's domain name, without a www. prefix. - examples: 'ridetarc.org', 'ttc.ca'
);
	create index geo_gtfs_agency_name on geo_gtfs_agency(name);
create table geo_gtfs_feed (			id			integer				primary key autoincrement,
						geo_gtfs_agency_id	integer		not null	foreign key references geo_gtfs_agency(id),
						url			text		not null,
						is_active		integer		not null	-- updated when feeds added, removed
);
	create index geo_gtfs_feed_active on geo_gtfs_feed(is_active);
create table geo_gtfs_feed_instance (		id			integer				primary key autoincrement,
						geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						filename		text		not null,
						retrieved		integer		not null,
						last_modified		integer		null,		-- SHOULD be specified, but some servers omit.
						is_latest		integer		not null	
);
	create index geo_gtfs_feed_instance_latest on geo_gtfs_feed_instance(is_latest);
create table geo_gtfs_realtime_feed (		id			integer				primary key autoincrement,
						geo_gtfs_agency_id	integer		not null	foreign key references geo_gtfs_agency(id),
						url			text		not null,
						feed_type		varchar(16)	not null,	-- 'updates', 'positions', 'alerts', 'all'
						is_active		integer		not null	-- updated when feeds added, removed
);
	create index geo_gtfs_realtime_feed_type on geo_gtfs_realtime_feed(feed_type);
	create index geo_gtfs_realtime_feed_active on geo_gtfs_realtime_feed(is_active);
create table geo_gtfs_realtime_feed_instance (	id			integer				primary key autoincrement,
						geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_realtime_feed(id),
						filename		text		not null,
						retrieved		integer		not null,
						last_modified		integer		null,
						is_latest		integer		not null
);
	create index geo_gtfs_realtime_feed_instance_latest on geo_gtfs_realtime_feed_instance(is_latest);
-------------------------------------------------------------------------------
create table gtfs_agency (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						agency_id		text		null,		-- indexed -- for feeds containing only one agency, this can be NULL.
						agency_name		text		not null,
						agency_url		text		not null,
						agency_timezone		text		not null,
						agency_lang		varchar(2)	null,
						agency_phone		text		null,
						agency_fare_url		text		null
);
	create unique index gtfs_agency_id on gtfs_agency(geo_gtfs_feed_id, agency_id);

create table gtfs_stops (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						stop_id			text		not null,	-- indexed --
						stop_code		text		null,
						stop_name		text		not null,
						stop_desc		text		null,
						stop_lat		numeric		not null,
						stop_lon		numeric		not null,
						zone_id			text		null,		-- indexed --
						stop_url		text		null,
						location_type		integer		null,
						parent_station		text		null,
						stop_timezone		text		null,
						wheelchair_boarding	integer		null
);
	create unique index gtfs_stops_id                  on gtfs_stops(geo_gtfs_feed_id, stop_id);
	create        index gtfs_stops_zone_id             on gtfs_stops(geo_gtfs_feed_id, zone_id);
	create        index gtfs_stops_location_type       on gtfs_stops(geo_gtfs_feed_id, location_type);
	create        index gtfs_stops_parent_station      on gtfs_stops(geo_gtfs_feed_id, parent_station);
	create        index gtfs_stops_wheelchair_boarding on gtfs_stops(geo_gtfs_feed_id, wheelchair_boarding);

create table gtfs_routes (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						route_id		text		not null,	-- indexed --
						agency_id		text		null		foreign key references gtfs_agency(id),
						route_short_name	text		not null,
						route_long_name		text		not null,
						route_desc		text		null,
						route_type		integer		not null,
						route_url		text		null,
						route_color		varchar(6)	null,
						route_text_color	varchar(6)	null
);
	create unique index gtfs_routes_id        on gtfs_routes (geo_gtfs_feed_id, route_id, agency_id);
	create        index gtfs_routes_agency_id on gtfs_routes (geo_gtfs_feed_id, agency_id);
	create        index gtfs_routes_route_id  on gtfs_routes (geo_gtfs_feed_id, route_id);
	create        index gtfs_routes_type      on gtfs_routes (geo_gtfs_feed_id, route_type);

create table gtfs_trips (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						route_id		text		not null	foreign key references gtfs_routes(id),
						service_id		text		not null,	-- indexed --
						trip_id			text		not null,	-- indexed --
						trip_headsign		text		null,
						trip_short_name		text		null,
						direction_id		integer		null,		-- indexed --
						block_id		text		null,		-- indexed --
						shape_id		text		null		foreign key references gtfs_shapes(id),
						wheelchair_accessible	integer		null,
						bikes_allowed		integer		null
);
	create unique index gtfs_trips_id           on gtfs_trips (geo_gtfs_feed_id, trip_id);
	create        index gtfs_trips_route_id     on gtfs_trips (geo_gtfs_feed_id, route_id);
	create        index gtfs_trips_service_id   on gtfs_trips (geo_gtfs_feed_id, service_id);
	create        index gtfs_trips_direction_id on gtfs_trips (geo_gtfs_feed_id, direction_id);
	create        index gtfs_trips_block_id     on gtfs_trips (geo_gtfs_feed_id, block_id);
	create        index gtfs_trips_shape_id     on gtfs_trips (geo_gtfs_feed_id, shape_id);

create table gtfs_stop_times (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						trip_id			text		not null	foreign key references gtfs_trips(id),
						arrival_time		varchar(8)	not null,
						departure_time		varchar(8)	not null,
						stop_id			text		not null	foreign key references gtfs_stops(id),
						stop_sequence		integer		not null,
						stop_headsign		text		null,
						pickup_type		integer		null,
						drop_off_type		integer		null,
						shape_dist_traveled	numeric		null
);
	create unique index on gtfs_stop_times (geo_gtfs_feed_id, stop_time_id);
	create        index on gtfs_stop_times (geo_gtfs_feed_id, trip_id);
	create        index on gtfs_stop_times (geo_gtfs_feed_id, stop_sequence);

create table gtfs_calendar (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						service_id		text		not null,	-- indexed --
						monday			integer		not null,
						tuesday			integer		not null,
						wednesday		integer		not null,
						thursday		integer		not null,
						friday			integer		not null,
						saturday		integer		not null,
						sunday			integer		not null,
						start_date		varchar(8)	not null,
						end_date		varchar(8)	not null
);
	create        index on gtfs_calendar(geo_gtfs_feed_id, service_id);
	create        index on gtfs_calendar(geo_gtfs_feed_id, monday);
	create        index on gtfs_calendar(geo_gtfs_feed_id, tuesday);
	create        index on gtfs_calendar(geo_gtfs_feed_id, wednesday);
	create        index on gtfs_calendar(geo_gtfs_feed_id, thursday);
	create        index on gtfs_calendar(geo_gtfs_feed_id, friday);
	create        index on gtfs_calendar(geo_gtfs_feed_id, saturday);
	create        index on gtfs_calendar(geo_gtfs_feed_id, sunday);
	create        index on gtfs_calendar(geo_gtfs_feed_id, start_date);
	create        index on gtfs_calendar(geo_gtfs_feed_id, end_date);

create table gtfs_calendar_dates (		geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						service_id		text		not null	-- indexed --,
						`date`			varchar(8)	not null,
						exception_type		integer		not null
);
	create        index on gtfs_calendar_dates(geo_gtfs_feed_id, service_id);
	create        index on gtfs_calendar_dates(geo_gtfs_feed_id, `date`);
	create        index on gtfs_calendar_dates(geo_gtfs_feed_id, exception_type);

create table gtfs_fare_attributes (		geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						fare_id			text		not null	-- indexed --,
						price			numeric		not null,
						currency_type		text		not null,
						payment_method		integer		not null,
						transfers		integer		not null,
						transfer_duration	integer		null
);
	create        index on gtfs_fare_attributes(geo_gtfs_feed_id, fare_id);

create table gtfs_fare_rules (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						fare_id			text		not null	foreign key references gtfs_fare_attributes(fare_id),
						route_id		text		null		foreign key references gtfs_routes(id),
						origin_id		text		null,		-- indexed --
						destination_id		text		null,		-- indexed --
						contains_id		text		null		-- indexed --
);
	create        index on gtfs_fare_rules(geo_gtfs_feed_id, fare_id);
	create        index on gtfs_fare_rules(geo_gtfs_feed_id, route_id);
	create        index on gtfs_fare_rules(geo_gtfs_feed_id, origin_id);
	create        index on gtfs_fare_rules(geo_gtfs_feed_id, destination_id);
	create        index on gtfs_fare_rules(geo_gtfs_feed_id, contains_id);

create table gtfs_shapes (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						shape_id		text		not null,	-- indexed --
						shape_pt_lat		numeric		not null,
						shape_pt_lon		numeric		not null,
						shape_pt_sequence	integer		not null,	-- indexed --
						shape_dist_traveled	numeric		null
);
	create        index on gtfs_fare_rules(geo_gtfs_feed_id, shape_id);
	create        index on gtfs_fare_rules(geo_gtfs_feed_id, shape_id, shape_pt_sequence);

create table gtfs_frequencies (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						trip_id			text		null		foreign key references gtfs_trips(id),
						start_time		varchar(8)	null, --indexed
						end_time		varchar(8)	null, --indexed
						headway_secs		integer		null,
						exact_times		integer		null
);
	create        index on gtfs_frequencies(geo_gtfs_feed_id, trip_id);
	create        index on gtfs_frequencies(geo_gtfs_feed_id, start_time);
	create        index on gtfs_frequencies(geo_gtfs_feed_id, end_time);

create table gtfs_transfers (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						from_stop_id		text		not null	foreign key references gtfs_stops(id),
						to_stop_id		text		not null	foreign key references gtfs_stops(id),
						transfer_type		integer		not null,
						min_transfer_time	integer		null
);
	create        index on gtfs_transfers(from_stop_id);
	create        index on gtfs_transfers(to_stop_id);

create table gtfs_feed_info (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						feed_publisher_name	text		not null,
						feed_publisher_url	text		not null,
						feed_lang		text		not null,
						feed_start_date		varchar(8)	null,
						feed_end_date		varchar(8)	null,
						feed_version		text		null
);

create index gtfs_agency__geo_gtfs_feed_id		on gtfs_agency		(geo_gtfs_feed_id);
create index gtfs_stops__geo_gtfs_feed_id		on gtfs_stops		(geo_gtfs_feed_id);
create index gtfs_routes__geo_gtfs_feed_id		on gtfs_routes		(geo_gtfs_feed_id);
create index gtfs_trips__geo_gtfs_feed_id		on gtfs_trips		(geo_gtfs_feed_id);
create index gtfs_stop__geo_gtfs_feed_id		on gtfs_stop		(geo_gtfs_feed_id);
create index gtfs_calendar__geo_gtfs_feed_id		on gtfs_calendar	(geo_gtfs_feed_id);
create index gtfs_calendar__geo_gtfs_feed_id		on gtfs_calendar	(geo_gtfs_feed_id);
create index gtfs_fare__geo_gtfs_feed_id		on gtfs_fare		(geo_gtfs_feed_id);
create index gtfs_fare__geo_gtfs_feed_id		on gtfs_fare		(geo_gtfs_feed_id);
create index gtfs_shapes__geo_gtfs_feed_id		on gtfs_shapes		(geo_gtfs_feed_id);
create index gtfs_frequencies__geo_gtfs_feed_id		on gtfs_frequencies	(geo_gtfs_feed_id);
create index gtfs_transfers__geo_gtfs_feed_id		on gtfs_transfers	(geo_gtfs_feed_id);
create index gtfs_feed__geo_gtfs_feed_id		on gtfs_feed		(geo_gtfs_feed_id);

