create table geo_gtfs (				name			varchar(32)	not null	primary key,
						value			text		null
);
create table geo_gtfs_agency (			id			integer				primary key autoincrement,
						name			varchar(64)	not null	-- preferably the transit agency's domain name, without a www. prefix. - examples: 'ridetarc.org', 'ttc.ca'
);
create table geo_gtfs_feed (			id			integer				primary key autoincrement,
						geo_gtfs_agency_id	integer		not null	foreign key references geo_gtfs_agency(id),
						url			text		not null,
						is_active		integer		not null	-- updated when feeds added, removed
);
create table geo_gtfs_feed_instance (		id			integer				primary key autoincrement,
						geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						filename		text		not null,
						retrieved		integer		not null,
						last_modified		integer		null,		-- SHOULD be specified, but some servers omit.
						is_latest		integer		not null	
);
create table geo_gtfs_realtime_feed (		id			integer				primary key autoincrement,
						geo_gtfs_agency_id	integer		not null	foreign key references geo_gtfs_agency(id),
						url			text		not null,
						feed_type		varchar(16)	not null,	-- 'updates', 'positions', 'alerts', 'all'
						is_active		integer		not null	-- updated when feeds added, removed
);
create table geo_gtfs_realtime_feed_instance (	id			integer				primary key autoincrement,
						geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_realtime_feed(id),
						filename		text		not null,
						retrieved		integer		not null,
						last_modified		integer		null,
						is_latest		integer		not null
);
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
create table gtfs_stops (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						stop_id			text		not null,	-- indexed --
						stop_code		text		null,
						stop_name		text		not null,
						stop_desc		text		null,
						stop_lat		numeric		not null,
						stop_lon		numeric		not null,
						zone_id			text		null,
						stop_url		text		null,
						location_type		integer		null,
						parent_station		text		null,
						stop_timezone		text		null,
						wheelchair_boarding	integer		null
);
create table gtfs_routes (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						route_id		text		not null,	-- indexed --
						agency_id		text		null,		-- indexed --
						route_short_name	text		not null,
						route_long_name		text		not null,
						route_desc		text		null,
						route_type		integer		not null,
						route_url		text		null,
						route_color		varchar(6)	null,
						route_text_color	varchar(6)	null
);
create table gtfs_trips (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						route_id		text		not null,
						service_id		text		not null,
						trip_id			text		not null,	-- indexed --
						trip_headsign		text		null,
						trip_short_name		text		null,
						direction_id		integer		null,
						block_id		text		null,
						shape_id		text		null,
						wheelchair_accessible	integer		null,
						bikes_allowed		integer		null
);
create table gtfs_stop_times (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						trip_id			text		not null	foreign key references gtfs_trips(id),
						arrival_time		varchar(8)	not null,
						departure_time		varchar(8)	not null,
						stop_id			text		not null,
						stop_sequence		integer		not null,
						stop_headsign		text		null,
						pickup_type		integer		null,
						drop_off_type		integer		null,
						shape_dist_traveled	numeric		null
);
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
create table gtfs_calendar_dates (		geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						service_id		text		not null,
						`date`			varchar(8)	not null,
						exception_type		integer		not null
);
create table gtfs_fare_attributes (		geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						fare_id			text		not null,
						price			numeric		not null,
						currency_type		text		not null,
						payment_method		integer		not null,
						transfers		integer		not null,
						transfer_duration	integer		null
);
create table gtfs_fare_rules (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						fare_id			text		not null,
						route_id		text		null,
						origin_id		text		null,
						destination_id		text		null,
						contains_id		text		null
);
create table gtfs_shapes (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						shape_id		text		not null,	-- indexed --
						shape_pt_lat		numeric		not null,
						shape_pt_lon		numeric		not null,
						shape_pt_sequence	integer		not null,
						shape_dist_traveled	numeric		null
);
create table gtfs_frequencies (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						trip_id			text		null,
						start_time		varchar(8)	null,
						end_time		varchar(8)	null,
						headway_secs		integer		null,
						exact_times		integer		null
);
create table gtfs_transfers (			geo_gtfs_feed_id	integer		not null	foreign key references geo_gtfs_feed(id),
						from_stop_id		text		not null,
						to_stop_id		text		not null,
						transfer_type		integer		not null,
						min_transfer_time	integer		null
);
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

create unique index gtfs_agency__geo_gtfs_feed_id		on gtfs_agency		(geo_gtfs_feed_id);
create unique index gtfs_stops__geo_gtfs_feed_id		on gtfs_stops		(geo_gtfs_feed_id);
create unique index gtfs_routes__geo_gtfs_feed_id		on gtfs_routes		(geo_gtfs_feed_id);
create unique index gtfs_trips__geo_gtfs_feed_id		on gtfs_trips		(geo_gtfs_feed_id);
create unique index gtfs_stop__geo_gtfs_feed_id			on gtfs_stop		(geo_gtfs_feed_id);
create unique index gtfs_calendar__geo_gtfs_feed_id		on gtfs_calendar	(geo_gtfs_feed_id);
create unique index gtfs_calendar__geo_gtfs_feed_id		on gtfs_calendar	(geo_gtfs_feed_id);
create unique index gtfs_fare__geo_gtfs_feed_id			on gtfs_fare		(geo_gtfs_feed_id);
create unique index gtfs_fare__geo_gtfs_feed_id			on gtfs_fare		(geo_gtfs_feed_id);
create unique index gtfs_shapes__geo_gtfs_feed_id		on gtfs_shapes		(geo_gtfs_feed_id);
create unique index gtfs_frequencies__geo_gtfs_feed_id		on gtfs_frequencies	(geo_gtfs_feed_id);
create unique index gtfs_transfers__geo_gtfs_feed_id		on gtfs_transfers	(geo_gtfs_feed_id);
create unique index gtfs_feed__geo_gtfs_feed_id			on gtfs_feed		(geo_gtfs_feed_id);
