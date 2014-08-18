create table geo_gtfs__agency (		id			integer				autoincrement,
       	     		     		name			varchar(64)	not null	-- preferably the transit agency's domain name, without a www. prefix. - examples: 'ridetarc.org', 'ttc.ca'
);
create table geo_gtfs__feed (		id			integer				autoincrement,
       	     		   		agencyid		integer		not null	references geo_gtfs__agency(id),
       	     		   		url			text		not null,
					retrieved		integer		not null,
					last_modified		integer		null		-- SHOULD be specified, but some servers may omit timestamp from HTTP response.
);
create table geo_gtfs__note (		id			integer				autoincrement,
					agencyid		integer		not null	references geo_gtfs__agency(id),
					note			text		not null
);
-------------------------------------------------------------------------------
create table gtfs__agency (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					agency_id		text		null,		-- for feeds containing only one agency, this can be NULL.
       	     		  		agency_name		text		not null,
					agency_url		text		not null,
					agency_timezone		text		not null,
					agency_lang		varchar(2)	null,
					agency_phone		text		null,
					agency_fare_url		text		null
);
create table gtfs__stops (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					stop_id			text		not null,
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
create table gtfs__routes (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					route_id		text		not null,
					agency_id		text		null,
					route_short_name	text		not null,
					route_long_name		text		not null,
					route_desc		text		null,
					route_type		integer		not null,
					route_url		text		null,
					route_color		varchar(6)	null,
					route_text_color	varchar(6)	null
);
create table gtfs__trips (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					route_id		text		not null,
					service_id		text		not null,
					trip_id			text		not null,
					trip_headsign		text		null,
					trip_short_name		text		null,
					direction_id		integer		null,
					block_id		text		null,
					shape_id		text		null,
					wheelchair_accessible	integer		null,
					bikes_allowed		integer		null
);
create table gtfs__stop_times (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					trip_id			text		not null,
					arrival_time		varchar(8)	not null,
					departure_time		varchar(8)	not null,
					stop_id			text		not null,
					stop_sequence		integer		not null,
					stop_headsign		text		null,
					pickup_type		integer		null,
					drop_off_type		integer		null,
					shape_dist_traveled	numeric		null
);
create table gtfs__calendar (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					service_id		text		not null,
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
create table gtfs__calendar_dates (	geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					service_id		text		not null,
					`date`			varchar(8)	not null,
					exception_type		integer		not null
);
create table gtfs__fare_attributes (	geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					fare_id			text		not null,
					price			numeric		not null,
					currency_type		text		not null,
					payment_method		integer		not null,
					transfers		integer		not null,
					transfer_duration	integer		null
);
create table gtfs__fare_rules (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					fare_id			text		not null,
					route_id		text		null,
					origin_id		text		null,
					destination_id		text		null,
					contains_id		text		null
);
create table gtfs__shapes (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					shape_id		text		not null,
					shape_pt_lat		numeric		not null,
					shape_pt_lon		numeric		not null,
					shape_pt_sequence	integer		not null,
					shape_dist_traveled	numeric		null
);
create table gtfs__frequencies (	geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					trip_id			text		null,
					start_time		varchar(8)	null,
					end_time		varchar(8)	null,
					headway_secs		integer		null,
					exact_times		integer		null
);
create table gtfs__transfers (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					from_stop_id		text		not null,
					to_stop_id		text		not null,
					transfer_type		integer		not null,
					min_transfer_time	integer		null
);
create table gtfs__feed_info (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					feed_publisher_name	text		not null,
					feed_publisher_url	text		not null,
					feed_lang		text		not null,
					feed_start_date		varchar(8)	null,
					feed_end_date		varchar(8)	null,
					feed_version		text		null
);
-------------------------------------------------------------------------------

