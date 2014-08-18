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
					route_id		text		null
					service_id		text		null
					trip_id			text		null
					trip_headsign		text		null
					trip_short_name		text		null
					direction_id		text		null
					block_id		text		null
					shape_id		text		null
					wheelchair_accessible	text		null
					bikes_allowed		text		null
);
create table gtfs__stop_times (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					trip_id			text		null
					arrival_time		text		null
					departure_time		text		null
					stop_id			text		null
					stop_sequence		text		null
					stop_headsign		text		null
					pickup_type		text		null
					drop_off_type		text		null
					shape_dist_traveled	text		null
);
create table gtfs__calendar (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					service_id		text		null
					monday			text		null
					tuesday			text		null
					wednesday		text		null
					thursday		text		null
					friday			text		null
					saturday		text		null
					sunday			text		null
					start_date		text		null
					end_date		text		null
);
create table gtfs__calendar_dates (	geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					service_id		text		null
					`date`			text		null
					exception_type		text		null
);
create table gtfs__fare_attributes (	geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					fare_id			text		null
					price			text		null
					currency_type		text		null
					payment_method		text		null
					transfers		text		null
					transfer_duration	text		null
);
create table gtfs__fare_rules (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					fare_id			text		null
					route_id		text		null
					origin_id		text		null
					destination_id		text		null
					contains_id		text		null
);
create table gtfs__shapes (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					shape_id		text		null
					shape_pt_lat		text		null
					shape_pt_lon		text		null
					shape_pt_sequence	text		null
					shape_dist_traveled	text		null
);
create table gtfs__frequencies (	geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					trip_id			text		null
					start_time		text		null
					end_time		text		null
					headway_secs		text		null
					exact_times		text		null
);
create table gtfs__transfers (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					from_stop_id		text		null
					to_stop_id		text		null
					transfer_type		text		null
					min_transfer_time	text		null
);
create table gtfs__feed_info (		geo_gtfs__feed_id	integer		not null	references geo_gtfs__feed(id),
					feed_publisher_name	text		null
					feed_publisher_url	text		null
					feed_lang		text		null
					feed_start_date		text		null
					feed_end_date		text		null
					feed_version		text		null
);
-------------------------------------------------------------------------------
create table gtfs_realtime__
