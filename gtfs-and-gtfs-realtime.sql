create table geo_gtfs__agency (		id			integer	autoincrement,
       	     		     		name			varchar(64) not null			-- preferably the transit agency's domain name, without a www. prefix. - examples: 'ridetarc.org', 'ttc.ca'
);
create table geo_gtfs__feed (		id			integer autoincrement,
       	     		   		agencyid		integer not null references geo_gtfs__agency(id),
       	     		   		url			text not null,
					retrieved		integer not null,
					timestamp		integer null				-- SHOULD be specified, but some servers may omit timestamp from HTTP response.
);
create table geo_gtfs__note (		id			integer autoincrement,
					agencyid		integer not null references geo_gtfs__agency(id),
					note			text not null
);
-------------------------------------------------------------------------------
create table gtfs__agency (		geo_gtfs__feed_id	integer not null references geo_gtfs__feed(id),
					agency_id		text null,
       	     		  		agency_name		text not null,
					agency_url		text not null,
					agency_timezone		text not null,
					agency_lang		text null,
					agency_phone		text null,
					agency_fare_url		text null
);
create table gtfs__stops (		geo_gtfs__feed_id	integer not null references geo_gtfs__feed(id),
					stop_id			text not null,
       	     		 		stop_code		text null,
					stop_name		text not null,
					stop_desc		text null,
					stop_lat		numeric not null,
					stop_lon		numeric not null,
					zone_id			text null,
					stop_url		text null,
					location_type		integer null,
					parent_station		text null,
					stop_timezone		text null,
					wheelchair_boarding	text null
);
create table gtfs__routes (		geo_gtfs__feed_id	integer not null references geo_gtfs__feed(id),
					route_id		text not null,
					agency_id		text null,
					route_short_name	text not null,
					route_long_name		text not null,
					route_desc		text null,
					route_type		integer not null,
					route_url		text null,
					route_color		text null,
					route_text_color	text null
);
create table gtfs__trips (		geo_gtfs__feed_id	integer not null references geo_gtfs__feed(id),
					route_id
					service_id
					trip_id
					trip_headsign
					trip_short_name
					direction_id
					block_id
					shape_id
					wheelchair_accessible
					bikes_allowed
);
create table gtfs__stop_times (		geo_gtfs__feed_id	integer not null references geo_gtfs__feed(id),
					trip_id
					arrival_time
					departure_time
					stop_id
					stop_sequence
					stop_headsign
					pickup_type
					drop_off_type
					shape_dist_traveled
);
create table gtfs__calendar (		geo_gtfs__feed_id	integer not null references geo_gtfs__feed(id),
					service_id
					monday
					tuesday
					wednesday
					thursday
					friday
					saturday
					sunday
					start_date
					end_date
);
create table gtfs__calendar_dates (	geo_gtfs__feed_id	integer not null references geo_gtfs__feed(id),
					service_id
					date
					exception_type
);
create table gtfs__fare_attributes (	geo_gtfs__feed_id	integer not null references geo_gtfs__feed(id),
					fare_id
					price
					currency_type
					payment_method
					transfers
					transfer_duration
);
create table gtfs__fare_rules (		geo_gtfs__feed_id	integer not null references geo_gtfs__feed(id),
					fare_id
					route_id
					origin_id
					destination_id
					contains_id
);
create table gtfs__shapes (		geo_gtfs__feed_id	integer not null references geo_gtfs__feed(id),
					shape_id
					shape_pt_lat
					shape_pt_lon
					shape_pt_sequence
					shape_dist_traveled
);
create table gtfs__frequencies (	geo_gtfs__feed_id	integer not null references geo_gtfs__feed(id),
					trip_id
					start_time
					end_time
					headway_secs
					exact_times
);
create table gtfs__transfers (		geo_gtfs__feed_id	integer not null references geo_gtfs__feed(id),
					from_stop_id
					to_stop_id
					transfer_type
					min_transfer_time
);
create table gtfs__feed_info (		geo_gtfs__feed_id	integer not null references geo_gtfs__feed(id),
					feed_publisher_name
					feed_publisher_url
					feed_lang
					feed_start_date
					feed_end_date
					feed_version
);
-------------------------------------------------------------------------------
create table gtfs_realtime__
