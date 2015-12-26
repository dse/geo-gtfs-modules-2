package Geo::GTFS2::DB;
use warnings;
use strict;

use DBI;
use File::Basename qw(dirname basename);
use File::Path qw(make_path);
use HTTP::Date qw(str2time);
use POSIX qw(strftime floor uname);

use fields qw(dir
	      sqlite_filename
	      dbh);

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

sub dbh {
    my ($self) = @_;
    if ($self->{dbh}) {
	return $self->{dbh};
    }
    my $dbfile = $self->{sqlite_filename};
    make_path(dirname($dbfile));
    $self->{dbh} = DBI->connect("dbi:SQLite:$dbfile", "", "",
				{ RaiseError => 1, AutoCommit => 0 });
    $self->create_tables();
    return $self->{dbh};
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
    my $dbh = $self->{dbh};
    print STDERR ("Dropping database tables...\n");
    $self->execute_multiple_sql_queries(<<"END");
drop table if exists geo_gtfs;
drop table if exists geo_gtfs_agency;
drop table if exists geo_gtfs_feed;
drop table if exists geo_gtfs_feed_instance;
drop table if exists geo_gtfs_realtime_feed;
drop table if exists geo_gtfs_realtime_feed_instance;
drop table if exists gtfs_agency;
drop table if exists gtfs_stops;
drop table if exists gtfs_routes;
drop table if exists gtfs_trips;
drop table if exists gtfs_stop_times;
drop table if exists gtfs_calendar;
drop table if exists gtfs_calendar_dates;
drop table if exists gtfs_fare_attributes;
drop table if exists gtfs_fare_rules;
drop table if exists gtfs_shapes;
drop table if exists gtfs_frequencies;
drop table if exists gtfs_transfers;
drop table if exists gtfs_feed_info;
END
}

sub create_tables {
    my ($self) = @_;
    my $dbh = $self->dbh;

    my $sql = <<"END";
create table if not exists geo_gtfs (
                                                        name                            varchar(32)     not null        primary key,
                                                        value                           text            null
);
delete from geo_gtfs where name = 'geo_gtfs.db.version';
insert into geo_gtfs (name, value) values('geo_gtfs.db.version', '0.1');

create table if not exists geo_gtfs_agency (
                                                        id                              integer                         primary key autoincrement,
                                                        name                            varchar(64)     not null        -- preferably the transit agency's domain name, without a www. prefix. - examples: 'ridetarc.org', 'ttc.ca'
);
create index if not exists geo_gtfs_agency_01 on geo_gtfs_agency(name);

create table if not exists geo_gtfs_feed (
                                                        id                              integer                         primary key autoincrement,
                                                        geo_gtfs_agency_id              integer         not null        references geo_gtfs_agency(id),
                                                        url                             text            not null,
                                                        is_active                       integer         not null        default 1       -- updated when feeds added, removed, I guess.
);
create index if not exists geo_gtfs_feed_01 on geo_gtfs_feed(is_active);

create table if not exists geo_gtfs_feed_instance (
                                                        id                              integer                         primary key autoincrement,
                                                        geo_gtfs_feed_id                integer         not null        references geo_gtfs_feed(id),
                                                        filename                        text            not null,
                                                        retrieved                       integer         not null,
                                                        last_modified                   integer         null,           -- SHOULD be specified, but some servers omit.
                                                        is_latest                       integer         not null        default 1
);
create index if not exists geo_gtfs_feed_instance_01 on geo_gtfs_feed_instance(is_latest);

create table if not exists geo_gtfs_realtime_feed (
                                                        id                              integer                         primary key,
                                                        geo_gtfs_agency_id              integer         not null        references geo_gtfs_agency(id),
                                                        url                             text            not null,
                                                        feed_type                       varchar(16)     not null,       -- 'updates', 'positions', 'alerts', 'all'
                                                        is_active                       integer         not null        default 1       -- updated when feeds added, removed
);
create index if not exists geo_gtfs_realtime_feed_01 on geo_gtfs_realtime_feed(feed_type);
create index if not exists geo_gtfs_realtime_feed_02 on geo_gtfs_realtime_feed(is_active);

create table if not exists geo_gtfs_realtime_feed_instance (
                                                        id                              integer                         primary key,
                                                        geo_gtfs_realtime_feed_id       integer         not null        references geo_gtfs_realtime_feed(id),
                                                        filename                        text            not null,
                                                        retrieved                       integer         not null,
                                                        last_modified                   integer         null,
                                                        header_timestamp                integer         null,
                                                        is_latest                       integer         not null        default 1
);
create index if not exists geo_gtfs_realtime_feed_instance_01 on geo_gtfs_realtime_feed_instance(is_latest);
-------------------------------------------------------------------------------
create table if not exists gtfs_agency (
                                                        geo_gtfs_feed_instance_id       integer         not null        references geo_gtfs_feed(id),
                                                        agency_id                       text            null,           -- indexed -- for feeds containing only one agency, this can be NULL.
                                                        agency_name                     text            not null,
                                                        agency_url                      text            not null,
                                                        agency_timezone                 text            not null,
                                                        agency_lang                     varchar(2)      null,
                                                        agency_phone                    text            null,
                                                        agency_fare_url                 text            null
);
create unique index if not exists gtfs_agency_01 on gtfs_agency(geo_gtfs_feed_instance_id, agency_id);

create table if not exists gtfs_stops (
                                                        geo_gtfs_feed_instance_id       integer         not null        references geo_gtfs_feed(id),
                                                        stop_id                         text            not null,       -- indexed --
                                                        stop_code                       text            null,
                                                        stop_name                       text            not null,
                                                        stop_desc                       text            null,
                                                        stop_lat                        numeric         not null,
                                                        stop_lon                        numeric         not null,
                                                        zone_id                         text            null,           -- indexed --
                                                        stop_url                        text            null,
                                                        location_type                   integer         null,
                                                        parent_station                  text            null,
                                                        stop_timezone                   text            null,
                                                        wheelchair_boarding             integer         null
);
create unique index if not exists gtfs_stops_01 on gtfs_stops(geo_gtfs_feed_instance_id, stop_id);
create        index if not exists gtfs_stops_02 on gtfs_stops(geo_gtfs_feed_instance_id, zone_id);
create        index if not exists gtfs_stops_03 on gtfs_stops(geo_gtfs_feed_instance_id, location_type);
create        index if not exists gtfs_stops_04 on gtfs_stops(geo_gtfs_feed_instance_id, parent_station);
create        index if not exists gtfs_stops_05 on gtfs_stops(geo_gtfs_feed_instance_id, wheelchair_boarding);

create table if not exists gtfs_routes (
                                                        geo_gtfs_feed_instance_id       integer         not null        references geo_gtfs_feed(id),
                                                        route_id                        text            not null,       -- indexed --
                                                        agency_id                       text            null            references gtfs_agency(id),
                                                        route_short_name                text            not null,
                                                        route_long_name                 text            not null,
                                                        route_desc                      text            null,
                                                        route_type                      integer         not null,
                                                        route_url                       text            null,
                                                        route_color                     varchar(6)      null,
                                                        route_text_color                varchar(6)      null
);
create unique index if not exists gtfs_routes_01 on gtfs_routes (geo_gtfs_feed_instance_id, route_id, agency_id);
create        index if not exists gtfs_routes_02 on gtfs_routes (geo_gtfs_feed_instance_id, agency_id);
create        index if not exists gtfs_routes_03 on gtfs_routes (geo_gtfs_feed_instance_id, route_id);
create        index if not exists gtfs_routes_04 on gtfs_routes (geo_gtfs_feed_instance_id, route_type);

create table if not exists gtfs_trips (
                                                        geo_gtfs_feed_instance_id       integer         not null        references geo_gtfs_feed(id),
                                                        route_id                        text            not null        references gtfs_routes(id),
                                                        service_id                      text            not null,       -- indexed --
                                                        trip_id                         text            not null,       -- indexed --
                                                        trip_headsign                   text            null,
                                                        trip_short_name                 text            null,
                                                        direction_id                    integer         null,           -- indexed --
                                                        block_id                        text            null,           -- indexed --
                                                        shape_id                        text            null            references gtfs_shapes(id),
                                                        wheelchair_accessible           integer         null,
                                                        bikes_allowed                   integer         null
);
create unique index if not exists gtfs_trips_01 on gtfs_trips (geo_gtfs_feed_instance_id, trip_id);
create        index if not exists gtfs_trips_02 on gtfs_trips (geo_gtfs_feed_instance_id, route_id);
create        index if not exists gtfs_trips_03 on gtfs_trips (geo_gtfs_feed_instance_id, service_id);
create        index if not exists gtfs_trips_04 on gtfs_trips (geo_gtfs_feed_instance_id, direction_id);
create        index if not exists gtfs_trips_05 on gtfs_trips (geo_gtfs_feed_instance_id, block_id);
create        index if not exists gtfs_trips_06 on gtfs_trips (geo_gtfs_feed_instance_id, shape_id);

create table if not exists gtfs_stop_times (
                                                        geo_gtfs_feed_instance_id       integer         not null        references geo_gtfs_feed(id),
                                                        trip_id                         text            not null        references gtfs_trips(id),
                                                        arrival_time                    varchar(8)      not null,
                                                        departure_time                  varchar(8)      not null,
                                                        stop_id                         text            not null        references gtfs_stops(id),
                                                        stop_sequence                   integer         not null,
                                                        stop_headsign                   text            null,
                                                        pickup_type                     integer         null,
                                                        drop_off_type                   integer         null,
                                                        shape_dist_traveled             numeric         null
);
create        index if not exists gtfs_stop_times_01 on gtfs_stop_times (geo_gtfs_feed_instance_id, stop_id);
create        index if not exists gtfs_stop_times_02 on gtfs_stop_times (geo_gtfs_feed_instance_id, trip_id);
create        index if not exists gtfs_stop_times_03 on gtfs_stop_times (geo_gtfs_feed_instance_id, stop_sequence);
create unique index if not exists gtfs_stop_times_01 on gtfs_stop_times (geo_gtfs_feed_instance_id, trip_id, stop_id);

create table if not exists gtfs_calendar (
                                                        geo_gtfs_feed_instance_id       integer         not null        references geo_gtfs_feed(id),
                                                        service_id                      text            not null,       -- indexed --
                                                        monday                          integer         not null,
                                                        tuesday                         integer         not null,
                                                        wednesday                       integer         not null,
                                                        thursday                        integer         not null,
                                                        friday                          integer         not null,
                                                        saturday                        integer         not null,
                                                        sunday                          integer         not null,
                                                        start_date                      varchar(8)      not null,
                                                        end_date                        varchar(8)      not null
);
create        index if not exists gtfs_calendar_01 on gtfs_calendar(geo_gtfs_feed_instance_id, service_id);
create        index if not exists gtfs_calendar_02 on gtfs_calendar(geo_gtfs_feed_instance_id, monday);
create        index if not exists gtfs_calendar_03 on gtfs_calendar(geo_gtfs_feed_instance_id, tuesday);
create        index if not exists gtfs_calendar_04 on gtfs_calendar(geo_gtfs_feed_instance_id, wednesday);
create        index if not exists gtfs_calendar_05 on gtfs_calendar(geo_gtfs_feed_instance_id, thursday);
create        index if not exists gtfs_calendar_06 on gtfs_calendar(geo_gtfs_feed_instance_id, friday);
create        index if not exists gtfs_calendar_07 on gtfs_calendar(geo_gtfs_feed_instance_id, saturday);
create        index if not exists gtfs_calendar_08 on gtfs_calendar(geo_gtfs_feed_instance_id, sunday);
create        index if not exists gtfs_calendar_09 on gtfs_calendar(geo_gtfs_feed_instance_id, start_date);
create        index if not exists gtfs_calendar_10 on gtfs_calendar(geo_gtfs_feed_instance_id, end_date);

create table if not exists gtfs_calendar_dates (
                                                        geo_gtfs_feed_instance_id       integer         not null        references geo_gtfs_feed(id),
                                                        service_id                      text            not null,       -- indexed --
                                                        `date`                          varchar(8)      not null,
                                                        exception_type                  integer         not null
);
create        index if not exists gtfs_calendar_dates_01 on gtfs_calendar_dates(geo_gtfs_feed_instance_id, service_id);
create        index if not exists gtfs_calendar_dates_02 on gtfs_calendar_dates(geo_gtfs_feed_instance_id, `date`);
create        index if not exists gtfs_calendar_dates_03 on gtfs_calendar_dates(geo_gtfs_feed_instance_id, exception_type);

create table if not exists gtfs_fare_attributes (
                                                        geo_gtfs_feed_instance_id       integer         not null        references geo_gtfs_feed(id),
                                                        fare_id                         text            not null,       -- indexed --
                                                        price                           numeric         not null,
                                                        currency_type                   text            not null,
                                                        payment_method                  integer         not null,
                                                        transfers                       integer         not null,
                                                        transfer_duration               integer         null
);
create        index if not exists gtfs_fare_attributes_01 on gtfs_fare_attributes(geo_gtfs_feed_instance_id, fare_id);

create table if not exists gtfs_fare_rules (
                                                        geo_gtfs_feed_instance_id       integer         not null        references geo_gtfs_feed(id),
                                                        fare_id                         text            not null        references gtfs_fare_attributes(fare_id),
                                                        route_id                        text            null            references gtfs_routes(id),
                                                        origin_id                       text            null,           -- indexed --
                                                        destination_id                  text            null,           -- indexed --
                                                        contains_id                     text            null            -- indexed --
);
create        index if not exists gtfs_fare_rules_01 on gtfs_fare_rules(geo_gtfs_feed_instance_id, fare_id);
create        index if not exists gtfs_fare_rules_02 on gtfs_fare_rules(geo_gtfs_feed_instance_id, route_id);
create        index if not exists gtfs_fare_rules_03 on gtfs_fare_rules(geo_gtfs_feed_instance_id, origin_id);
create        index if not exists gtfs_fare_rules_04 on gtfs_fare_rules(geo_gtfs_feed_instance_id, destination_id);
create        index if not exists gtfs_fare_rules_05 on gtfs_fare_rules(geo_gtfs_feed_instance_id, contains_id);

create table if not exists gtfs_shapes (
                                                        geo_gtfs_feed_instance_id       integer         not null        references geo_gtfs_feed(id),
                                                        shape_id                        text            not null,       -- indexed --
                                                        shape_pt_lat                    numeric         not null,
                                                        shape_pt_lon                    numeric         not null,
                                                        shape_pt_sequence               integer         not null,       -- indexed --
                                                        shape_dist_traveled             numeric         null
);
create        index if not exists gtfs_shapes_01 on gtfs_shapes(geo_gtfs_feed_instance_id, shape_id);
create        index if not exists gtfs_shapes_02 on gtfs_shapes(geo_gtfs_feed_instance_id, shape_id, shape_pt_sequence);

create table if not exists gtfs_frequencies (
                                                        geo_gtfs_feed_instance_id       integer         not null        references geo_gtfs_feed(id),
                                                        trip_id                         text            null            references gtfs_trips(id),
                                                        start_time                      varchar(8)      null, --indexed
                                                        end_time                        varchar(8)      null, --indexed
                                                        headway_secs                    integer         null,
                                                        exact_times                     integer         null
);
create        index if not exists gtfs_frequencies_01 on gtfs_frequencies(geo_gtfs_feed_instance_id, trip_id);
create        index if not exists gtfs_frequencies_02 on gtfs_frequencies(geo_gtfs_feed_instance_id, start_time);
create        index if not exists gtfs_frequencies_03 on gtfs_frequencies(geo_gtfs_feed_instance_id, end_time);

create table if not exists gtfs_transfers (
                                                        geo_gtfs_feed_instance_id       integer         not null        references geo_gtfs_feed(id),
                                                        from_stop_id                    text            not null        references gtfs_stops(id),
                                                        to_stop_id                      text            not null        references gtfs_stops(id),
                                                        transfer_type                   integer         not null,
                                                        min_transfer_time               integer         null
);
create        index if not exists gtfs_transfers_01 on gtfs_transfers(from_stop_id);
create        index if not exists gtfs_transfers_02 on gtfs_transfers(to_stop_id);

create table if not exists gtfs_feed_info (
                                                        geo_gtfs_feed_instance_id       integer         not null        references geo_gtfs_feed(id),
                                                        feed_publisher_name             text            not null,
                                                        feed_publisher_url              text            not null,
                                                        feed_lang                       text            not null,
                                                        feed_start_date                 varchar(8)      null,
                                                        feed_end_date                   varchar(8)      null,
                                                        feed_version                    text            null
);

create index if not exists geo_gtfs_agency_00                   on gtfs_agency          (geo_gtfs_feed_instance_id);
create index if not exists geo_gtfs_stops_00                    on gtfs_stops           (geo_gtfs_feed_instance_id);
create index if not exists geo_gtfs_routes_00                   on gtfs_routes          (geo_gtfs_feed_instance_id);
create index if not exists geo_gtfs_trips_00                    on gtfs_trips           (geo_gtfs_feed_instance_id);
create index if not exists geo_gtfs_stop_times_00               on gtfs_stop_times      (geo_gtfs_feed_instance_id);
create index if not exists geo_gtfs_calendar_00                 on gtfs_calendar        (geo_gtfs_feed_instance_id);
create index if not exists geo_gtfs_calendar_dates_00           on gtfs_calendar_dates  (geo_gtfs_feed_instance_id);
create index if not exists geo_gtfs_fare_attributes_00          on gtfs_fare_attributes (geo_gtfs_feed_instance_id);
create index if not exists geo_gtfs_fare_rules_00               on gtfs_fare_rules      (geo_gtfs_feed_instance_id);
create index if not exists geo_gtfs_shapes_00                   on gtfs_shapes          (geo_gtfs_feed_instance_id);
create index if not exists geo_gtfs_frequencies_00              on gtfs_frequencies     (geo_gtfs_feed_instance_id);
create index if not exists geo_gtfs_transfers_00                on gtfs_transfers       (geo_gtfs_feed_instance_id);
create index if not exists geo_gtfs_feed_info_00                on gtfs_feed_info       (geo_gtfs_feed_instance_id);
END
    $self->execute_multiple_sql_queries($sql);
}

sub execute_multiple_sql_queries {
    my ($self, $sql) = @_;
    $sql =~ s{--.*?$}{}gsm;
    my @sql = split(qr{;$}m, $sql);
    foreach my $sql (@sql) {
	next unless $sql =~ m{\S};
	my $short = $sql;
	$short =~ s{\s+}{ }gsm;
	$short =~ s{\(.*}{};
	eval { $self->dbh->do($sql); };
	if ($@) {
	    my $error = $@;
	    die($error);
	}
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
    $sth1->execute($service_id_xml, $geo_gtfs_feed_instance_id);
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
    return $geo_gtfs_agency_name if $geo_gtfs_agency_name =~ m{^\d+$};
    return $self->select_or_insert_id("table_name" => "geo_gtfs_agency",
				      "id_name" => "id",
				      "key_fields" => { "name" => $geo_gtfs_agency_name });
}

###############################################################################
# MISC.
###############################################################################

sub DESTROY {
    my ($self) = @_;
    my $dbh = $self->{dbh};
    if ($dbh) {
	$dbh->rollback();
    }
    # STFU: Issuing rollback() due to DESTROY without explicit disconnect() of DBD::SQLite::db handle /Users/dse/.geo-gtfs2/google_transit.sqlite.
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



=cut

1;
