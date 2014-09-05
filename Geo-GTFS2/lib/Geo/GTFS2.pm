package Geo::GTFS2;
use strict;
use warnings;

BEGIN {
    foreach my $dir ("/home/dse/git/HTTP-Cache-Transparent/lib",
		     "/Users/dse/git/HTTP-Cache-Transparent/lib",
		     "/home/dse/git/geo-gtfs-modules-2/Geo-GTFS2/lib",
		     "/Users/dse/git/geo-gtfs-modules-2/Geo-GTFS2/lib",
		    ) {
	unshift(@INC, $dir) if -d $dir;
    }
    # my fork adds a special feature called NoUpdateImpatient.
}

use DBI;
use Data::Dumper;
use File::Basename qw(dirname basename);
use File::MMagic;		# File::MMagic best detects .zip
                                # files, and allows us to add magic
                                # for Google Protocol Buffers files.
use File::Path qw(make_path);
use File::Spec;
use Google::ProtocolBuffers;
use HTTP::Cache::Transparent;
use HTTP::Date qw(str2time);
use JSON qw(-convert_blessed_universally);
use LWP::UserAgent;
use List::MoreUtils qw(all);
use POSIX qw(strftime floor uname);
use Text::CSV;

use fields qw(dir http_cache_dir
	      sqlite_filename
	      dbh db
	      ua
	      magic
	      json

	      gtfs_realtime_proto
	      gtfs_realtime_protocol_pulled

	      vehicle_positions
	      vehicle_positions_by_trip_id
	      vehicle_positions_array
	      trip_updates
	      trip_updates_array
	      realtime_feed
	      header
	      headers

	      geo_gtfs_agency_name
	      geo_gtfs_agency_id
	      geo_gtfs_feed_id
	      geo_gtfs_realtime_feed_id
	      geo_gtfs_realtime_feed_instance_id);

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
    $self->{http_cache_dir} //= "$dir/http-cache";
    my $dbfile = $self->{sqlite_filename} //= "$dir/google_transit.sqlite";
    $self->{magic} = File::MMagic->new();
    $self->{magic}->addMagicEntry("0\tstring\t\\x0a\\x0b\\x0a\\x03\tapplication/x-protobuf");
    $self->{gtfs_realtime_proto} = "https://developers.google.com/transit/gtfs-realtime/gtfs-realtime.proto";
    $self->{gtfs_realtime_protocol_pulled} = 0;
}

###############################################################################
# GENERAL
###############################################################################

sub set_agency {
    my ($self, $agency_name) = @_;

    $self->{geo_gtfs_agency_name} = $agency_name;
    $self->{geo_gtfs_agency_id} =
      $self->db->select_or_insert_geo_gtfs_agency_id($agency_name);
}

sub process_url {
    my ($self, $url) = @_;

    my $ua = $self->ua;
    if ($url =~ m{\.pb$}) {
	HTTP::Cache::Transparent::init({ BasePath => $self->{http_cache_dir},
					 Verbose => 0,
					 NoUpdate => 30,
					 NoUpdateImpatient => 1 });
	$ua->show_progress(1);
    } elsif ($url =~ m{\.zip$}) {
	HTTP::Cache::Transparent::init({ BasePath => $self->{http_cache_dir},
					 Verbose => 1 });
	$ua->show_progress(1);
    }
    my $request = HTTP::Request->new("GET", $url);
    my $response = $ua->request($request);
    $request->header("Date-Epoch", time());
    if (!$response->is_success) {
	warn(sprintf("%s => %s\n", $response->base, $response->status_line));
	return;
    }
    if ($response->content_type eq "application/x-zip-compressed") {
	return $self->process_gtfs_feed($request, $response);
    } elsif ($response->content_type eq "application/protobuf") {
	return $self->process_protocol_buffers($request, $response);
    } elsif ($response->base =~ m{\.zip$}i) {
	return $self->process_gtfs_feed($request, $response);
    } elsif ($response->base =~ m{\.pb$}i) {
	return $self->process_protocol_buffers($request, $response);
    } elsif ($url =~ m{\.zip$}i) {
	return $self->process_gtfs_feed($request, $response);
    } elsif ($url =~ m{\.pb$}i) {
	return $self->process_protocol_buffers($request, $response);
    } else {
	return $self->process_not_yet_known_content($request, $response);
    }
}

sub process_not_yet_known_content {
    my ($self, $request, $response) = @_;
    my $url = $response->base;
    my $cref = $response->content_ref;
    my $type = $self->{magic}->checktype_contents($$cref);
    if ($type eq "application/protobuf") {
	return $self->process_protocol_buffers($request, $response);
    } elsif ($type eq "application/x-zip-compressed") {
	return $self->process_gtfs_feed($request, $response);
    } else {
	warn("Sorry, but I do not recognize the content at:\n  $url\n");
	return;
    }
}

###############################################################################
# GTFS-REALTIME
###############################################################################

sub pull_gtfs_realtime_protocol {
    my ($self) = @_;
    return 1 if $self->{gtfs_realtime_protocol_pulled};
    HTTP::Cache::Transparent::init({ BasePath => $self->{http_cache_dir},
				     Verbose => 1,
				     NoUpdate => 86400,
				     UseCacheOnTimeout => 1,
				     NoUpdateImpatient => 0 });
    warn("    Pulling GTFS-realtime protocol...\n");
    my $request = HTTP::Request->new("GET", $self->{gtfs_realtime_proto});
    my $to = $self->ua->timeout();
    $self->ua->timeout(5);
    my $response = $self->ua->request($request);
    $self->ua->timeout($to);
    if (!$response->is_success()) {
	warn(sprintf("Failed to pull protocol: %s\n", $response->status_line()));
	exit(1);
    }
    my $proto = $response->content();
    if (!defined $proto) {
	die("Failed to pull protocol: undefined content\n");
    }
    if (!$proto) {
	die("Failed to pull protocol: no content\n");
    }
    warn("    Parsing...\n");
    Google::ProtocolBuffers->parse($proto);
    warn("    Done.\n");
    $self->{gtfs_realtime_protocol_pulled} = 1;
}

sub process_protocol_buffers {
    my ($self, $request, $response) = @_;
    $self->pull_gtfs_realtime_protocol();
    my $url = $response->base;
    my $cached = ($response->code == 304 || ($response->header("X-Cached") && $response->header("X-Content-Unchanged")));
    my $cref = $response->content_ref;

    my $feed_type;
    if ($url =~ m{/realtime/alerts/}i) {
	$feed_type = "alerts";
    } elsif ($url =~ m{/realtime/gtfs-realtime/}i) {
	$feed_type = "all";
    } elsif ($url =~ m{/realtime/trip_update/}i) {
	$feed_type = "updates";
    } elsif ($url =~ m{/realtime/vehicle/}i) {
	$feed_type = "positions";
    } else {
	die("Cannot determine GTFS-realtime feed type from URL:\n  $url\n");
    }

    my $retrieved      = $response->date // $response->last_modified // $request->header("Date-Epoch");
    my $last_modified  = $response->last_modified // -1;
    my $content_length = $response->content_length;

    my $o = TransitRealtime::FeedMessage->decode($$cref);
    my $header_timestamp = $o->{header}->{timestamp};
    my $base_filename = strftime("%Y/%m/%d/%H%M%SZ", gmtime($header_timestamp // $last_modified));
    my $pb_filename     = sprintf("%s/data/%s/pb/%s/%s.pb",     $self->{dir}, $self->{geo_gtfs_agency_name}, $feed_type, $base_filename);
    my $rel_pb_filename = sprintf(   "data/%s/pb/%s/%s.pb",                   $self->{geo_gtfs_agency_name}, $feed_type, $base_filename);
    my $json_filename   = sprintf("%s/data/%s/json/%s/%s.json", $self->{dir}, $self->{geo_gtfs_agency_name}, $feed_type, $base_filename);

    stat($pb_filename);
    if (!($cached && -e _ && defined $content_length && $content_length == (stat(_))[7])) {
	make_path(dirname($pb_filename));
	if (open(my $fh, ">", $pb_filename)) {
	    warn("Writing $pb_filename ...\n");
	    binmode($fh);
	    print {$fh} $$cref;
	    close($fh);
	} else {
	    die("Cannot write $pb_filename: $!\n");
	}
	make_path(dirname($json_filename));
	if (open(my $fh, ">", $json_filename)) {
	    warn("Writing $json_filename ...\n");
	    binmode($fh);
	    print {$fh} $self->json->encode($o);
	    close($fh);
	} else {
	    die("Cannot write $pb_filename: $!\n");
	}
	warn("Done.\n");
    }

    $self->{geo_gtfs_realtime_feed_id} =
      $self->db->select_or_insert_geo_gtfs_realtime_feed_id($self->{geo_gtfs_agency_id}, $url, $feed_type);

    $self->{geo_gtfs_realtime_feed_instance_id} =
      $self->db->select_or_insert_geo_gtfs_realtime_feed_instance_id($self->{geo_gtfs_realtime_feed_id},
								     $rel_pb_filename,
								     $retrieved,
								     $last_modified,
								     $header_timestamp);
}

sub update_realtime {
    my ($self) = @_;

    my @feeds = $self->db->get_geo_gtfs_realtime_feeds($self->{geo_gtfs_agency_id});
    foreach my $feed (@feeds) {
	my $url = $feed->{url};
	$self->process_url($url);
    }
}

sub list_latest_realtime_feeds {
    my ($self) = @_;

    my @instances = $self->db->get_latest_geo_gtfs_realtime_feed_instances($self->{geo_gtfs_agency_id});
    print("id      feed type  filename\n");
    print("------  ---------  -------------------------------------------------------------------------------\n");
    foreach my $i (@instances) {
	printf("%6d  %-9s  %s\n",
	       @{$i}{qw(id feed_type filename)});
    }
}

sub get_all_data {
    my ($self) = @_;

    my @instances = $self->db->get_latest_geo_gtfs_realtime_feed_instances($self->{geo_gtfs_agency_id});
    my %instances = map { ($_->{feed_type}, $_) } @instances;
    my $all = $instances{all};
    return unless $all;

    $self->{vehicle_positions} = {};
    $self->{vehicle_positions_by_trip_id} = {};
    $self->{trip_updates} = {};
    $self->{vehicle_positions_array} = [];
    $self->{trip_updates_array} = [];

    my $o = $self->{realtime_feed}->{all} = $self->read_realtime_feed($all->{filename});
    my $header_timestamp = eval { $o->{header}->{timestamp} };
    $self->{header} = $o->{header};
    $self->{headers}->{all} = $o->{header};
    foreach my $e (@{$o->{entity}}) {
	my $vp = $e->{vehicle};
	my $tu = $e->{trip_update};
	if ($vp) {
	    eval {
		$vp->{header_timestamp} = $header_timestamp if defined $header_timestamp;
		my $label   = eval { $vp->{vehicle}->{label} };
		my $trip_id = eval { $vp->{trip}->{trip_id} };

		if (defined $label || defined $trip_id) {
		    $self->{vehicle_positions}->{$label} = $vp if defined $label;
		    $self->{vehicle_positions_by_trip_id}->{$trip_id} = $vp if defined $trip_id;
		    $vp->{_consolidate_} = 1;
		}
		push(@{$self->{vehicle_positions_array}}, $vp);
	    }
	}
	if ($tu) {
	    eval {
		$tu->{header_timestamp} = $header_timestamp if defined $header_timestamp;
		$self->{trip_updates}->{$tu->{trip}->{trip_id}} = $tu;
		push(@{$self->{trip_updates_array}}, $tu);
	    }
	}
    }
    return $o;
}

sub process_010_flatten_vehicle_positions {
    my ($self) = @_;
    foreach my $vp (@{$self->{vehicle_positions_array}}) {
	eval { $vp->{trip_id}   = delete $vp->{trip}->{trip_id}       if defined $vp->{trip}->{trip_id};       };
	eval { $vp->{latitude}  = delete $vp->{position}->{latitude}  if defined $vp->{position}->{latitude};  };
	eval { $vp->{longitude} = delete $vp->{position}->{longitude} if defined $vp->{position}->{longitude}; };
	eval { $vp->{label}     = delete $vp->{vehicle}->{label}      if defined $vp->{vehicle}->{label};      };
	foreach my $k (qw(trip position vehicle)) {
	    delete $vp->{$k} if exists $vp->{$k} && !scalar(keys(%{$vp->{$k}}));
	}
    }
}

sub process_020_flatten_trip_updates {
    my ($self) = @_;
    foreach my $tu ($self->get_trip_update_list()) {
	eval { $tu->{trip_id}    = delete $tu->{trip}->{trip_id}    if defined $tu->{trip}->{trip_id};    };
	eval { $tu->{start_time} = delete $tu->{trip}->{start_time} if defined $tu->{trip}->{start_time}; };
	eval { $tu->{start_date} = delete $tu->{trip}->{start_date} if defined $tu->{trip}->{start_date}; };
	eval { $tu->{route_id}   = delete $tu->{trip}->{route_id}   if defined $tu->{trip}->{route_id};   };
	eval { $tu->{label}      = delete $tu->{vehicle}->{label}   if defined $tu->{vehicle}->{label};   };
	foreach my $stu (eval { @{$tu->{stop_time_update}} }) {
	    my $dep_time      = eval { delete $stu->{departure}->{time} };
	    my $arr_time      = eval { delete $stu->{arrival}->{time} };
	    my $dep_delay     = eval { delete $stu->{departure}->{delay} };
	    my $arr_delay     = eval { delete $stu->{arrival}->{delay} };
	    my $realtime_time = $arr_time // $dep_time;
	    my $delay         = $arr_delay // $dep_delay;
	    foreach my $k (qw(departure arrival)) {
		delete $stu->{$k} if exists $stu->{$k} && !scalar(keys(%{$stu->{$k}}));
	    }
	    $stu->{realtime_time} = $realtime_time if defined $realtime_time;
	    $stu->{delay}         = $delay         if defined $delay;
	}
	foreach my $k (qw(trip vehicle)) {
	    delete $tu->{$k} if !scalar(keys(%{$tu->{$k}}));
	}
    }
}

sub process_040_mark_as_of_times {
    my ($self) = @_;
    foreach my $tu ($self->get_trip_update_list()) {
	my $timestamp        = delete $tu->{timestamp};
	my $header_timestamp = delete $tu->{header_timestamp};
	$tu->{as_of} = $timestamp // $header_timestamp // time();
    }
}

sub get_trip_update_list {
    my ($self) = @_;
    return grep { !$_->{_exclude_} } @{$self->{trip_updates_array}};
}

sub process_030_consolidate_records {
    my ($self) = @_;
    foreach my $tu ($self->get_trip_update_list()) {
	my $label   = eval { $tu->{label} };
	my $trip_id = eval { $tu->{trip_id} };

	my $vp_by_label   = defined $label   && $self->{vehicle_positions}->{$label};
	my $vp_by_trip_id = defined $trip_id && $self->{vehicle_positions_by_trip_id}->{$trip_id};

	if ($vp_by_label && $vp_by_trip_id && $vp_by_label ne $vp_by_trip_id) {
	    next;
	}
	my $vp = $vp_by_label // $vp_by_trip_id;
	$vp->{_consolidate_} = 1;

	foreach my $key (grep { $_ ne "_consolidate_" } keys(%$vp)) {
	    if (!exists $tu->{$key}) {
		$tu->{$key} = delete $vp->{$key};
	    } elsif ($tu->{$key} eq $vp->{$key}) {
		delete $vp->{$key};
	    }
	}
	delete $self->{vehicle_positions}->{$label} if defined $label;
	delete $self->{vehicle_positions_by_trip_id}->{$trip_id} if defined $trip_id;

	$vp->{_delete_} = !scalar(grep { $_ ne "_consolidate_" } keys(%$vp));
    }

    my $vp_array = $self->{vehicle_positions_array};
    @$vp_array = grep { !$_->{_delete_} } @$vp_array;

    my $entity_array_ref = $self->{realtime_feed}->{all}->{entity};
    @$entity_array_ref = grep { !$_->{vehicle} || !$_->{vehicle}->{_delete_} } @$entity_array_ref;
}

sub get_sorted_trip_updates {
    my ($self) = @_;
    my @tu = $self->get_trip_update_list();
    @tu = map { [ $_, $_->{route_id} // 0, $_->{direction_id} // 0 ] } @tu;
    @tu = sort { _route_id_cmp($a->[1], $b->[1]) || $a->[2] <=> $b->[2] } @tu;
    @tu = map { $_->[0] } @tu;
    return @tu;
}

use constant TIMESTAMP_CUTOFF_AGE => 3600;

sub process_050_remove_turds {
    my ($self) = @_;

    foreach my $tu ($self->get_trip_update_list()) {
	my $label    = eval { $tu->{label} };
	my $trip_id  = eval { $tu->{trip_id} };
	my $route_id = eval { $tu->{route_id} };
	my $start_date = eval { $tu->{start_date} };
	if (!defined $label) {
	    $tu->{_exclude_} = "no vehicle label";
	    next;
	}
	if (!defined $trip_id) {
	    $tu->{_exclude_} = "no trip_id";
	    next;
	}
	if (!defined $route_id) {
	    $tu->{_exclude_} = "no route_id";
	    next;
	}
	if (!defined $start_date) {
	    $tu->{_exclude_} = "no start_date";
	    next;
	}
	if ($tu->{as_of} < time() - TIMESTAMP_CUTOFF_AGE) {
	    $tu->{_exclude_} = "old";
	    next;
	}
    }
}

sub process_060_populate_trip_and_route_info {
    my ($self) = @_;

    foreach my $tu ($self->get_trip_update_list()) {
	my $label    = eval { $tu->{label} };
	my $trip_id  = eval { $tu->{trip_id} };
	my $route_id = eval { $tu->{route_id} };
	my $start_date = eval { $tu->{start_date} };

	my ($geo_gtfs_feed_instance_id, $service_id) = $self->db->get_geo_gtfs_feed_instance_id_and_service_id($self->{geo_gtfs_agency_id}, $tu->{start_date});
	my $trip = $self->db->get_gtfs_trip($geo_gtfs_feed_instance_id, $trip_id);
	if (!$trip) {
	    $tu->{_exclude_} = "no trip record";
	    next;
	}

	foreach my $key (qw(trip_headsign direction_id)) {
	    if (!defined $tu->{$key}) {
		$tu->{$key} = delete $trip->{$key};
	    } elsif ($tu->{$key} eq $trip->{$key}) {
		delete $trip->{$key};
	    }
	}

	my $route = $self->db->get_gtfs_route($geo_gtfs_feed_instance_id, $route_id);
	if (defined $route) {
	    foreach my $key (qw(route_color
				route_desc
				route_long_name
				route_short_name
				route_text_color
				route_type
				route_url)) {
		if (!defined $tu->{$key}) {
		    $tu->{$key} = $route->{$key};
		} elsif ($tu->{$key} eq $route->{$key}) {
		    $tu->{$key} = $route->{$key};
		}
	    }
	} else {
	    $tu->{_exclude_} = "no route for $geo_gtfs_feed_instance_id $route_id\n";
	    next;
	}
    }
}

sub process_070_mark_next_coming_stops {
    my ($self) = @_;
    foreach my $tu ($self->get_trip_update_list()) {
	my $current_time = $tu->{header_timestamp} // $tu->{timestamp} // time();
	next unless defined $current_time;
	my $stu_index = -1;
	foreach my $stu (@{$tu->{stop_time_update}}) {
	    ++$stu_index;
	    my $stu_time = $stu->{realtime_time};
	    if (defined $stu_time && $current_time <= $stu_time) {
		$tu->{next_stop_time_update_index} = $stu_index;
		$stu->{is_next_stop_time_update} = 1;
		last;
	    }
	}
    }
}

sub process_080_remove_most_stop_time_update_data {
    my ($self) = @_;
    foreach my $tu ($self->get_trip_update_list()) {
	my $stu_array = $tu->{stop_time_update};
	next unless $stu_array && scalar(@$stu_array);
	my $idx = delete $tu->{next_stop_time_update_index};
	if (defined $idx) {
	    my $ns = $tu->{stop_time_update}->[$idx];
	    if ($ns) {
		delete $tu->{stop_time_update};
		$tu->{next_stop} = $ns;
		delete $ns->{is_next_stop_time_update};
	    }
	} else {
	    delete $tu->{stop_time_update};
	}
	delete $tu->{next_stop_time_update_index};
    }
}

sub process_090_populate_stop_time_update {
    my ($self, $tu, $stu) = @_;

    my $stop_id = $stu->{stop_id};
    if (!defined $stop_id || $stop_id eq "UN") {
	$stu->{_exclude_} = "no stop_id";
	return;
    }

    my ($geo_gtfs_feed_instance_id, $service_id) = $self->db->get_geo_gtfs_feed_instance_id_and_service_id($self->{geo_gtfs_agency_id}, $tu->{start_date});
    if (!defined $geo_gtfs_feed_instance_id) {
	$stu->{_exclude_} = "no geo_gtfs_feed_instance_id";
	return;
    }

    my $stop = $self->db->get_gtfs_stop($geo_gtfs_feed_instance_id, $stop_id);
    if (!$stop) {
	$stu->{_exclude_} = sprintf("no stop info for %s, %s\n", $geo_gtfs_feed_instance_id, $stop_id);
	return;
    }

    foreach my $key (qw(stop_code stop_name stop_desc stop_lat stop_lon
			zone_id stop_url location_type parent_station
			stop_timezone wheelchair_boarding)) {
	if (!defined $stu->{$key}) {
	    $stu->{$key} = $stop->{$key};
	} elsif ($stu->{$key} eq $stop->{$key}) {
	    $stu->{$key} = $stop->{$key};
	}
    }

    my $trip_id = $tu->{trip_id};
    if (!defined $trip_id) {
	$stu->{_exclude_} = "no trip_id";
	return;
    }

    my $stop_time_record = $self->db->get_gtfs_stop_time($geo_gtfs_feed_instance_id,
							 $stop_id, $trip_id);
    if ($stop_time_record) {
	my $scheduled_arrival_time   = $stop_time_record->{arrival_time};
	my $scheduled_departure_time = $stop_time_record->{departure_time};
	my $scheduled_time           = $scheduled_arrival_time // $scheduled_departure_time;
	my $sequence                 = $stop_time_record->{stop_sequence};
	$stu->{scheduled_time}   = $scheduled_time if defined $scheduled_time;
	$stu->{stop_time_record} = $stop_time_record;
	$stu->{stop_sequence}    = $sequence if defined $sequence;
    } else {
	warn("no stop_time_record for $geo_gtfs_feed_instance_id, $stop_id, $trip_id\n");
    }
}

sub process_090_populate_stop_information {
    my ($self) = @_;
    foreach my $tu ($self->get_trip_update_list()) {
	my $ns = $tu->{next_stop};
	if ($ns) {
	    $self->process_090_populate_stop_time_update($tu, $ns);
	}
	my $stu_array = $tu->{stop_time_update};
	if ($stu_array && scalar(@$stu_array)) {
	    foreach my $stu (@$stu_array) {
		$self->process_090_populate_stop_time_update($tu, $stu);
	    }
	}
    }
}

sub get_realtime_status_data {
    my ($self, %args) = @_;
    my $gtfs2 = $self;
    my $stage = $args{stage} // 9999;
    if (!$args{nofetch}) {
	$gtfs2->update_realtime();
    }

    my $o = $gtfs2->get_all_data();
    if ($stage < 1) {
	return $o;
    }
    $gtfs2->process_010_flatten_vehicle_positions();
    $gtfs2->process_020_flatten_trip_updates();
    if ($stage < 2) {
	return $o;
    }
    $gtfs2->process_030_consolidate_records();
    if ($stage < 3) {
	return $o;
    }
    $gtfs2->process_040_mark_as_of_times();
    $gtfs2->process_050_remove_turds();
    $gtfs2->process_060_populate_trip_and_route_info();
    $gtfs2->process_070_mark_next_coming_stops();
    if ($args{limit_stop_time_updates}) {
	$gtfs2->process_080_remove_most_stop_time_update_data();
    }
    if ($stage < 4) {
	return $o;
    }
    $gtfs2->process_090_populate_stop_information();
    return $o;
}

sub print_realtime_status_raw {
    my ($self) = @_;
    my $o = $self->get_realtime_status_data(limit_stop_time_updates => 1);
    print(Dumper($o));
}

sub print_realtime_status {
    my ($self) = @_;
    my $o = $self->get_realtime_status_data(limit_stop_time_updates => 1);

    print("                                                                                                   Stop                                  SCHDULED Realtime    \n");
    print("Coach Route                                  Trip ID Headsign                           As of      Seq. Next Stop Location               Time     Time     Dly\n");
    print("----- ----- -------------------------------- ------- --------------------------------   --------   ---- -------------------------------- -------- -------- ---\n");

    foreach my $tu ($self->get_sorted_trip_updates()) {
	my $as_of = $tu->{as_of};
	my $stu = $tu->{next_stop};

	my $dep_time        = eval { $stu->{dep_time} };
	my $next_stop_name  = eval { $stu->{stop_name} };
	my $next_stop_delay = eval { $stu->{delay} };
	for ($next_stop_delay) {
	    if (defined $_) {
		$_ = int($_ / 60 + 0.5);
	    }
	}
	my $realtime_time = eval { $stu->{realtime_time} };

	my $fmt_as_of         = $as_of         && eval { strftime("%H:%M:%S", localtime($as_of        )) } // "-";
	my $fmt_realtime_time = $realtime_time && eval { strftime("%H:%M:%S", localtime($realtime_time)) } // "-";
	my $scheduled_time = eval { $stu->{scheduled_time} };

	for ($stu->{stop_sequence}) {
	    $_ = "#$_" if defined $_;
	}

	printf("%-5s %5s %-32.32s %-7s %-32.32s   %-8s   %4s %-32.32s %-8s %-8s %3s\n",
	       $tu->{label} // "-",
	       $tu->{route_short_name} // "-",
	       $tu->{route_long_name} // "-",
	       $tu->{trip_id},
	       $tu->{trip_headsign} // "-",
	       $fmt_as_of,
	       $stu->{stop_sequence} // "",
	       $next_stop_name // "-",
	       $scheduled_time // "-",
	       $fmt_realtime_time,
	       $next_stop_delay // "",
	      );
    }
}

sub _route_id_cmp {
    my ($stringA, $stringB) = @_;
    if ($stringA =~ m{\d+}) {
	my ($prefixA, $numberA, $suffixA) = ($`, $&, $');
	if ($stringB =~ m{\d+}) {
	    my ($prefixB, $numberB, $suffixB) = ($`, $&, $');
	    if ($prefixA eq $prefixB) {
		return ($numberA <=> $numberB) || _route_id_cmp($suffixA, $suffixB);
	    }
	}
    }
    return $stringA cmp $stringB;
}

sub read_realtime_feed {
    my ($self, $filename) = @_;
    $self->pull_gtfs_realtime_protocol();
    $filename = File::Spec->rel2abs($filename, $self->{dir});
    if (open(my $fh, "<", $filename)) {
	binmode($fh);
	my $o = TransitRealtime::FeedMessage->decode(join("", <$fh>));
	return $o;
    } else {
	die("Cannot read $filename: $!\n");
    }
}

sub list_realtime_feeds {
    my ($self) = @_;

    my @feeds = $self->db->get_geo_gtfs_realtime_feeds($self->{geo_gtfs_agency_id});
    print("id    active?  feed type  url\n");
    print("----  -------  ---------  -------------------------------------------------------------------------------\n");
    foreach my $feed (@feeds) {
	printf("%4d  %4d     %-9s  %s\n", @{$feed}{qw(id is_active feed_type url)});
    }
}

###############################################################################
# GTFS
###############################################################################

use Archive::Zip qw( :ERROR_CODES :CONSTANTS );
use Archive::Zip::MemberRead;

use Digest::MD5 qw/md5_hex/;

sub process_gtfs_feed {
    my ($self, $request, $response) = @_;
    my $url = $response->base;
    my $cached = ($response->code == 304 || ($response->header("X-Cached") && $response->header("X-Content-Unchanged")));
    my $cref = $response->content_ref;

    my $retrieved     = $response->date;
    my $last_modified = $response->last_modified;
    my $content_length = $response->content_length;

    my $md5 = md5_hex($url);

    my $zip_filename     = sprintf("%s/data/%s/gtfs/%s-%s-%s-%s.zip", $self->{dir}, $self->{geo_gtfs_agency_name}, $md5, $retrieved, $last_modified, $content_length);
    my $rel_zip_filename = sprintf(   "data/%s/gtfs/%s-%s-%s-%s.zip",               $self->{geo_gtfs_agency_name}, $md5, $retrieved, $last_modified, $content_length);
    make_path(dirname($zip_filename));
    if (open(my $fh, ">", $zip_filename)) {
	binmode($fh);
	print {$fh} $$cref;
	close($fh);
    } else {
	die("Cannot write $zip_filename: $!\n");
    }

    my $zip = Archive::Zip->new();
    unless ($zip->read($zip_filename) == AZ_OK) {
	die("zip read error $zip_filename\n");
    }
    my @members = $zip->members();

    $self->{geo_gtfs_feed_id} = $self->db->select_or_insert_geo_gtfs_feed_id($self->{geo_gtfs_agency_id}, $url);
    my $geo_gtfs_feed_instance_id = $self->db->select_or_insert_geo_gtfs_feed_instance_id($self->{geo_gtfs_feed_id}, $rel_zip_filename, $retrieved, $last_modified);

    my $sth;

    my $save_select = select(STDERR);
    my $save_flush  = $|;
    $| = 1;

    foreach my $member (@members) {
	my $filename = $member->fileName();
	my $basename = basename($filename, ".txt");
	my $table_name = "gtfs_$basename";

	$sth = $self->dbh->table_info(undef, "%", $table_name, "TABLE");
	$sth->execute();
	my $row = $sth->fetchrow_hashref();
	if (!$row) {
	    warn("No such table: $table_name\n");
	    next;
	}

	my $fh = Archive::Zip::MemberRead->new($zip, $filename);

	my $csv = Text::CSV->new ({ binary => 1 });
	my $fields = $csv->getline($fh);
	die("no fields in member $filename of $zip_filename\n")
	  unless $fields or scalar(@$fields);

	$self->dbh->do("delete from $table_name where geo_gtfs_feed_instance_id = ?",
		       {}, $geo_gtfs_feed_instance_id);

	my $sql = sprintf("insert into $table_name(geo_gtfs_feed_instance_id, %s) values(?, %s);",
			  join(", ", @$fields),
			  join(", ", ("?") x scalar(@$fields)));
	$sth = $self->dbh->prepare($sql);

	print STDERR ("Populating $table_name ... ");

	my $rows = 0;
	while (defined(my $data = $csv->getline($fh))) {
	    $sth->execute($geo_gtfs_feed_instance_id, @$data);
	    $rows += 1;
	}
	print STDERR ("$rows rows inserted.\n");
    }
    $self->dbh->commit();

    $| = $save_flush;
    select($save_select);
}

#------------------------------------------------------------------------------

sub update {
    my ($self) = @_;
}

sub list_routes {
    my ($self) = @_;
}

###############################################################################
# AGENCIES
###############################################################################

sub list_agencies {
    my ($self) = @_;
    my $sth = $self->dbh->prepare("select * from geo_gtfs_agency");
    $sth->execute();
    print("id    name\n");
    print("----  --------------------------------\n");
    while (my $row = $sth->fetchrow_hashref()) {
	printf("%4d  %s\n", $row->{id}, $row->{name});
    }
}

sub is_agency_name {
    my ($self, $arg) = @_;
    return $arg =~ m{^
		     [A-Za-z0-9]+(-[A-Za-z0-9]+)*
		     (\.[A-Za-z0-9]+(-[A-Za-z0-9]+)*)+
		     $}xi
		       && !$self->is_ipv4_address($arg);
}

###############################################################################
# MISCELLANY
###############################################################################

sub exec_sqlite_utility {
    my ($self) = @_;
    my $dbfile = $self->{sqlite_filename};
    exec("sqlite3", $dbfile) or die("cannot exec sqlite: $!\n");
}

sub is_ipv4_address {
    my ($self, $arg) = @_;
    return 0 unless $arg =~ m{^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$};
    my @octet = ($1, $2, $3, $4);
    return all { $_ >= 0 && $_ <= 255 } @octet;
}

sub is_url {
    my ($self, $arg) = @_;
    return $arg =~ m{^https?://}i;
}

###############################################################################
# WEBBERNETS
###############################################################################

BEGIN {
    # in osx you may have to run: cpan Crypt::SSLeay and do other
    # things
    my ($uname) = uname();
    if ($uname =~ m{^Darwin}) {
	my $ca_file = "/usr/local/opt/curl-ca-bundle/share/ca-bundle.crt";
	if (-e $ca_file) {
	    $ENV{HTTPS_CA_FILE} = $ca_file;
	} else {
	    warn(<<"END");

Looks like you are using a Mac.  You should run:
    brew install curl-ca-bundle.
You may also need to run:
    sudo cpan Crypt::SSLeay

END
	    exit(1);
	}
    }
}

sub ua {
    my ($self) = @_;
    return $self->{ua} //= LWP::UserAgent->new();
}

###############################################################################
# GENERIC UTILITY FUNCTIONS
###############################################################################

sub json {
    my ($self) = @_;
    return $self->{json} //= JSON->new()->allow_nonref()->pretty()->convert_blessed();
}

###############################################################################
# DB WRAPPER
###############################################################################

use Geo::GTFS2::DB;

sub dbh {
    my ($self) = @_;
    return $self->db->dbh;
}

sub db {
    my ($self) = @_;
    return $self->{db} if $self->{db};
    return $self->{db} = Geo::GTFS2::DB->new();
}

1; # End of Geo::GTFS2
