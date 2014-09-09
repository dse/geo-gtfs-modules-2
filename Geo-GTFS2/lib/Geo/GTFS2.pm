package Geo::GTFS2;
use strict;
use warnings;

use DBI;
use Data::Dumper;
use File::Basename qw(dirname basename);
use File::MMagic;		# see note [1] below
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

# [1] File::MMagic best detects .zip files, and allows us to add magic
# for Google Protocol Buffers files.

use constant IGNORE_FIELDS => 1;

BEGIN {
    if (!IGNORE_FIELDS) {
	require fields;
	import fields qw(dir http_cache_dir
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
    }
}

sub new {
    my ($class, %args) = @_;
    my $self = IGNORE_FIELDS ? bless({}, $class) : fields::new($class);
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

sub set_agency_id {
    my ($self, $agency_id) = @_;
    $self->{geo_gtfs_agency_id} = $agency_id;
    my $agency = $self->db->select_geo_gtfs_agency_by_id($agency_id);
    if (!$agency) {
	die("No agency with id: $agency_id\n");
    }
    $self->{geo_gtfs_agency_name} = $agency->{name};
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

    return $o;
}

###############################################################################

sub get_vehicle_feed {
    my ($self) = @_;
    my $o = $self->fetch_realtime_all_data_feed();
    my @t;
    my @v;
    foreach my $e (@{$o->{entity}}) {
	my $v = $e->{vehicle};
	my $t = $e->{trip_update};
	push(@v, $v) if $v;
	push(@t, $t) if $t;
    }
    foreach my $v (@v) {
	$self->flatten_vehicle_record($v);
    }
    foreach my $t (@t) {
	$self->flatten_trip_update_record($t);
    }
    my @combined = $self->get_combined_records(\@t, \@v);
    foreach my $combined (@combined) {
	my $stu_array = $combined->{stop_time_update};

	if ($stu_array) {
	    foreach my $stu (@$stu_array) {
		$self->flatten_stop_time_update_record($stu);
		$self->enhance_stop_time_update_record($stu);
	    }
	    $self->mark_expected_next_stop($combined);
	    @$stu_array = grep { $_->{expected_next_stop} } @$stu_array;
	    if (scalar(@$stu_array)) {
		foreach my $stu (@$stu_array) {
		    $self->enhance_timestamps_in($stu);
		    $self->populate_stop_information($combined, $stu);
		}
		$combined->{next_stop} = $stu_array->[0];
	    }
	    delete $combined->{stop_time_update};
	}

	$self->populate_trip_and_route_info($combined);
	$self->enhance_timestamps_in($combined);
	$self->mark_combined_record_as_needed($combined);
    }

    @combined = map { [ $_, $_->{route_id} // 0, $_->{direction_id} // 0 ] } @combined;
    @combined = sort { _route_id_cmp($a->[1], $b->[1]) || $a->[2] <=> $b->[2] } @combined;
    @combined = map { $_->[0] } @combined;

    return {
	header  => $o->{header},
	vehicle => \@combined
       };
}

sub flatten_vehicle_record {
    my ($self, $v) = @_;
    my $trip_id = eval { delete $v->{trip}->{trip_id} };
    my $latitude     = eval { delete $v->{position}->{latitude} };
    my $longitude     = eval { delete $v->{position}->{longitude} };
    my $label   = eval { delete $v->{vehicle}->{label} };
    delete $v->{trip}     if !scalar(keys(%{$v->{trip}}));
    delete $v->{position} if !scalar(keys(%{$v->{position}}));
    delete $v->{vehicle}  if !scalar(keys(%{$v->{vehicle}}));
    $v->{trip_id} = $trip_id if defined $trip_id;
    $v->{latitude}     = $latitude     if defined $latitude;
    $v->{longitude}     = $longitude     if defined $longitude;
    $v->{label}   = $label   if defined $label;
}

sub flatten_trip_update_record {
    my ($self, $t) = @_;
    my $trip_id    = eval { delete $t->{trip}->{trip_id} };
    my $label      = eval { delete $t->{vehicle}->{label} };
    my $route_id   = eval { delete $t->{trip}->{route_id} };
    my $start_time = eval { delete $t->{trip}->{start_time} };
    my $start_date = eval { delete $t->{trip}->{start_date} };
    delete $t->{trip}     if !scalar(keys(%{$t->{trip}}));
    delete $t->{vehicle}  if !scalar(keys(%{$t->{vehicle}}));
    $t->{trip_id}  = $trip_id  if defined $trip_id;
    $t->{label}    = $label    if defined $label;
    $t->{route_id} = $route_id if defined $route_id;
    $t->{start_time} = $start_time if defined $start_time;
    $t->{start_date} = $start_date if defined $start_date;
    foreach my $stu (eval { @{$t->{stop_time_update}} }) {
	$self->flatten_stop_time_update_record($stu);
    }
}

sub get_combined_records {
    my ($self, $vehicle_array, $trip_update_array) = @_;
    my %vehicle_by_trip_id;
    my @combined;
    foreach my $v (@$vehicle_array) {
	my $trip_id = eval { $v->{trip} && $v->{trip}->{trip_id} } // $v->{trip_id};
	$vehicle_by_trip_id{$trip_id} = $v if defined $trip_id;
    }
    foreach my $t (@$trip_update_array) {
	my $trip_id = eval { $t->{trip} && $t->{trip}->{trip_id} } // $t->{trip_id};
	my $v = $vehicle_by_trip_id{$trip_id};
	my $combined = { %$v, %$t };
	push(@combined, $combined);
    }
    return @combined;
}

sub mark_combined_record_as_needed {
    my ($self, $r) = @_;
    my $timestamp = $r->{timestamp};
    if (!defined $timestamp || $timestamp < time() - 3600) {
	$r->{_exclude_} = 1;
    }
}

sub populate_trip_and_route_info {
    my ($self, $t) = @_;
    my $route_id   = $t->{route_id};
    my $start_date = $t->{start_date};
    my $trip_id    = $t->{trip_id};
    if (defined $start_date && defined $route_id) {
	my ($geo_gtfs_feed_instance_id, $service_id) = $self->db->get_geo_gtfs_feed_instance_id_and_service_id($self->{geo_gtfs_agency_id}, $start_date);
	my $trip_record = $self->db->get_gtfs_trip($geo_gtfs_feed_instance_id, $trip_id);
	$t->{trip_headsign}    = $trip_record->{trip_headsign};
	$t->{direction_id}     = $trip_record->{direction_id};
	$t->{block_id}         = $trip_record->{block_id};
	my $route_record = $self->db->get_gtfs_route($geo_gtfs_feed_instance_id, $route_id);
	$t->{route_short_name} = $route_record->{route_short_name};
	$t->{route_long_name}  = $route_record->{route_long_name};
    }
}

sub enhance_timestamps_in {
    my ($self, $r) = @_;

    my $ts = $r->{timestamp};
    if (defined $ts && $ts =~ m{^\d+$}) {
	$r->{timestamp_fmt} = strftime("%a, %d %b %Y %H:%M:%S %z", localtime($ts));
    }

    my $t = $r->{time};
    if (defined $t && $t =~ m{^\d+$}) {
	$r->{time_fmt} = strftime("%a, %d %b %Y %H:%M:%S %z", localtime($t));
	$r->{time_hh_mm_ss} = strftime("%H:%M:%S", localtime($t));
    }
}

#------------------------------------------------------------------------------

sub get_trip_details_feed {
    my ($self, $trip_id) = @_;
    my $o = $self->fetch_realtime_all_data_feed();
    my @t = grep { eval { $_->{trip}->{trip_id} eq $trip_id } } map { $_->{trip_update} || () } @{$o->{entity}};
    my @v = grep { eval { $_->{trip}->{trip_id} eq $trip_id } } map { $_->{vehicle}     || () } @{$o->{entity}};
    foreach my $t (@t) {
	$self->flatten_trip_update_record($t);
    }
    foreach my $v (@v) {
	$self->flatten_vehicle_record($v);
    }
    my @combined = $self->get_combined_records(\@t, \@v);
    foreach my $combined (@combined) {
	$self->enhance_timestamps_in($combined);
	$self->populate_trip_and_route_info($combined);
	$self->mark_combined_record_as_needed($combined);
	my $stu_array = $combined->{stop_time_update};
	next unless $stu_array;
	foreach my $stu (@{$stu_array}) {
	    $self->flatten_stop_time_update_record($stu);
	    $self->enhance_stop_time_update_record($stu);
	    $self->enhance_timestamps_in($stu);
	    $self->populate_stop_information($combined, $stu);
	}
	$self->mark_expected_next_stop($combined);
    }
    return {
	header => $o->{header},
	trip_update => \@combined
       };
}

sub populate_stop_information {
    my ($self, $tu, $stu) = @_;

    my $stop_id = $stu->{stop_id};
    if (!defined $stop_id) {
	return;
    }
    
    my ($geo_gtfs_feed_instance_id, $service_id) = $self->db->get_geo_gtfs_feed_instance_id_and_service_id($self->{geo_gtfs_agency_id}, $tu->{start_date});
    if (!defined $geo_gtfs_feed_instance_id) {
	return;
    }

    my $stop = $self->db->get_gtfs_stop($geo_gtfs_feed_instance_id, $stop_id);
    if (!$stop) {
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
	return;
    }

    my $stop_time_record = $self->db->get_gtfs_stop_time($geo_gtfs_feed_instance_id,
							 $stop_id, $trip_id);
    if ($stop_time_record) {

	my $scheduled_arrival_time   = $stop_time_record->{arrival_time};
	my $scheduled_departure_time = $stop_time_record->{departure_time};
	my $scheduled_time           = $scheduled_arrival_time // $scheduled_departure_time;
	my $sequence                 = $stop_time_record->{stop_sequence};

	$stu->{scheduled_arrival_time}   = $scheduled_arrival_time   if defined $scheduled_arrival_time;
	$stu->{scheduled_departure_time} = $scheduled_departure_time if defined $scheduled_departure_time;
	$stu->{scheduled_time}           = $scheduled_time           if defined $scheduled_time;
    }
}

sub flatten_stop_time_update_record {
    my ($self, $stu) = @_;

    my $dep_time      = eval { delete $stu->{departure}->{time} };
    my $arr_time      = eval { delete $stu->{arrival}->{time} };
    my $dep_delay     = eval { delete $stu->{departure}->{delay} };
    my $arr_delay     = eval { delete $stu->{arrival}->{delay} };

    $stu->{departure_time}  = $dep_time  if defined $dep_time;
    $stu->{arrival_time}    = $arr_time  if defined $arr_time;
    $stu->{departure_delay} = $dep_delay if defined $dep_delay;
    $stu->{arrival_delay}   = $arr_delay if defined $arr_delay;

    delete $stu->{departure} if !eval { scalar(keys(%{$stu->{departure}})) };
    delete $stu->{arrival}   if !eval { scalar(keys(%{$stu->{arrival}})) };
}

sub enhance_stop_time_update_record {
    my ($self, $stu) = @_;

    my $time  = $stu->{arrival_time}  // $stu->{departure_time};
    my $delay = $stu->{arrival_delay} // $stu->{departure_delay};
    my $delay_minutes = defined $delay && int($delay / 60 + 0.5);

    $stu->{time}          = $time          if defined $time;
    $stu->{delay}         = $delay         if defined $delay;
    $stu->{delay_minutes} = $delay_minutes if defined $delay_minutes;
}

sub mark_expected_next_stop {
    my ($self, $r) = @_;
    my $stu_array = $r->{stop_time_update};
    if (!$stu_array) {
	return;
    }

    my $index = 0;
    my $time = time();
    foreach my $stu (@{$stu_array}) {
	if ($stu->{time} < $time) {
	    $stu->{expected_to_have_been_passed} = 1;
	} elsif ($stu->{time} >= $time) {
	    $r->{expected_next_stop_index} = $index;
	    $stu->{expected_next_stop} = 1;
	    return;
	}
	$index += 1;
    }
}

#------------------------------------------------------------------------------

sub realtime_status {
    my ($self) = @_;
    my $data = $self->get_vehicle_feed();
    print(<<"END");
Bus# Rte. Destination                              Trip ID Exp.Time Dly Exp.Next Stop
---- ---- ---------------------------------------- ------- -------- --- ----------------------------------------
END
    foreach my $r (grep { !$_->{_exclude_} } @{$data->{vehicle}}) {
	my $ns = $r->{next_stop};
	printf("%4s %4s %-40s %-7s %-8s %3s %-40s\n",
	       $r->{label}                   // "-",
	       $r->{route_short_name}        // "-",
	       $r->{trip_headsign}           // "-",
	       $r->{trip_id}                 // "-",
	       eval { $ns->{time_hh_mm_ss} } // "-",
	       eval { $ns->{delay_minutes} } // "-",
	       eval { $ns->{stop_name} }     // "-");
    }
}

sub trip_status {
    my ($self, $trip_id) = @_;
    my $data = $self->get_trip_details_feed($trip_id);
    my ($tu) = @{$data->{trip_update}};
    print(<<"END");
    Exp.Time Dly Stop                                     Sch.Time
    -------- --- ---------------------------------------- --------
END
    foreach my $stu (@{$tu->{stop_time_update}}) {
	my $expected_next_stop = $stu->{expected_next_stop} ? "***" : "   ";
	printf("%-3s %-8s %3s %-40s %8s\n",
	       $expected_next_stop,
	       $stu->{time_hh_mm_ss}  // "-",
	       $stu->{delay_minutes}  // "-",
	       $stu->{stop_name}      // "-",
	       $stu->{scheduled_time} // "-");
    }
}

sub fetch_realtime_all_data_feed {
    my ($self) = @_;
    my $feed = $self->db->get_geo_gtfs_realtime_feed_by_type($self->{geo_gtfs_agency_id}, "all");
    my $url = $feed->{url};
    return $self->process_url($url);
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

###############################################################################

# sub fetch_all_realtime_data {
#     my ($self) = @_;
#     my @feeds = $self->db->get_geo_gtfs_realtime_feeds($self->{geo_gtfs_agency_id});
#     my %feeds = map { ($_->{feed_type} => $_) } @feeds;
#     if ($feeds{all}) {
# 	my $feed = $feeds{all};
# 	my $url = $feed->{url};
# 	$self->process_url($url);
#     } else {
# 	$self->fetch_all_realtime_feeds();
#     }
# }

# sub fetch_all_realtime_feeds {
#     my ($self) = @_;
#     my @feeds = $self->db->get_geo_gtfs_realtime_feeds($self->{geo_gtfs_agency_id});
#     foreach my $feed (@feeds) {
# 	my $url = $feed->{url};
# 	$self->process_url($url);
#     }
# }

# sub fetch_realtime_vehicle_position_feed {
#     my ($self) = @_;
#     my $feed = $self->db->get_geo_gtfs_realtime_feed_by_type($self->{geo_gtfs_agency_id}, "positions");
#     my $url = $feed->{url};
#     $self->process_url($url);
# }

# sub get_cooked_realtime_vehicle_position_feed {
#     my ($self) = @_;
#     my $o = $self->fetch_realtime_vehicle_position_feed;
#     my @v;
#     foreach my $e (@{$o->{entity}}) {
# 	my $v = $e->{vehicle};
# 	push(@v, $v);
#     }
#     $o->{vehicle} = \@v;
#     delete $o->{entity};
#     return $o;
# }

# sub populate_trip_update_record {
#     my ($self, $t) = @_;
#     my $route_id   = $t->{route_id};
#     my $start_date = $t->{start_date};
#     my $trip_id    = $t->{trip_id};
#     if (defined $start_date && defined $route_id) {
# 	my ($geo_gtfs_feed_instance_id, $service_id) = $self->db->get_geo_gtfs_feed_instance_id_and_service_id($self->{geo_gtfs_agency_id}, $start_date);
# 	my $trip_record = $self->db->get_gtfs_trip($geo_gtfs_feed_instance_id, $trip_id);
# 	$t->{trip_headsign}    = $trip_record->{trip_headsign};
# 	$t->{direction_id}     = $trip_record->{direction_id};
# 	$t->{block_id}         = $trip_record->{block_id};
# 	my $route_record = $self->db->get_gtfs_route($geo_gtfs_feed_instance_id, $route_id);
# 	$t->{route_short_name} = $route_record->{route_short_name};
# 	$t->{route_long_name}  = $route_record->{route_long_name};
#     }
# }

# sub populate_stop_time_update_record {
#     my ($self, $tu, $stu) = @_;
#     $self->process_090_populate_stop_time_update($tu, $stu);
# }

# sub list_latest_realtime_feeds {
#     my ($self) = @_;

#     my @instances = $self->db->get_latest_geo_gtfs_realtime_feed_instances($self->{geo_gtfs_agency_id});
#     print("id      feed type  filename\n");
#     print("------  ---------  -------------------------------------------------------------------------------\n");
#     foreach my $i (@instances) {
# 	printf("%6d  %-9s  %s\n",
# 	       @{$i}{qw(id feed_type filename)});
#     }
# }

# sub get_latest_fetched_data {
#     my ($self) = @_;

#     my @instances = $self->db->get_latest_geo_gtfs_realtime_feed_instances($self->{geo_gtfs_agency_id});
#     my %instances = map { ($_->{feed_type}, $_) } @instances;
#     my $all = $instances{all};
#     return unless $all;

#     my @instances_to_check;
#     if ($instances{all}) {
# 	@instances_to_check = ($instances{all});
#     } else {
# 	@instances_to_check = @instances;
#     }

#     $self->{vehicle_positions} = {};
#     $self->{vehicle_positions_by_trip_id} = {};
#     $self->{trip_updates} = {};
#     $self->{vehicle_positions_array} = [];
#     $self->{trip_updates_array} = [];

#     my @o;

#     foreach my $instance (@instances) {
# 	my $o = $self->read_realtime_feed($instance->{filename});
# 	if ($instance->{feed_type} eq "all") {
# 	    $self->{realtime_feed}->{all} = $o;
# 	}
# 	my $header_timestamp = eval { $o->{header}->{timestamp} };
# 	$self->{header} = $o->{header};
# 	$self->{headers}->{all} = $o->{header};
# 	foreach my $e (@{$o->{entity}}) {
# 	    my $vp = $e->{vehicle};
# 	    my $tu = $e->{trip_update};
# 	    if ($vp) {
# 		eval {
# 		    $vp->{header_timestamp} = $header_timestamp if defined $header_timestamp;
# 		    my $label   = eval { $vp->{vehicle}->{label} };
# 		    my $trip_id = eval { $vp->{trip}->{trip_id} };

# 		    if (defined $label || defined $trip_id) {
# 			$self->{vehicle_positions}->{$label} = $vp if defined $label;
# 			$self->{vehicle_positions_by_trip_id}->{$trip_id} = $vp if defined $trip_id;
# 			$vp->{_consolidate_} = 1;
# 		    }
# 		    push(@{$self->{vehicle_positions_array}}, $vp);
# 		}
# 	    }
# 	    if ($tu) {
# 		eval {
# 		    $tu->{header_timestamp} = $header_timestamp if defined $header_timestamp;
# 		    $self->{trip_updates}->{$tu->{trip}->{trip_id}} = $tu;
# 		    push(@{$self->{trip_updates_array}}, $tu);
# 		}
# 	    }
# 	}
# 	push(@o, $o);
#     }

#     if (wantarray) {
# 	return @o;
#     }
#     return \@o;
# }

# sub process_010_flatten_vehicle_positions {
#     my ($self) = @_;
#     foreach my $vp (@{$self->{vehicle_positions_array}}) {
# 	eval { $vp->{trip_id}   = delete $vp->{trip}->{trip_id}       if defined $vp->{trip}->{trip_id};       };
# 	eval { $vp->{latitude}       = delete $vp->{position}->{latitude}  if defined $vp->{position}->{latitude};  };
# 	eval { $vp->{longitude}       = delete $vp->{position}->{longitude} if defined $vp->{position}->{longitude}; };
# 	eval { $vp->{label}     = delete $vp->{vehicle}->{label}      if defined $vp->{vehicle}->{label};      };
# 	foreach my $k (qw(trip position vehicle)) {
# 	    delete $vp->{$k} if exists $vp->{$k} && !scalar(keys(%{$vp->{$k}}));
# 	}
#     }
# }

# sub process_020_flatten_trip_updates {
#     my ($self) = @_;
#     foreach my $tu ($self->get_trip_update_list()) {
# 	eval { $tu->{trip_id}    = delete $tu->{trip}->{trip_id}    if defined $tu->{trip}->{trip_id};    };
# 	eval { $tu->{start_time} = delete $tu->{trip}->{start_time} if defined $tu->{trip}->{start_time}; };
# 	eval { $tu->{start_date} = delete $tu->{trip}->{start_date} if defined $tu->{trip}->{start_date}; };
# 	eval { $tu->{route_id}   = delete $tu->{trip}->{route_id}   if defined $tu->{trip}->{route_id};   };
# 	eval { $tu->{label}      = delete $tu->{vehicle}->{label}   if defined $tu->{vehicle}->{label};   };
# 	foreach my $stu (eval { @{$tu->{stop_time_update}} }) {
# 	    $self->flatten_stop_time_update_record($stu);
# 	}
# 	foreach my $k (qw(trip vehicle)) {
# 	    delete $tu->{$k} if !scalar(keys(%{$tu->{$k}}));
# 	}
#     }
# }

# sub process_040_mark_as_of_times {
#     my ($self) = @_;
#     foreach my $tu ($self->get_trip_update_list()) {
# 	my $timestamp        = delete $tu->{timestamp};
# 	my $header_timestamp = delete $tu->{header_timestamp};
# 	$tu->{as_of} = $timestamp // $header_timestamp // time();
#     }
# }

# sub get_trip_update_list {
#     my ($self) = @_;
#     return grep { !$_->{_exclude_} } @{$self->{trip_updates_array}};
# }

# sub process_030_consolidate_records {
#     my ($self) = @_;
#     foreach my $tu ($self->get_trip_update_list()) {
# 	my $label   = eval { $tu->{label} };
# 	my $trip_id = eval { $tu->{trip_id} };

# 	my $vp_by_label   = defined $label   && $self->{vehicle_positions}->{$label};
# 	my $vp_by_trip_id = defined $trip_id && $self->{vehicle_positions_by_trip_id}->{$trip_id};

# 	if ($vp_by_label && $vp_by_trip_id && $vp_by_label ne $vp_by_trip_id) {
# 	    next;
# 	}
# 	my $vp = $vp_by_label // $vp_by_trip_id;
# 	$vp->{_consolidate_} = 1;

# 	foreach my $key (grep { $_ ne "_consolidate_" } keys(%$vp)) {
# 	    if (!exists $tu->{$key}) {
# 		$tu->{$key} = delete $vp->{$key};
# 	    } elsif ($tu->{$key} eq $vp->{$key}) {
# 		delete $vp->{$key};
# 	    }
# 	}
# 	delete $self->{vehicle_positions}->{$label} if defined $label;
# 	delete $self->{vehicle_positions_by_trip_id}->{$trip_id} if defined $trip_id;

# 	$vp->{_delete_} = !scalar(grep { $_ ne "_consolidate_" } keys(%$vp));
#     }

#     my $vp_array = $self->{vehicle_positions_array};
#     @$vp_array = grep { !$_->{_delete_} } @$vp_array;

#     my $entity_array_ref = $self->{realtime_feed}->{all}->{entity};
#     @$entity_array_ref = grep { !$_->{vehicle} || !$_->{vehicle}->{_delete_} } @$entity_array_ref;
# }

# sub get_sorted_trip_updates {
#     my ($self) = @_;
#     my @tu = $self->get_trip_update_list();
#     @tu = map { [ $_, $_->{route_id} // 0, $_->{direction_id} // 0 ] } @tu;
#     @tu = sort { _route_id_cmp($a->[1], $b->[1]) || $a->[2] <=> $b->[2] } @tu;
#     @tu = map { $_->[0] } @tu;
#     return @tu;
# }

# use constant TIMESTAMP_CUTOFF_AGE => 3600;

# sub process_050_remove_turds {
#     my ($self) = @_;

#     foreach my $tu ($self->get_trip_update_list()) {
# 	my $label    = eval { $tu->{label} };
# 	my $trip_id  = eval { $tu->{trip_id} };
# 	my $route_id = eval { $tu->{route_id} };
# 	my $start_date = eval { $tu->{start_date} };
# 	if (!defined $label) {
# 	    $tu->{_exclude_} = "no vehicle label";
# 	    next;
# 	}
# 	if (!defined $trip_id) {
# 	    $tu->{_exclude_} = "no trip_id";
# 	    next;
# 	}
# 	if (!defined $route_id) {
# 	    $tu->{_exclude_} = "no route_id";
# 	    next;
# 	}
# 	if (!defined $start_date) {
# 	    $tu->{_exclude_} = "no start_date";
# 	    next;
# 	}
# 	if ($tu->{as_of} < time() - TIMESTAMP_CUTOFF_AGE) {
# 	    $tu->{_exclude_} = "old";
# 	    next;
# 	}
#     }
# }

# sub process_060_populate_trip_and_route_info {
#     my ($self) = @_;

#     foreach my $tu ($self->get_trip_update_list()) {
# 	my $label    = eval { $tu->{label} };
# 	my $trip_id  = eval { $tu->{trip_id} };
# 	my $route_id = eval { $tu->{route_id} };
# 	my $start_date = eval { $tu->{start_date} };

# 	my ($geo_gtfs_feed_instance_id, $service_id) = $self->db->get_geo_gtfs_feed_instance_id_and_service_id($self->{geo_gtfs_agency_id}, $tu->{start_date});
# 	my $trip = $self->db->get_gtfs_trip($geo_gtfs_feed_instance_id, $trip_id);
# 	if (!$trip) {
# 	    $tu->{_exclude_} = "no trip record";
# 	    next;
# 	}

# 	foreach my $key (qw(trip_headsign direction_id)) {
# 	    if (!defined $tu->{$key}) {
# 		$tu->{$key} = delete $trip->{$key};
# 	    } elsif ($tu->{$key} eq $trip->{$key}) {
# 		delete $trip->{$key};
# 	    }
# 	}

# 	my $route = $self->db->get_gtfs_route($geo_gtfs_feed_instance_id, $route_id);
# 	if (defined $route) {
# 	    foreach my $key (qw(route_color
# 				route_desc
# 				route_long_name
# 				route_short_name
# 				route_text_color
# 				route_type
# 				route_url)) {
# 		if (!defined $tu->{$key}) {
# 		    $tu->{$key} = $route->{$key};
# 		} elsif ($tu->{$key} eq $route->{$key}) {
# 		    $tu->{$key} = $route->{$key};
# 		}
# 	    }
# 	} else {
# 	    $tu->{_exclude_} = "no route for $geo_gtfs_feed_instance_id $route_id\n";
# 	    next;
# 	}
#     }
# }

# sub process_070_mark_next_coming_stops {
#     my ($self) = @_;
#     foreach my $tu ($self->get_trip_update_list()) {
# 	my $current_time = $tu->{header_timestamp} // $tu->{timestamp} // time();
# 	next unless defined $current_time;
# 	my $stu_index = -1;
# 	foreach my $stu (@{$tu->{stop_time_update}}) {
# 	    ++$stu_index;
# 	    my $stu_time = $stu->{realtime_time};
# 	    if (defined $stu_time && $current_time <= $stu_time) {
# 		$tu->{next_stop_time_update_index} = $stu_index;
# 		$stu->{is_next_stop_time_update} = 1;
# 		last;
# 	    }
# 	}
#     }
# }

# sub process_080_remove_most_stop_time_update_data {
#     my ($self) = @_;
#     foreach my $tu ($self->get_trip_update_list()) {
# 	my $stu_array = $tu->{stop_time_update};
# 	next unless $stu_array && scalar(@$stu_array);
# 	my $idx = delete $tu->{next_stop_time_update_index};
# 	if (defined $idx) {
# 	    my $ns = $tu->{stop_time_update}->[$idx];
# 	    if ($ns) {
# 		delete $tu->{stop_time_update};
# 		$tu->{next_stop} = $ns;
# 		delete $ns->{is_next_stop_time_update};
# 	    }
# 	} else {
# 	    delete $tu->{stop_time_update};
# 	}
# 	delete $tu->{next_stop_time_update_index};
#     }
# }

# sub process_090_populate_stop_time_update {
#     my ($self, $tu, $stu) = @_;

#     my $stop_id = $stu->{stop_id};
#     if (!defined $stop_id || $stop_id eq "UN") {
# 	$stu->{_exclude_} = "no stop_id";
# 	return;
#     }

#     my ($geo_gtfs_feed_instance_id, $service_id) = $self->db->get_geo_gtfs_feed_instance_id_and_service_id($self->{geo_gtfs_agency_id}, $tu->{start_date});
#     if (!defined $geo_gtfs_feed_instance_id) {
# 	$stu->{_exclude_} = "no geo_gtfs_feed_instance_id";
# 	return;
#     }

#     my $stop = $self->db->get_gtfs_stop($geo_gtfs_feed_instance_id, $stop_id);
#     if (!$stop) {
# 	$stu->{_exclude_} = sprintf("no stop info for %s, %s\n", $geo_gtfs_feed_instance_id, $stop_id);
# 	return;
#     }

#     foreach my $key (qw(stop_code stop_name stop_desc stop_lat stop_lon
# 			zone_id stop_url location_type parent_station
# 			stop_timezone wheelchair_boarding)) {
# 	if (!defined $stu->{$key}) {
# 	    $stu->{$key} = $stop->{$key};
# 	} elsif ($stu->{$key} eq $stop->{$key}) {
# 	    $stu->{$key} = $stop->{$key};
# 	}
#     }

#     my $trip_id = $tu->{trip_id};
#     if (!defined $trip_id) {
# 	$stu->{_exclude_} = "no trip_id";
# 	return;
#     }

#     my $stop_time_record = $self->db->get_gtfs_stop_time($geo_gtfs_feed_instance_id,
# 							 $stop_id, $trip_id);
#     if ($stop_time_record) {
# 	my $scheduled_arrival_time   = $stop_time_record->{arrival_time};
# 	my $scheduled_departure_time = $stop_time_record->{departure_time};
# 	my $scheduled_time           = $scheduled_arrival_time // $scheduled_departure_time;
# 	my $sequence                 = $stop_time_record->{stop_sequence};
# 	$stu->{scheduled_time}   = $scheduled_time if defined $scheduled_time;
# 	$stu->{stop_time_record} = $stop_time_record;
# 	$stu->{stop_sequence}    = $sequence if defined $sequence;
#     } else {
# 	warn("no stop_time_record for $geo_gtfs_feed_instance_id, $stop_id, $trip_id\n");
#     }
# }

# sub process_090_populate_stop_information {
#     my ($self) = @_;
#     foreach my $tu ($self->get_trip_update_list()) {
# 	my $ns = $tu->{next_stop};
# 	if ($ns) {
# 	    $self->process_090_populate_stop_time_update($tu, $ns);
# 	}
# 	my $stu_array = $tu->{stop_time_update};
# 	if ($stu_array && scalar(@$stu_array)) {
# 	    foreach my $stu (@$stu_array) {
# 		$self->process_090_populate_stop_time_update($tu, $stu);
# 	    }
# 	}
#     }
# }

# sub get_realtime_status_data {
#     my ($self, %args) = @_;
#     my $stage = $args{stage} // 9999;
#     if (!$args{nofetch}) {
# 	$self->fetch_all_realtime_data();
#     }

#     my $o = $self->get_latest_fetched_data();
#     if ($stage < 1) {
# 	return $o;
#     }
#     $self->process_010_flatten_vehicle_positions();
#     $self->process_020_flatten_trip_updates();
#     if ($stage < 2) {
# 	return $o;
#     }
#     $self->process_030_consolidate_records();
#     if ($stage < 3) {
# 	return $o;
#     }
#     $self->process_040_mark_as_of_times();
#     $self->process_050_remove_turds();
#     $self->process_060_populate_trip_and_route_info();
#     $self->process_070_mark_next_coming_stops();
#     if ($args{limit_stop_time_updates}) {
# 	$self->process_080_remove_most_stop_time_update_data();
#     }
#     if ($stage < 4) {
# 	return $o;
#     }
#     $self->process_090_populate_stop_information();
#     return $o;
# }

# sub print_trip_status_raw {
#     my ($self, $trip_id) = @_;
#     $self->get_realtime_status_data();
#     my $tu = $self->{trip_updates}->{$trip_id};
#     if (!$tu) {
# 	my @keys = keys %{$self->{trip_updates}};
# 	print("@keys\n");
# 	die("No trip with trip_id $trip_id.\n");
#     }
#     print(Dumper($tu));
# }

# sub print_trip_status {
#     my ($self, $trip_id) = @_;
#     $self->get_realtime_status_data();
#     my $tu = $self->{trip_updates}->{$trip_id};
#     if (!$tu) {
# 	my @keys = keys %{$self->{trip_updates}};
# 	print("@keys\n");
# 	die("No trip with trip_id $trip_id.\n");
#     }

#     printf("            Trip ID: %s\n", $trip_id);
#     printf("Longitude, Latitude: %.6f, %.6f\n",
# 	   $tu->{longitude}, $tu->{latitude});
#     printf("              Route: %s %s\n", $tu->{route_short_name}, $tu->{route_long_name});
#     printf("        Destination: %s\n", $tu->{trip_headsign});
#     printf("              As of: %s\n",  strftime("%Y-%m-%d %H:%M:%S %Z", localtime($tu->{as_of})));
#     printf("               (Now: %s)\n", strftime("%Y-%m-%d %H:%M:%S %Z", localtime()));
#     print("\n");

#     print("    Seq. Due Time                                  Longitude   Latitude    | Est.Time Delay\n");
#     print("    ---- -------- -------------------------------- ----------- ----------- | -------- -----\n");

#     foreach my $stu (@{$tu->{stop_time_update}}) {
# 	printf("%3s %4s %-8s %-32.32s %11.6f %11.6f | %-8s %5s\n",
# 	       eval { $stu->{is_next_stop_time_update} } ? "***" : "",
# 	       $stu->{stop_sequence} // "-",
# 	       $stu->{scheduled_time} // "-",
# 	       $stu->{stop_name} // "-",
# 	       $stu->{stop_lon},
# 	       $stu->{stop_lat},
# 	       $stu->{realtime_time} ? strftime("%H:%M:%S", localtime($stu->{realtime_time})) : "-",
# 	       $stu->{delay_minutes} // "");
#     }
    

# }

# sub print_realtime_status_raw {
#     my ($self) = @_;
#     my $o = $self->get_realtime_status_data(limit_stop_time_updates => 1);
#     print(Dumper($o));
# }

# sub print_realtime_status {
#     my ($self) = @_;
#     $self->get_realtime_status_data(limit_stop_time_updates => 1);

#     print("                                                                                                   Stop                                  SCHDULED Realtime    \n");
#     print("Coach Route                                  Trip ID Headsign                           As of      Seq. Next Stop Location               Time     Time     Dly\n");
#     print("----- ----- -------------------------------- ------- --------------------------------   --------   ---- -------------------------------- -------- -------- ---\n");

#     foreach my $tu ($self->get_sorted_trip_updates()) {
# 	my $as_of = $tu->{as_of};
# 	my $stu = $tu->{next_stop};

# 	my $dep_time        = eval { $stu->{dep_time} };
# 	my $next_stop_name  = eval { $stu->{stop_name} };
# 	my $next_stop_delay = eval { $stu->{delay_minutes} };
# 	my $realtime_time   = eval { $stu->{realtime_time} };

# 	my $fmt_as_of         = $as_of         && eval { strftime("%H:%M:%S", localtime($as_of        )) } // "-";
# 	my $fmt_realtime_time = $realtime_time && eval { strftime("%H:%M:%S", localtime($realtime_time)) } // "-";
# 	my $scheduled_time    = eval { $stu->{scheduled_time} };

# 	for ($stu->{stop_sequence}) {
# 	    $_ = "#$_" if defined $_;
# 	}

# 	printf("%-5s %5s %-32.32s %-7s %-32.32s   %-8s   %4s %-32.32s %-8s %-8s %3s\n",
# 	       $tu->{label} // "-",
# 	       $tu->{route_short_name} // "-",
# 	       $tu->{route_long_name} // "-",
# 	       $tu->{trip_id},
# 	       $tu->{trip_headsign} // "-",
# 	       $fmt_as_of,
# 	       $stu->{stop_sequence} // "",
# 	       $next_stop_name // "-",
# 	       $scheduled_time // "-",
# 	       $fmt_realtime_time,
# 	       $next_stop_delay // "",
# 	      );
#     }
# }

# sub read_realtime_feed {
#     my ($self, $filename) = @_;
#     $self->pull_gtfs_realtime_protocol();
#     $filename = File::Spec->rel2abs($filename, $self->{dir});
#     if (open(my $fh, "<", $filename)) {
# 	binmode($fh);
# 	my $o = TransitRealtime::FeedMessage->decode(join("", <$fh>));
# 	return $o;
#     } else {
# 	die("Cannot read $filename: $!\n");
#     }
# }

# sub get_realtime_feeds {
#     my ($self) = @_;
#     return $self->db->get_geo_gtfs_realtime_feeds($self->{geo_gtfs_agency_id});
# }

# sub list_realtime_feeds {
#     my ($self) = @_;

#     my @feeds = $self->db->get_geo_gtfs_realtime_feeds($self->{geo_gtfs_agency_id});
#     print("id    active?  feed type  url\n");
#     print("----  -------  ---------  -------------------------------------------------------------------------------\n");
#     foreach my $feed (@feeds) {
# 	printf("%4d  %4d     %-9s  %s\n", @{$feed}{qw(id is_active feed_type url)});
#     }
# }

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

sub list_routes {
    my ($self) = @_;
}

###############################################################################
# AGENCIES
###############################################################################

sub get_agencies {
    my ($self) = @_;
    my $sth = $self->dbh->prepare("select * from geo_gtfs_agency");
    $sth->execute();
    my @rows;
    while (my $row = $sth->fetchrow_hashref()) {
	push(@rows, $row);
    }
    return @rows;
}

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
