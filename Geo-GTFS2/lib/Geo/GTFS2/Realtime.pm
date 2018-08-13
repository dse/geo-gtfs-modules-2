package Geo::GTFS2::Realtime;
use warnings;
use strict;

use base "Geo::GTFS2::Object";

use Google::ProtocolBuffers;
use HTTP::Cache::Transparent;
use LWP::UserAgent;
use POSIX qw(strftime);
use JSON qw(-convert_blessed_universally);
use File::MMagic;		# see note [1] below
use Data::Dumper;

# [1] File::MMagic best detects .zip files, and allows us to add magic
# for Google Protocol Buffers files.

sub init {
    my ($self, %args) = @_;
    $self->SUPER::init(%args);

    if (!defined $self->{magic}) {
        $self->{magic} = File::MMagic->new();
        $self->{magic}->addMagicEntry("0\tstring\t\\x0a\\x0b\\x0a\\x03\tapplication/x-protobuf");
    }
    $self->{gtfs_realtime_proto} //= "https://developers.google.com/transit/gtfs-realtime/gtfs-realtime.proto";
    $self->{gtfs_realtime_protocol_pulled} //= 0;
}

sub process_url {
    my ($self, $url) = @_;
    my $ua = $self->ua;
    HTTP::Cache::Transparent::init({ BasePath => $self->{gtfs2}->{http_cache_dir},
                                     Verbose => 0,
                                     NoUpdate => 30,
                                     NoUpdateImpatient => 1 });
    my $request = HTTP::Request->new("GET", $url);
    my $response = $ua->request($request);
    $request->header("Date-Epoch", time());
    if (!$response->is_success) {
	warn(sprintf("%s => %s\n", $response->base, $response->status_line));
	return undef;
    }
    if ($response->content_type eq "application/protobuf") {
	return $self->process_gtfs_realtime_data($request, $response);
    } elsif ($response->content_type eq "application/json") {
	return $self->process_gtfs_realtime_data($request, $response);
    } elsif ($response->base =~ m{\.pb$}i) {
	return $self->process_gtfs_realtime_data($request, $response);
    } elsif ($response->base =~ m{\.json$}i) {
	return $self->process_gtfs_realtime_data($request, $response);
    } elsif ($url =~ m{\.pb$}i) {
	return $self->process_gtfs_realtime_data($request, $response);
    } elsif ($url =~ m{\.json$}i) {
	return $self->process_gtfs_realtime_data($request, $response);
    } else {
        return "declined";
    }
}

sub force_pull_gtfs_realtime_protocol {
    my ($self) = @_;
    $self->pull_gtfs_realtime_protocol(force => 1);
}

sub pull_gtfs_realtime_protocol {
    my ($self, %options) = @_;
    if (!$options{force}) {
        return 1 if $self->{gtfs_realtime_protocol_pulled};
    }
    HTTP::Cache::Transparent::init({ BasePath => $self->{gtfs2}->{http_cache_dir},
				     Verbose => 1,
				     $options{force} ? () : (NoUpdate => 86400),
                                     $options{force} ? (MaxAge => 0) : (),
				     $options{force} ? () : (UseCacheOnTimeout => 1),
				     NoUpdateImpatient => 0 });
    $self->warn_1("    Pulling GTFS-realtime protocol...\n");
    my $request = HTTP::Request->new("GET", $self->{gtfs_realtime_proto});
    my $to = $self->ua->timeout();
    $self->ua->timeout(5);
    my $response = $self->ua->request($request);
    $self->ua->timeout($to);
    if (!$response->is_success()) {
	die(sprintf("Failed to pull protocol: %s\n", $response->status_line()));
    }
    my $proto = $response->content();
    if (!defined $proto) {
	die("Failed to pull protocol: undefined content\n");
    }
    if (!$proto) {
	die("Failed to pull protocol: no content\n");
    }
    $self->warn_1("    Parsing...\n");
    Google::ProtocolBuffers->parse($proto);
    $self->warn_1("    Done.\n");
    $self->{gtfs_realtime_protocol_pulled} = 1;
}

sub process_gtfs_realtime_data {
    my ($self, $request, $response) = @_;
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

    my $o;

    if ($response->content_type eq "application/protobuf") {
        $self->pull_gtfs_realtime_protocol();
        $o = TransitRealtime::FeedMessage->decode($$cref);
    } elsif ($response->content_type eq "application/json") {
        $o = $self->json->decode($response->decoded_content);
    }

    my $header_timestamp = $o->{header}->{timestamp};
    my $base_filename = strftime("%Y/%m/%d/%H%M%SZ", gmtime($header_timestamp // $last_modified));

    my $gtfs2 = $self->{gtfs2};

    my $agency_dir_name = $gtfs2->{geo_gtfs_agency_name};
    my $dir = $gtfs2->{dir};

    my $pb_filename     = sprintf("%s/data/%s/pb/%s/%s.pb",     $dir, $agency_dir_name, $feed_type, $base_filename);
    my $rel_pb_filename = sprintf(   "data/%s/pb/%s/%s.pb",           $agency_dir_name, $feed_type, $base_filename);
    my $json_filename   = sprintf("%s/data/%s/json/%s/%s.json", $dir, $agency_dir_name, $feed_type, $base_filename);

    stat($pb_filename);
    if (!($cached && -e _ && defined $content_length && $content_length == (stat(_))[7])) {
        if ($response->content_type eq "application/protobuf" && $self->{write_pb}) {
            $self->write_pb($pb_filename, $cref);
        }
        if ($self->{write_json}) {
            $self->write_json($json_filename, $o);
        }
    }
    if ($gtfs2) {
        $gtfs2->{geo_gtfs_realtime_feed_id} =
            $gtfs2->db->select_or_insert_geo_gtfs_realtime_feed_id($gtfs2->{geo_gtfs_agency_id}, $url, $feed_type);
        $gtfs2->{geo_gtfs_realtime_feed_instance_id} =
            $gtfs2->db->select_or_insert_geo_gtfs_realtime_feed_instance_id($gtfs2->{geo_gtfs_realtime_feed_id},
                                                                            $rel_pb_filename,
                                                                            $retrieved,
                                                                            $last_modified,
                                                                            $header_timestamp);
        warn(sprintf("process_gtfs_realtime_data: geo_gtfs_realtime_feed_id is %s\n", $gtfs2->{geo_gtfs_realtime_feed_id}));
        warn(sprintf("process_gtfs_realtime_data: geo_gtfs_realtime_feed_instance_id is %s\n", $gtfs2->{geo_gtfs_realtime_feed_instance_id}));
        $gtfs2->db->dbh->commit;
    }
    return $o;
}

sub write_pb {
    my ($self, $filename, $cref) = @_;
    make_path(dirname($filename));
    if (open(my $fh, ">", $filename)) {
        $self->warn_1("Writing $filename ...\n");
        binmode($fh);
        print {$fh} $$cref;
        close($fh);
        $self->warn_1("Done.\n");
    } else {
        die("Cannot write $filename: $!\n");
    }
}

sub write_json {
    my ($self, $filename, $o) = @_;
    make_path(dirname($filename));
    if (open(my $fh, ">", $filename)) {
        $self->warn_1("Writing $filename ...\n");
        binmode($fh);
        print {$fh} $self->json->encode($o);
        close($fh);
        $self->warn_1("Done.\n");
    } else {
        die("Cannot write $filename: $!\n");
    }
}

sub get_vehicle_feed {
    my ($self) = @_;
    my $o = $self->fetch_realtime_all_data_feed();
    if (-t 1 && -t 2) {
        print(Dumper($o));
    }
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
	my ($geo_gtfs_feed_instance_id, $service_id) = $self->db->get_geo_gtfs_feed_instance_id_and_service_id($self->{gtfs2}->{geo_gtfs_agency_id}, $start_date);
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
    
    my ($geo_gtfs_feed_instance_id, $service_id) = $self->db->get_geo_gtfs_feed_instance_id_and_service_id($self->{gtfs2}->{geo_gtfs_agency_id}, $tu->{start_date});
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
    my $feed = $self->db->get_geo_gtfs_realtime_feed_by_type($self->{gtfs2}->{geo_gtfs_agency_id}, "all");
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

sub ua {
    my ($self) = @_;
    return $self->{gtfs2}->ua;
}

sub json {
    my ($self) = @_;
    return $self->{gtfs2}->json;
}

sub db {
    my ($self) = @_;
    return $self->{gtfs2}->db;
}

1;                              # End of Geo::GTFS2::Realtime
