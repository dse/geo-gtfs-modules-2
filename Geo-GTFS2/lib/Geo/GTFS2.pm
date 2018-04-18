package Geo::GTFS2;
use strict;
use warnings;

use base "Geo::GTFS2::Object";

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
use POSIX qw(strftime);
use Text::CSV;

# [1] File::MMagic best detects .zip files, and allows us to add magic
# for Google Protocol Buffers files.

BEGIN {
    our @FIELDS = qw(dir
                     http_cache_dir
                     sqlite_filename
                     dbh
                     db
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

                     write_json

                     geo_gtfs_agency_name
                     geo_gtfs_agency_id
                     geo_gtfs_feed_id
                     geo_gtfs_realtime_feed_id
                     geo_gtfs_realtime_feed_instance_id);
}

sub init {
    my ($self, %args) = @_;
    $self->SUPER::init(%args);

    my $dir = $self->{dir};
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
    } elsif ($url =~ m{\.json$}) {
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
    } elsif ($response->content_type eq "application/json") {
	return $self->process_gtfs_realtime_data($request, $response);
    } elsif ($response->content_type eq "application/protobuf") {
	return $self->process_gtfs_realtime_data($request, $response);
    } elsif ($response->base =~ m{\.zip$}i) {
	return $self->process_gtfs_feed($request, $response);
    } elsif ($response->base =~ m{\.pb$}i) {
	return $self->process_gtfs_realtime_data($request, $response);
    } elsif ($response->base =~ m{\.json$}i) {
	return $self->process_gtfs_realtime_data($request, $response);
    } elsif ($url =~ m{\.zip$}i) {
	return $self->process_gtfs_feed($request, $response);
    } elsif ($url =~ m{\.pb$}i) {
	return $self->process_gtfs_realtime_data($request, $response);
    } elsif ($url =~ m{\.json$}i) {
	return $self->process_gtfs_realtime_data($request, $response);
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
	return $self->process_gtfs_realtime_data($request, $response);
    } elsif ($type eq "application/json") {
	return $self->process_gtfs_realtime_data($request, $response);
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

sub force_pull_gtfs_realtime_protocol {
    my ($self) = @_;
    $self->pull_gtfs_realtime_protocol(force => 1);
}

sub pull_gtfs_realtime_protocol {
    my ($self, %options) = @_;
    if (!$options{force}) {
        return 1 if $self->{gtfs_realtime_protocol_pulled};
    }
    HTTP::Cache::Transparent::init({ BasePath => $self->{http_cache_dir},
				     Verbose => 1,
				     $options{force} ? () : (NoUpdate => 86400),
                                     $options{force} ? (MaxAge => 0) : (),
				     $options{force} ? () : (UseCacheOnTimeout => 1),
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
    } else {
        die("Unrecognized MIME type " . $response->content_type . "\n");
    }

    my $header_timestamp = $o->{header}->{timestamp};
    my $base_filename = strftime("%Y/%m/%d/%H%M%SZ", gmtime($header_timestamp // $last_modified));
    my $pb_filename     = sprintf("%s/data/%s/pb/%s/%s.pb",     $self->{dir}, $self->{geo_gtfs_agency_name}, $feed_type, $base_filename);
    my $rel_pb_filename = sprintf(   "data/%s/pb/%s/%s.pb",                   $self->{geo_gtfs_agency_name}, $feed_type, $base_filename);
    my $json_filename   = sprintf("%s/data/%s/json/%s/%s.json", $self->{dir}, $self->{geo_gtfs_agency_name}, $feed_type, $base_filename);

    stat($pb_filename);
    if (!($cached && -e _ && defined $content_length && $content_length == (stat(_))[7])) {
        if ($self->{write_pb}) {
            $self->write_pb($pb_filename, $cref);
        }
        if ($self->{write_json}) {
            $self->write_json($json_filename, $o);
        }
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

sub write_pb {
    my ($self, $filename, $cref) = @_;
    make_path(dirname($filename));
    if (open(my $fh, ">", $filename)) {
        warn("Writing $filename ...\n");
        binmode($fh);
        print {$fh} $$cref;
    } else {
        die("Cannot write $filename: $!\n");
    }
}

sub write_json {
    my ($self, $filename, $o) = @_;
    make_path(dirname($filename));
    if (open(my $fh, ">", $filename)) {
        warn("Writing $filename ...\n");
        binmode($fh);
        print {$fh} $self->json->encode($o);
    } else {
        die("Cannot write $filename: $!\n");
    }
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
	my $line = $fh->getline();
	$line =~ s{[\r\n]+$}{};
	$csv->parse($line);
	my $fields = [$csv->fields()];
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
	while (defined(my $line = $fh->getline())) {
	    $line =~ s{[\r\n]+$}{};
	    $csv->parse($line);
	    my $data = [$csv->fields()];
	    if (scalar(@$data) < scalar(@$fields)) {
		next;
	    }
	    $sth->execute($geo_gtfs_feed_instance_id, @$data);
	    $rows += 1;
	}
	print STDERR ("$rows rows inserted.\n");
    }
    $self->dbh->commit();

    $| = $save_flush;
    select($save_select);
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
    my $sth = $self->dbh->prepare(<<"END");
        select geo_gtfs_agency.id as id,
               geo_gtfs_agency.name as name,
               count(*) as feed_count
        from geo_gtfs_agency
             left join geo_gtfs_feed on geo_gtfs_agency.id = geo_gtfs_feed.geo_gtfs_agency_id
        group by geo_gtfs_agency.id
END
    $sth->execute();
    print("id    feed_count  name\n");
    print("----  ----------  --------------------------------\n");
    while (my $row = $sth->fetchrow_hashref()) {
	printf("%4d  %10d  %s\n", $row->{id}, $row->{feed_count}, $row->{name});
    }
}

sub list_feeds {
    my ($self) = @_;
    my $sth = $self->dbh->prepare(<<"END");
	select
                f.url as url,
                i.last_modified as last_modified,
                a.name as agency_name
        from geo_gtfs_feed f
            left join geo_gtfs_feed_instance i on f.id = i.geo_gtfs_feed_id
            left join geo_gtfs_agency a on f.geo_gtfs_agency_id = a.id
        order by agency_name asc, last_modified desc
END
    $sth->execute();
    print("agency_name       last_modified  url\n");
    print("----------------  -------------  ----------------------------------------------\n");
    while (my $row = $sth->fetchrow_hashref()) {
	printf("%-16s  %-13s  %s\n", @{$row}{qw(agency_name last_modified url)});
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
};

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

sub ua {
    my ($self) = @_;
    return $self->{ua} //= LWP::UserAgent->new();
}

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

1;                              # End of Geo::GTFS2
