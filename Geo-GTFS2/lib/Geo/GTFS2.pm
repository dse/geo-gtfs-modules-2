package Geo::GTFS2;
use strict;
use warnings;

BEGIN {
    foreach my $dir ("/home/dse/git/HTTP-Cache-Transparent/lib",
		     "/Users/dse/git/HTTP-Cache-Transparent/lib") {
	unshift(@INC, $dir) if -d $dir;
    }
    # my fork adds a special feature called NoUpdateImpatient.
}

use DBI;
use Data::Dumper;
use File::Basename qw(dirname basename);
use File::MMagic;		# best detects .zip files; allows us
                                # to add magic for Google Protocol
                                # Buffers files.
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

use fields qw(dir
	      sqlite_filename
	      dbh
	      ua
	      magic
	      http_cache_dir
	      gtfs_realtime_proto
	      json
	      gtfs_realtime_protocol_pulled
	      db);

sub new {
    my ($class, %args) = @_;
    my $self = fields::new($class);
    $self->init(%args);
    return $self;
}

sub init {
    my ($self, %args) = @_;

    my $HOME = $ENV{HOME} // (getpwent())[7];
    my $dir = $self->{dir} = "$HOME/.geo-gtfs2";

    $self->{http_cache_dir} = "$dir/http-cache";
    my $dbfile = $self->{sqlite_filename} = "$dir/google_transit.sqlite";
    while (my ($k, $v) = each(%args)) {
	$self->{$k} = $v;
    }
    $self->{magic} = File::MMagic->new();
    $self->{magic}->addMagicEntry("0\tstring\t\\x0a\\x0b\\x0a\\x03\tapplication/x-protobuf");
    $self->{gtfs_realtime_proto} = "https://developers.google.com/transit/gtfs-realtime/gtfs-realtime.proto";
    $self->{gtfs_realtime_protocol_pulled} = 0;
}

###############################################################################
# GENERAL
###############################################################################

sub process_url {
    my ($self, $geo_gtfs_agency_name, $url) = @_;
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
	return $self->process_gtfs_feed($geo_gtfs_agency_name, $request, $response);
    } elsif ($response->content_type eq "application/protobuf") {
	return $self->process_protocol_buffers($geo_gtfs_agency_name, $request, $response);
    } elsif ($response->base =~ m{\.zip$}i) {
	return $self->process_gtfs_feed($geo_gtfs_agency_name, $request, $response);
    } elsif ($response->base =~ m{\.pb$}i) {
	return $self->process_protocol_buffers($geo_gtfs_agency_name, $request, $response);
    } elsif ($url =~ m{\.zip$}i) {
	return $self->process_gtfs_feed($geo_gtfs_agency_name, $request, $response);
    } elsif ($url =~ m{\.pb$}i) {
	return $self->process_protocol_buffers($geo_gtfs_agency_name, $request, $response);
    } else {
	return $self->process_not_yet_known_content($geo_gtfs_agency_name, $request, $response);
    }
}

sub process_not_yet_known_content {
    my ($self, $geo_gtfs_agency_name, $request, $response) = @_;
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
    my ($self, $geo_gtfs_agency_name, $request, $response) = @_;
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
    my $pb_filename     = sprintf("%s/data/%s/pb/%s/%s.pb",     $self->{dir}, $geo_gtfs_agency_name, $feed_type, $base_filename);
    my $rel_pb_filename = sprintf(   "data/%s/pb/%s/%s.pb",                   $geo_gtfs_agency_name, $feed_type, $base_filename);
    my $json_filename   = sprintf("%s/data/%s/json/%s/%s.json", $self->{dir}, $geo_gtfs_agency_name, $feed_type, $base_filename);

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

    my $geo_gtfs_agency_id = $self->db->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
    my $geo_gtfs_realtime_feed_id = $self->db->select_or_insert_geo_gtfs_realtime_feed_id($geo_gtfs_agency_id, $url, $feed_type);
    my $geo_gtfs_realtime_feed_instance_id =
      $self->db->select_or_insert_geo_gtfs_realtime_feed_instance_id($geo_gtfs_realtime_feed_id,
								 $rel_pb_filename,
								 $retrieved,
								 $last_modified,
								 $header_timestamp);
}

sub update_realtime {
    my ($self, $geo_gtfs_agency_name) = @_;
    my $geo_gtfs_agency_id = $self->db->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
    my @feeds = $self->db->get_geo_gtfs_realtime_feeds($geo_gtfs_agency_id);
    foreach my $feed (@feeds) {
	my $url = $feed->{url};
	$self->process_url($geo_gtfs_agency_name, $url);
    }
}

sub list_latest_realtime_feeds {
    my ($self, $geo_gtfs_agency_name) = @_;
    my $geo_gtfs_agency_id = $self->db->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
    my @instances = $self->db->get_latest_geo_gtfs_realtime_feed_instances($geo_gtfs_agency_id);
    print("id      feed type  filename\n");
    print("------  ---------  -------------------------------------------------------------------------------\n");
    foreach my $i (@instances) {
	printf("%6d  %-9s  %s\n",
	       @{$i}{qw(id feed_type filename)});
    }
}

sub realtime_status {
    my ($self, $geo_gtfs_agency_name, %args) = @_;
    warn("Querying database for agency ID, feed instance ID...\n");
    my $geo_gtfs_agency_id = $self->db->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
    my @instances = $self->db->get_latest_geo_gtfs_realtime_feed_instances($geo_gtfs_agency_id);
    warn("Done.\n");
    my %instances = map { ($_->{feed_type}, $_) } @instances;

    my @vp;
    my @tu;
    my @alerts;

    my $process_entity = sub {
	my ($e) = @_;
	my $alert = $e->{alert};
	my $vp    = $e->{vehicle};
	my $tu    = $e->{trip_update};
	if (defined $e->{header_timestamp}) {
	    $alert->{header_timestamp} = $e->{header_timestamp} if $alert;
	    $vp   ->{header_timestamp} = $e->{header_timestamp} if $vp   ;
	    $tu   ->{header_timestamp} = $e->{header_timestamp} if $tu   ;
	}
	push(@vp, $vp)       if $vp;
	push(@tu, $tu)       if $tu;
	push(@alerts, $alert) if $alert;
    };

    warn("Pass 1...\n");
    if ($instances{all}) {
	warn("  Reading comprehensive realtime feed $instances{all}{filename}...\n");
	my $o = $self->read_realtime_feed($instances{all}{filename});
	warn("  Processing entities...\n");
	my $header_timestamp = eval { $o->{header}->{timestamp} };
	foreach my $e (@{$o->{entity}}) {
	    $e->{header_timestamp} = $header_timestamp if defined $header_timestamp;
	    $process_entity->($e);
	}
	warn("  Done.\n");
    } else {
	foreach my $feed_type (qw(alerts updates positions)) {
	    warn("  Reading $feed_type realtime feed...\n");
	    my $i = $instances{$feed_type};
	    if ($i) {
		my $o = $self->read_realtime_feed($instances{all}{filename});
		my $header_timestamp = eval { $o->{header}->{timestamp} };
		foreach my $e (@{$o->{entity}}) {
		    $e->{header_timestamp} = $header_timestamp if defined $header_timestamp;
		    $process_entity->($e);
		}
	    }
	    warn("  Done.\n");
	}
    }
    warn("Done.\n");

    warn("Processing status info...\n");
    my $status_info = $self->get_useful_realtime_status_info($geo_gtfs_agency_id, \@vp, \@tu, \@alerts);
    my @info = @{$status_info->{info}};

    print("                                                                                                                 sched.   realtime      \n");
    print("veh.  lat.      lng.       route                                  time          stop                             dep/arr  dep/arr  delay\n");
    print("----- --------- ---------- ----- -------------------------------- --------      -------------------------------- -------- -------- -----\n");

    my $sprintf_info_line = sub {
	my ($info, $route, $trip) = @_;
	return sprintf("%-5s %9.5f %10.5f %5s %-32.32s %-8s",
		       $info->{label} // "-",
		       $info->{latitude} // 0,
		       $info->{longitude} // 0,
		       eval { $info->{route}->{route_short_name} } // "-",
		       eval { $info->{trip}->{trip_headsign}     } // "-",
		       $info->{timestamp_time} // "-",
		      );
    };

    my $sprintf_stuinfo_line = sub {
	my ($stuinfo, $info, $route, $trip) = @_;
	return sprintf("%3s %-32.32s %-8s %-8s %5d",
		       $stuinfo->{is_coming_stop_time_update} ? "***" : "",
		       $stuinfo->{stop_name} // "-",
		       $stuinfo->{scheduled_departure_time} // $stuinfo->{scheduled_arrival_time} // "-",
		       defined $stuinfo->{time} ? strftime("%H:%M:%S", localtime($stuinfo->{time})) : "-",
		       int(($stuinfo->{delay} // 0) / 60 + 0.5));
    };

    my @current = grep { $_->{old} == 0 } @info;
    my @dad     = grep { $_->{old} == 1 } @info;
    my @cooldad = grep { $_->{old} == 2 } @info;

    foreach my $info (@current) {
	my $route = $info->{route};
	my $trip  = $info->{trip};
	my $line = $sprintf_info_line->($info, $route, $trip);
	
	print($line);

	if ($args{summary}) {
	    my $idx = $info->{coming_stop_time_update_index};
	    my $stuinfo = (defined $info->{stop_time_update} && defined $idx) ? $info->{stop_time_update}->[$idx] : undef;
	    if ($stuinfo) {
		print("  ");
		print $sprintf_stuinfo_line->($stuinfo, $info, $route, $trip);
	    } else {
		goto full_stop_time_update_info;
	    }
	    print("\n");
	} else {
	  full_stop_time_update_info:
	    my $first = 1;
	    foreach my $stuinfo (eval { @{$info->{stop_time_update}} }) {
		if ($first) {
		    print("  ");
		} else {
		    print(" " x (length($line) + 2));
		}
		print $sprintf_stuinfo_line->($stuinfo, $info, $route, $trip);
		print "\n";
		$first = 0;
	    }
	    if (!$info->{stop_time_update} || !scalar(@{$info->{stop_time_update}})) {
		print("\n");
	    }
	}
    }
}

sub get_useful_realtime_status_info {
    my ($self, $geo_gtfs_agency_id, $vp_array, $tu_array, $alerts_array) = @_;
    
    my %vehicle_info;
    my %trip_info;
    my @info;

    foreach my $vp (@$vp_array) {
	my $header_timestamp = eval { $vp->{header_timestamp} };
	my $timestamp        = eval { $vp->{timestamp} };
	my $trip_id          = eval { $vp->{trip}->{trip_id} };
	my $label            = eval { $vp->{vehicle}->{label} };
	my $latitude         = eval { $vp->{position}->{latitude} };
	my $longitude        = eval { $vp->{position}->{longitude} };

	my $info = {};
	push(@info, $info);
	$info->{timestamp}        = $timestamp	      if defined $timestamp;
	$info->{header_timestamp} = $header_timestamp if defined $header_timestamp;
	$info->{trip_id}	  = $trip_id	      if defined $trip_id;
	$info->{label}		  = $label	      if defined $label;
	$info->{longitude}	  = $longitude	      if defined $longitude;
	$info->{latitude}	  = $latitude	      if defined $latitude;

	if (defined $timestamp) {
	    $info->{timestamp_date} = strftime("%m/%d",    localtime($timestamp));
	    $info->{timestamp_time} = strftime("%H:%M:%S", localtime($timestamp));
	}
	if (defined $header_timestamp) {
	    $info->{header_timestamp_date} = strftime("%m/%d",    localtime($header_timestamp));
	    $info->{header_timestamp_time} = strftime("%H:%M:%S", localtime($header_timestamp));
	}

	$vehicle_info{$label} = $info if defined $label;
	$trip_info{$trip_id} = $info if defined $trip_id;
    }

    foreach my $tu (@$tu_array) {
	my $trip_id         = eval { $tu->{trip}->{trip_id} };
	my $start_time      = eval { $tu->{trip}->{start_time} };
	my $route_id        = eval { $tu->{trip}->{route_id} };
	my $start_date      = eval { $tu->{trip}->{start_date} };
	my $label           = eval { $tu->{vehicle}->{label} };

	my $vehicle_info = $vehicle_info{$label};
	my $trip_info    = $trip_info{$trip_id};
	if ($vehicle_info && $trip_info && $vehicle_info ne $trip_info) {
	    die("UNEXPECTED ERROR TYPE 2\n");
	}
	if (!$vehicle_info && !$trip_info) {
	    next;
	}
	my $info = $vehicle_info // $trip_info;
	if (!$info) {
	    $info = {};
	    push(@info, $info);
	    if (defined $trip_id) {
		$trip_info{$trip_id} = $info;
		$info->{trip_id} = $trip_id;
	    }
	    if (defined $label) {
		$vehicle_info{$label} = $info;
		$info->{label} = $label;
	    }
	}

	$info->{start_time} = $start_time if defined $start_time;
	$info->{start_date} = $start_date if defined $start_date;
	$info->{route_id}   = $route_id   if defined $route_id;

	my @stu = eval { @{$tu->{stop_time_update}} };
	my $stu_idx = 0;
	my $coming_stop_time_update_index;
	foreach my $stu (@stu) {
	    my $stuinfo = {};
	    my $stop_sequence = $stu->{stop_sequence};
	    my $stop_id = $stu->{stop_id};
	    my $departure_time  = eval { $stu->{departure}->{time} }  // eval { $stu->{artival}->{time}  };
	    my $departure_delay = eval { $stu->{departure}->{delay} } // eval { $stu->{artival}->{delay} };
	    my $is_arrival      = ($stu->{arrival} && !$stu->{departure}) ? 1 : undef;
	    $stuinfo->{stop_sequence} = $stop_sequence   if defined $stop_sequence;
	    $stuinfo->{stop_id}       = $stop_id         if defined $stop_id;
	    $stuinfo->{time}          = $departure_time  if defined $departure_time;
	    $stuinfo->{delay}         = $departure_delay if defined $departure_delay;
	    $stuinfo->{is_arrival}    = $is_arrival      if defined $is_arrival;
	    my $notion_of_current_time = $info->{header_timestamp} // $info->{timestamp};
	    if (scalar(keys(%$stuinfo))) {
		push(@{$info->{stop_time_update}}, $stuinfo);
		if (!defined $coming_stop_time_update_index) {
		    if (defined $notion_of_current_time && defined $stuinfo->{time} &&
			  $notion_of_current_time <= $stuinfo->{time}) {
			$info->{coming_stop_time_update_index} = $coming_stop_time_update_index = $stu_idx;
			$stuinfo->{is_coming_stop_time_update} = 1;
		    }
		}
		$stu_idx += 1;
	    }
	}
    }

    foreach my $info (@info) {
	my $age = $info->{header_timestamp} - $info->{timestamp};
	$info->{age} = $age;
	if ($age >= 3600) {
	    $info->{old} = 2;
	} elsif ($age >= 600) {
	    $info->{old} = 1;
	} else {
	    $info->{old} = 0;
	}

	if (!defined $info->{start_date}) {
	    $info->{old} = 3;
	    next;
	}

	my ($geo_gtfs_feed_instance_id, $service_id)
	  = $self->db->get_geo_gtfs_feed_instance_id_and_service_id($geo_gtfs_agency_id, $info->{start_date});

	$info->{geo_gtfs_feed_instance_id} = $geo_gtfs_feed_instance_id;
	$info->{service_id}                = $service_id;
	if (defined $info->{route_id}) {
	    my $route = $info->{route} = $self->db->get_gtfs_route($geo_gtfs_feed_instance_id, $info->{route_id});
	    if (defined $route) {
		$info->{route_short_name} = $route->{route_short_name};
		$info->{route_long_name}  = $route->{route_long_name};
		$info->{route_desc}       = $route->{route_desc};
		$info->{route_type}       = $route->{route_type};
		$info->{route_url}        = $route->{route_url};
		$info->{route_color}      = $route->{route_color};
		$info->{route_text_color} = $route->{route_text_color};
	    }
	}
	if (defined $info->{trip_id}) {
	    my $trip = $info->{trip} = $self->db->get_gtfs_trip($geo_gtfs_feed_instance_id, $info->{route_id}, $service_id, $info->{trip_id});
	    if (defined $trip) {
		$info->{wheelchair_accessible} = $trip->{wheelchair_accessible};
		$info->{trip_headsign}         = $trip->{trip_headsign};
		$info->{trip_short_name}       = $trip->{trip_short_name};
		$info->{direction_id}          = $trip->{direction_id};
		$info->{block_id}              = $trip->{block_id};
		$info->{shape_id}              = $trip->{shape_id};
		$info->{bikes_allowed}         = $trip->{bikes_allowed};
	    }
	}
	if ($info->{stop_time_update}) {
	    foreach my $stuinfo (@{$info->{stop_time_update}}) {
		my $stop_id = $stuinfo->{stop_id};
		my $trip_id = $info->{trip_id};
		if (defined $stop_id) {
		    my $stop = $self->db->get_gtfs_stop($geo_gtfs_feed_instance_id, $stop_id);
		    if ($stop) {
			$stuinfo->{stop_code}		= $stop->{stop_code}		if defined $stop->{stop_code};
			$stuinfo->{stop_name}		= $stop->{stop_name}		if defined $stop->{stop_name};
			$stuinfo->{stop_desc}		= $stop->{stop_desc}		if defined $stop->{stop_desc};
			$stuinfo->{stop_lat}		= $stop->{stop_lat}		if defined $stop->{stop_lat};
			$stuinfo->{stop_lon}		= $stop->{stop_lon}		if defined $stop->{stop_lon};
			$stuinfo->{zone_id}		= $stop->{zone_id}		if defined $stop->{zone_id};
			$stuinfo->{stop_url}		= $stop->{stop_url}		if defined $stop->{stop_url};
			$stuinfo->{location_type}	= $stop->{location_type}	if defined $stop->{location_type};
			$stuinfo->{parent_station}	= $stop->{parent_station}	if defined $stop->{parent_station};
			$stuinfo->{stop_timezone}	= $stop->{stop_timezone}	if defined $stop->{stop_timezone};
			$stuinfo->{wheelchair_boarding}	= $stop->{wheelchair_boarding}	if defined $stop->{wheelchair_boarding};
		    }
		    if (defined $trip_id) {
			my $stop_time = $self->db->get_gtfs_stop_time($geo_gtfs_feed_instance_id, $stop_id, $trip_id);
			if ($stop_time) {
			    $stuinfo->{scheduled_arrival_time}   = $stop_time->{arrival_time}		if defined $stop_time->{arrival_time};
			    $stuinfo->{scheduled_departure_time} = $stop_time->{departure_time}		if defined $stop_time->{departure_time};
			    $stuinfo->{scheduled_stop_sequence}  = $stop_time->{stop_sequence}		if defined $stop_time->{stop_sequence};
			    $stuinfo->{stop_headsign}		 = $stop_time->{stop_headsign}		if defined $stop_time->{stop_headsign};
			    $stuinfo->{pickup_type}		 = $stop_time->{pickup_type}		if defined $stop_time->{pickup_type};
			    $stuinfo->{drop_off_type}		 = $stop_time->{drop_off_type}		if defined $stop_time->{drop_off_type};
			    $stuinfo->{shape_dist_traveled}	 = $stop_time->{shape_dist_traveled}	if defined $stop_time->{shape_dist_traveled};
			}
		    }
		}
	    }
	}
    }

    @info = (map { $_->[0] }
	       sort { _route_id_sort($a->[1], $b->[1]) || $a->[2] <=> $b->[2] }
		 map { [$_,
			$_->{route_id} // "",
			$_->{direction_id} // 0] }
		   @info);
    return {
	vp           => $vp_array,
	tu           => $tu_array,
	alerts       => $alerts_array,
	vehicle_info => \%vehicle_info,
	trip_info    => \%trip_info,
	info         => \@info,
    };
}

sub _route_id_sort {
    my ($stringA, $stringB) = @_;
    if ($stringA =~ m{\d+}) {
	my ($prefixA, $numberA, $suffixA) = ($`, $&, $');
	if ($stringB =~ m{\d+}) {
	    my ($prefixB, $numberB, $suffixB) = ($`, $&, $');
	    if ($prefixA eq $prefixB) {
		return ($numberA <=> $numberB) || _route_id_sort($suffixA, $suffixB);
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
    my ($self, $geo_gtfs_agency_name) = @_;
    my $geo_gtfs_agency_id = $self->db->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
    my @feeds = $self->db->get_geo_gtfs_realtime_feeds($geo_gtfs_agency_id);
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
    my ($self, $geo_gtfs_agency_name, $request, $response) = @_;
    my $url = $response->base;
    my $cached = ($response->code == 304 || ($response->header("X-Cached") && $response->header("X-Content-Unchanged")));
    my $cref = $response->content_ref;

    my $retrieved     = $response->date;
    my $last_modified = $response->last_modified;
    my $content_length = $response->content_length;

    my $md5 = md5_hex($url);

    my $zip_filename     = sprintf("%s/data/%s/gtfs/%s-%s-%s-%s.zip", $self->{dir}, $geo_gtfs_agency_name, $md5, $retrieved, $last_modified, $content_length);
    my $rel_zip_filename = sprintf(   "data/%s/gtfs/%s-%s-%s-%s.zip",               $geo_gtfs_agency_name, $md5, $retrieved, $last_modified, $content_length);
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

    my $geo_gtfs_agency_id =
      $self->db->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
    my $geo_gtfs_feed_id =
      $self->db->select_or_insert_geo_gtfs_feed_id($geo_gtfs_agency_id, $url);
    my $geo_gtfs_feed_instance_id =
      $self->db->select_or_insert_geo_gtfs_feed_instance_id($geo_gtfs_feed_id,
							$rel_zip_filename,
							$retrieved,
							$last_modified);

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

	my $sql = sprintf("insert into $table_name(geo_gtfs_feed_instance_id, %s) " .
			    "values(?, %s);",
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

sub update {
    my ($self, $geo_gtfs_agency_name) = @_;
    my $geo_gtfs_agency_id = $self->db->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
}

sub list_routes {
    my ($self, $geo_gtfs_agency_name) = @_;
    my $geo_gtfs_agency_id = $self->db->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
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
# COMMAND LINE UTILITY FUNCTIONALITY
###############################################################################

sub help_cmdline { print(<<"END"); }
  gtfs2 ridetarc.org <URL> ...
  gtfs2 ridetarc.org update [<URL> ...]
  gtfs2 ridetarc.org update-realtime
  gtfs2 ridetarc.org realtime-status
  gtfs2 list-agencies
  gtfs2 ridetarc.org list-routes
END

sub run_cmdline {
    my ($self, @args) = @_;
    if (!@args) {
	$self->help_cmdline();
    } elsif ($self->is_agency_name($args[0])) {
	my $geo_gtfs_agency_name = shift(@args);
	if (!@args) {
	    my ($geo_gtfs_agency_id, $status) = $self->db->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
	    printf("%8d %-8s %s\n", $geo_gtfs_agency_id, $status, $geo_gtfs_agency_name);
	} elsif ($self->is_url($args[0])) {
	    foreach my $arg (@args) {
		if ($self->is_url($arg)) {
		    $self->process_url($geo_gtfs_agency_name, $arg);
		} else {
		    warn("Unknown argument: $arg\n");
		}
	    }
	} elsif ($args[0] eq "update") {
	    $self->update($geo_gtfs_agency_name);
	} elsif ($args[0] eq "list-realtime-feeds") {
	    $self->list_realtime_feeds($geo_gtfs_agency_name);
	} elsif ($args[0] eq "update-realtime") {
	    $self->update_realtime($geo_gtfs_agency_name);
	} elsif ($args[0] eq "realtime-status") {
	    $self->realtime_status($geo_gtfs_agency_name);
	} elsif ($args[0] eq "realtime-summary") {
	    $self->realtime_status($geo_gtfs_agency_name, summary => 1);
	} elsif ($args[0] eq "list-routes") {
	    $self->list_routes($geo_gtfs_agency_name);
	} elsif ($args[0] eq "sqlite") {
	    $self->exec_sqlite_utility();
	}
    } elsif ($args[0] eq "list-agencies") {
	$self->list_agencies();
    } elsif ($args[0] eq "help") {
	$self->help_cmdline();
    } elsif ($args[0] eq "sqlite") {
	$self->exec_sqlite_utility();
    } else {
	die("Unknown command: $args[0]\n");
    }
}

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
# GENERIC UTILITY
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

###############################################################################

1; # End of Geo::GTFS2
