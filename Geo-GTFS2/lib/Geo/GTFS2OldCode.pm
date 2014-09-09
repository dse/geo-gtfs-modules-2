###############################################################################
# OLD CODE
###############################################################################

sub get_alerts_old {
    my ($self) = @_;
    my @results = $self->get_records("alert");
    return @results;
}

sub get_vehicle_positions_old {
    my ($self) = @_;
    my @results = $self->get_records("vehicle");
    $self->{vehicle_positions} = {};
    foreach my $result (@results) {
	my $label = eval { $result->{vehicle}->{label} };
	next unless defined $label;
	$self->{vehicle_positions}->{$label} = $result;
    }
    $self->{vehicle_positions_array} = \@results;
    return @results;
}

sub get_trip_updates_old {
    my ($self) = @_;
    my @results = $self->get_records("trip_update");
    $self->{trip_updates} = {};
    foreach my $result (@results) {
	my $trip_id = eval { $result->{trip}->{trip_id} };
	next unless defined $trip_id;
	$self->{trip_updates}->{$trip_id} = $result;
    }
    $self->{trip_updates_array} = \@results;
    return @results;
}

sub realtime_status_old {
    my ($self, %args) = @_;

    my @vp     = $self->get_vehicle_positions_old();
    my @tu     = $self->get_trip_updates_old();
    my @alerts = $self->get_alerts_old();

    if ($args{raw}) {
	foreach my $vp (@vp) {
	    print(Dumper($vp));
	}
	foreach my $tu (@tu) {
	    print(Dumper($tu));
	}
	foreach my $alert (@alerts) {
	    print(Dumper($alert));
	}
	return;
    }

    warn("Processing status info...\n");
    my $status_info = $self->get_useful_realtime_status_info_old($self->{geo_gtfs_agency_id}, \@vp, \@tu, \@alerts);
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

sub get_useful_realtime_status_info_old {
    my ($self, $vp_array, $tu_array, $alerts_array) = @_;
    
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
    }

    foreach my $tu (@$tu_array) {
	my $trip_id         = eval { $tu->{trip}->{trip_id} };
	my $start_time      = eval { $tu->{trip}->{start_time} };
	my $route_id        = eval { $tu->{trip}->{route_id} };
	my $start_date      = eval { $tu->{trip}->{start_date} };
	my $label           = eval { $tu->{vehicle}->{label} };

	my $vehicle_info = $self->{vehicle_positions}->{$label};
	my $trip_info    = $self->{trip_updates}->{$trip_id};

	if ($vehicle_info && $trip_info && (eval { $vehicle_info->{trip}->{trip_id} } ne $trip_id ||
					      eval { $trip_info->{vehicle}->{label} } ne $label)) {
	    warn(Dumper($vehicle_info));
	    warn(Dumper($trip_info));
	    die(__PACKAGE__ . ": UNEXPECTED ERROR TYPE 2\n");
	}
	if (!$vehicle_info && !$trip_info) {
	    next;
	}
	my $info = $vehicle_info // $trip_info;
	if (!$info) {
	    $info = {};
	    push(@info, $info);
	    if (defined $trip_id) {
		$self->{trip_updates}->{$trip_id} = $info;
		$info->{trip_id} = $trip_id;
	    }
	    if (defined $label) {
		$self->{vehicle_positions}->{$label} = $info;
		$info->{label} = $label;
	    }
	}

	$info->{start_time} = $start_time if defined $start_time;
	$info->{start_date} = $start_date if defined $start_date;
	$info->{route_id}   = $route_id   if defined $route_id;

	my @stu = eval { @{$tu->{stop_time_update}} };
	my $stu_src_idx = -1;
	my $coming_stop_time_update_index;
	foreach my $stu (@stu) {
	    $stu_src_idx += 1;	# 0 .. $#stu
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
	    my $here = 0;
	    if (scalar(keys(%$stuinfo))) {
		push(@{$info->{stop_time_update}}, $stuinfo);
		if (defined $notion_of_current_time && defined $stuinfo->{time} && $notion_of_current_time <= $stuinfo->{time}) {
		    $here = 1;
		} elsif ($stu_src_idx == $#stu) {
		    $here = 1;
		}
	    }
	    if ($here) {
		if (!defined $coming_stop_time_update_index) {
		    $info->{coming_stop_time_update_index} = $coming_stop_time_update_index = $#{$info->{stop_time_update}};
		    $stuinfo->{is_coming_stop_time_update} = 1;
		}
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
	  = $self->db->get_geo_gtfs_feed_instance_id_and_service_id($self->{geo_gtfs_agency_id}, $info->{start_date});
	$self->{geo_gtfs_feed_instance_id} = $geo_gtfs_feed_instance_id;

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
	    my $trip = $info->{trip} = $self->db->get_gtfs_trip($geo_gtfs_feed_instance_id, $info->{trip_id}, $info->{route_id}, $service_id);
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
	       sort { _route_id_cmp($a->[1], $b->[1]) || $a->[2] <=> $b->[2] }
		 map { [$_,
			$_->{route_id} // "",
			$_->{direction_id} // 0] }
		   @info);
    return {
	vp           => $vp_array,
	tu           => $tu_array,
	alerts       => $alerts_array,
	vehicle_info => $self->{vehicle_positions},
	trip_info    => $self->{trip_updates},
	info         => \@info,
    };
}

sub get_records {
    my ($self, $feed_type) = @_;

    my @instances = $self->db->get_latest_geo_gtfs_realtime_feed_instances($self->{geo_gtfs_agency_id});
    my %instances = map { ($_->{feed_type}, $_) } @instances;
    my @results;
    my $process_entity = sub {
	my ($e) = @_;
	my $record = $e->{$feed_type};
	return unless $record;
	$record->{header_timestamp} = $e->{header_timestamp} if $e;
	# $record->{source_feed_type} = $e->{source_feed_type} if $e;
	push(@results, $record);
    };

    my $source_feed_type;
    my $i = $instances{all};
    if ($i) {
	$source_feed_type = "all";
    } else {
	$i = $instances{$feed_type};
	$source_feed_type = $feed_type;
    }

    if ($i) {
	my $o = $self->{realtime_feed}->{$feed_type} = $self->read_realtime_feed($i->{filename});
	$self->{header} = $o->{header} if $feed_type eq "all";
	$self->{headers}->{$feed_type} = $o->{header};
	my $header_timestamp = eval { $o->{header}->{timestamp} };
	foreach my $e (@{$o->{entity}}) {
	    $e->{header_timestamp} = $header_timestamp if defined $header_timestamp;
	    # $e->{source_feed_type} = $source_feed_type;
	    $process_entity->($e);
	}
    }
    return @results;
}



sub fetch_all_realtime_data {
    my ($self) = @_;
    my @feeds = $self->db->get_geo_gtfs_realtime_feeds($self->{geo_gtfs_agency_id});
    my %feeds = map { ($_->{feed_type} => $_) } @feeds;
    if ($feeds{all}) {
	my $feed = $feeds{all};
	my $url = $feed->{url};
	$self->process_url($url);
    } else {
	$self->fetch_all_realtime_feeds();
    }
}

sub fetch_all_realtime_feeds {
    my ($self) = @_;
    my @feeds = $self->db->get_geo_gtfs_realtime_feeds($self->{geo_gtfs_agency_id});
    foreach my $feed (@feeds) {
	my $url = $feed->{url};
	$self->process_url($url);
    }
}

sub fetch_realtime_vehicle_position_feed {
    my ($self) = @_;
    my $feed = $self->db->get_geo_gtfs_realtime_feed_by_type($self->{geo_gtfs_agency_id}, "positions");
    my $url = $feed->{url};
    $self->process_url($url);
}

sub get_cooked_realtime_vehicle_position_feed {
    my ($self) = @_;
    my $o = $self->fetch_realtime_vehicle_position_feed;
    my @v;
    foreach my $e (@{$o->{entity}}) {
	my $v = $e->{vehicle};
	push(@v, $v);
    }
    $o->{vehicle} = \@v;
    delete $o->{entity};
    return $o;
}

sub populate_trip_update_record {
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

sub populate_stop_time_update_record {
    my ($self, $tu, $stu) = @_;
    $self->process_090_populate_stop_time_update($tu, $stu);
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

sub get_latest_fetched_data {
    my ($self) = @_;

    my @instances = $self->db->get_latest_geo_gtfs_realtime_feed_instances($self->{geo_gtfs_agency_id});
    my %instances = map { ($_->{feed_type}, $_) } @instances;
    my $all = $instances{all};
    return unless $all;

    my @instances_to_check;
    if ($instances{all}) {
	@instances_to_check = ($instances{all});
    } else {
	@instances_to_check = @instances;
    }

    $self->{vehicle_positions} = {};
    $self->{vehicle_positions_by_trip_id} = {};
    $self->{trip_updates} = {};
    $self->{vehicle_positions_array} = [];
    $self->{trip_updates_array} = [];

    my @o;

    foreach my $instance (@instances) {
	my $o = $self->read_realtime_feed($instance->{filename});
	if ($instance->{feed_type} eq "all") {
	    $self->{realtime_feed}->{all} = $o;
	}
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
	push(@o, $o);
    }

    if (wantarray) {
	return @o;
    }
    return \@o;
}

sub process_010_flatten_vehicle_positions {
    my ($self) = @_;
    foreach my $vp (@{$self->{vehicle_positions_array}}) {
	eval { $vp->{trip_id}   = delete $vp->{trip}->{trip_id}       if defined $vp->{trip}->{trip_id};       };
	eval { $vp->{latitude}       = delete $vp->{position}->{latitude}  if defined $vp->{position}->{latitude};  };
	eval { $vp->{longitude}       = delete $vp->{position}->{longitude} if defined $vp->{position}->{longitude}; };
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
	    $self->flatten_stop_time_update_record($stu);
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
    my $stage = $args{stage} // 9999;
    if (!$args{nofetch}) {
	$self->fetch_all_realtime_data();
    }

    my $o = $self->get_latest_fetched_data();
    if ($stage < 1) {
	return $o;
    }
    $self->process_010_flatten_vehicle_positions();
    $self->process_020_flatten_trip_updates();
    if ($stage < 2) {
	return $o;
    }
    $self->process_030_consolidate_records();
    if ($stage < 3) {
	return $o;
    }
    $self->process_040_mark_as_of_times();
    $self->process_050_remove_turds();
    $self->process_060_populate_trip_and_route_info();
    $self->process_070_mark_next_coming_stops();
    if ($args{limit_stop_time_updates}) {
	$self->process_080_remove_most_stop_time_update_data();
    }
    if ($stage < 4) {
	return $o;
    }
    $self->process_090_populate_stop_information();
    return $o;
}

sub print_trip_status_raw {
    my ($self, $trip_id) = @_;
    $self->get_realtime_status_data();
    my $tu = $self->{trip_updates}->{$trip_id};
    if (!$tu) {
	my @keys = keys %{$self->{trip_updates}};
	print("@keys\n");
	die("No trip with trip_id $trip_id.\n");
    }
    print(Dumper($tu));
}

sub print_trip_status {
    my ($self, $trip_id) = @_;
    $self->get_realtime_status_data();
    my $tu = $self->{trip_updates}->{$trip_id};
    if (!$tu) {
	my @keys = keys %{$self->{trip_updates}};
	print("@keys\n");
	die("No trip with trip_id $trip_id.\n");
    }

    printf("            Trip ID: %s\n", $trip_id);
    printf("Longitude, Latitude: %.6f, %.6f\n",
	   $tu->{longitude}, $tu->{latitude});
    printf("              Route: %s %s\n", $tu->{route_short_name}, $tu->{route_long_name});
    printf("        Destination: %s\n", $tu->{trip_headsign});
    printf("              As of: %s\n",  strftime("%Y-%m-%d %H:%M:%S %Z", localtime($tu->{as_of})));
    printf("               (Now: %s)\n", strftime("%Y-%m-%d %H:%M:%S %Z", localtime()));
    print("\n");

    print("    Seq. Due Time                                  Longitude   Latitude    | Est.Time Delay\n");
    print("    ---- -------- -------------------------------- ----------- ----------- | -------- -----\n");

    foreach my $stu (@{$tu->{stop_time_update}}) {
	printf("%3s %4s %-8s %-32.32s %11.6f %11.6f | %-8s %5s\n",
	       eval { $stu->{is_next_stop_time_update} } ? "***" : "",
	       $stu->{stop_sequence} // "-",
	       $stu->{scheduled_time} // "-",
	       $stu->{stop_name} // "-",
	       $stu->{stop_lon},
	       $stu->{stop_lat},
	       $stu->{realtime_time} ? strftime("%H:%M:%S", localtime($stu->{realtime_time})) : "-",
	       $stu->{delay_minutes} // "");
    }


}

sub print_realtime_status_raw {
    my ($self) = @_;
    my $o = $self->get_realtime_status_data(limit_stop_time_updates => 1);
    print(Dumper($o));
}

sub print_realtime_status {
    my ($self) = @_;
    $self->get_realtime_status_data(limit_stop_time_updates => 1);

    print("                                                                                                   Stop                                  SCHDULED Realtime    \n");
    print("Coach Route                                  Trip ID Headsign                           As of      Seq. Next Stop Location               Time     Time     Dly\n");
    print("----- ----- -------------------------------- ------- --------------------------------   --------   ---- -------------------------------- -------- -------- ---\n");

    foreach my $tu ($self->get_sorted_trip_updates()) {
	my $as_of = $tu->{as_of};
	my $stu = $tu->{next_stop};

	my $dep_time        = eval { $stu->{dep_time} };
	my $next_stop_name  = eval { $stu->{stop_name} };
	my $next_stop_delay = eval { $stu->{delay_minutes} };
	my $realtime_time   = eval { $stu->{realtime_time} };

	my $fmt_as_of         = $as_of         && eval { strftime("%H:%M:%S", localtime($as_of        )) } // "-";
	my $fmt_realtime_time = $realtime_time && eval { strftime("%H:%M:%S", localtime($realtime_time)) } // "-";
	my $scheduled_time    = eval { $stu->{scheduled_time} };

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

sub get_realtime_feeds {
    my ($self) = @_;
    return $self->db->get_geo_gtfs_realtime_feeds($self->{geo_gtfs_agency_id});
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

