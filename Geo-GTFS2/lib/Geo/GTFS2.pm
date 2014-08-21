package Geo::GTFS2;
use strict;
use warnings;

use POSIX qw(strftime floor uname);

use LWP::UserAgent;
use HTTP::Cache::Transparent;
use File::MMagic;		# best detects .zip files; allows us
                                # to add magic for Google Protocol
                                # Buffers files.
use DBI;
use File::Path qw(make_path);
use File::Basename qw(dirname);
use HTTP::Date;
use Data::Dumper;
use Google::ProtocolBuffers;
use JSON qw(-convert_blessed_universally);

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

use fields qw(dir
	      sqlite_filename
	      dbh
	      ua
	      magic
	      http_cache_dir
	      gtfs_realtime_proto
	      json
	      gtfs_realtime_protocol_pulled

	      vehicle_positions
	      trip_updates
	      alerts
	    );

sub new {
    my ($class, %args) = @_;
    my $self = fields::new($class);
    $self->init(%args);
    return $self;
}

sub init {
    my ($self, %args) = @_;
    if (!defined $ENV{HOME}) {
	$ENV{HOME} = (getpwent())[7];
    }
    my $dir = $self->{dir} = "$ENV{HOME}/.geo-gtfs2";
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

#------------------------------------------------------------------------------

sub pull_gtfs_realtime_protocol {
    my ($self) = @_;
    return 1 if $self->{gtfs_realtime_protocol_pulled};
    HTTP::Cache::Transparent::init({ BasePath => $self->{http_cache_dir},
				     Verbose => 0,
				     NoUpdate => 86400,
				     NoUpdateImpatient => 0 });
    my $request = HTTP::Request->new("GET", $self->{gtfs_realtime_proto});
    my $response = $self->ua->request($request);
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
    Google::ProtocolBuffers->parse($proto);
    $self->{gtfs_realtime_protocol_pulled} = 1;
}
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
sub process_gtfs_feed {
    my ($self, $geo_gtfs_agency_name, $request, $response) = @_;
    my $url = $response->base;
    my $cached = ($response->code == 304 || ($response->header("X-Cached") && $response->header("X-Content-Unchanged")));
    my $cref = $response->content_ref;

    my $retrieved     = $response->date;
    my $last_modified = $response->last_modified;
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
	} else {
	    die("Cannot write $pb_filename: $!\n");
	}
	make_path(dirname($json_filename));
	if (open(my $fh, ">", $json_filename)) {
	    warn("Writing $json_filename ...\n");
	    binmode($fh);
	    print {$fh} $self->json->encode($o);
	} else {
	    die("Cannot write $pb_filename: $!\n");
	}
	warn("Done.\n");
    }

    my $geo_gtfs_agency_id = $self->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
    my $geo_gtfs_realtime_feed_id = $self->select_or_insert_geo_gtfs_realtime_feed_id($geo_gtfs_agency_id, $url, $feed_type);
    my $geo_gtfs_realtime_feed_instance_id =
      $self->select_or_insert_geo_gtfs_realtime_feed_instance_id($geo_gtfs_realtime_feed_id,
								 $rel_pb_filename,
								 $retrieved,
								 $last_modified,
								 $header_timestamp);
}

sub update {
    my ($self, $geo_gtfs_agency_name) = @_;
    my $geo_gtfs_agency_id = $self->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
}

sub update_realtime {
    my ($self, $geo_gtfs_agency_name) = @_;
    my $geo_gtfs_agency_id = $self->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
    my @feeds = $self->get_geo_gtfs_realtime_feeds($geo_gtfs_agency_id);
    foreach my $feed (@feeds) {
	my $url = $feed->{url};
	$self->process_url($geo_gtfs_agency_name, $url);
    }
}

sub list_latest_realtime_feeds {
    my ($self, $geo_gtfs_agency_name) = @_;
    my $geo_gtfs_agency_id = $self->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
    my @instances = $self->get_latest_geo_gtfs_realtime_feed_instances($geo_gtfs_agency_id);
    print("id      feed type  filename\n");
    print("------  ---------  -------------------------------------------------------------------------------\n");
    foreach my $i (@instances) {
	printf("%6d  %-9s  %s\n",
	       @{$i}{qw(id feed_type filename)});
    }
}

sub realtime_status {
    my ($self, $geo_gtfs_agency_name) = @_;
    my $geo_gtfs_agency_id = $self->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
    my @instances = $self->get_latest_geo_gtfs_realtime_feed_instances($geo_gtfs_agency_id);
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

    if ($instances{all}) {
	my $o = $self->read_feed($instances{all}{filename});
	my $header_timestamp = eval { $o->{header}->{timestamp} };
	foreach my $e (@{$o->{entity}}) {
	    $e->{header_timestamp} = $header_timestamp if defined $header_timestamp;
	    $process_entity->($e);
	}
    } else {
	foreach my $feed_type (qw(alerts updates positions)) {
	    my $i = $instances{$feed_type};
	    if ($i) {
		my $o = $self->read_feed($instances{all}{filename});
		my $header_timestamp = eval { $o->{header}->{timestamp} };
		foreach my $e (@{$o->{entity}}) {
		    $e->{header_timestamp} = $header_timestamp if defined $header_timestamp;
		    $process_entity->($e);
		}
	    }
	}
    }

    my $status_info = $self->get_useful_realtime_status_info(\@vp, \@tu, \@alerts);
    my @info = @{$status_info->{info}};

    @info = (map { $_->[0] }
	       sort { _route_id_sort($a->[1], $b->[1]) }
		 map { [$_, $_->{route_id} // ""] }
		   @info);

    foreach my $info (@info) {
	my $age = $info->{header_timestamp} - $info->{timestamp};
	if ($age >= 3600) {
	    $info->{old} = 2;
	} elsif ($age >= 600) {
	    $info->{old} = 1;
	} else {
	    $info->{old} = 0;
	}
    }

    my $print = sub {
	my ($info) = @_;
	printf("%-7s  %10.6f  %10.6f  %-5s  %-5s %-8s  %-5s %-8s\n",
	       $info->{label} // "-",
	       $info->{latitude} // 0,
	       $info->{longitude} // 0,
	       $info->{route_id} // "-",
	       $info->{timestamp_date} // "-",
	       $info->{timestamp_time} // "-",
	       $info->{header_timestamp_date} // "-",
	       $info->{header_timestamp_time} // "-",
	      );
    };

    print("                                                        header        \n");
    print("vehicle  lat.        lng.        route  date  time      date  time    \n");
    print("-------  ----------  ----------  -----  ----- --------  ----- --------\n");
    $print->($_) foreach grep { $_->{old} == 0 } @info;
    print("-------  ----------  ----------  -----  ----- --------  ----- --------\n");
    $print->($_) foreach grep { $_->{old} == 1 } @info;
    print("-------  ----------  ----------  -----  ----- --------  ----- --------\n");
    $print->($_) foreach grep { $_->{old} == 2 } @info;
}

sub get_useful_realtime_status_info {
    my ($self, $vp_array, $tu_array, $alerts_array) = @_;

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
    }

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

use File::Spec;

sub read_feed {
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

sub list_routes {
    my ($self, $geo_gtfs_agency_name) = @_;
}

sub list_realtime_feeds {
    my ($self, $geo_gtfs_agency_name) = @_;
    my $geo_gtfs_agency_id = $self->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
    my @feeds = $self->get_geo_gtfs_realtime_feeds($geo_gtfs_agency_id);
    print("id    active?  feed type  url\n");
    print("----  -------  ---------  -------------------------------------------------------------------------------\n");
    foreach my $feed (@feeds) {
	printf("%4d  %4d     %-9s  %s\n", @{$feed}{qw(id is_active feed_type url)});
    }
}

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

sub select_or_insert_geo_gtfs_agency_id {
    my ($self, $geo_gtfs_agency_name) = @_;
    return $self->select_or_insert_id("table_name" => "geo_gtfs_agency",
				      "id_name" => "id",
				      "key_fields" => { "name" => $geo_gtfs_agency_name });
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

sub exec_sqlite_utility {
    my ($self) = @_;
    my $dbfile = $self->{sqlite_filename};
    exec("sqlite3", $dbfile) or die("cannot exec sqlite: $!\n");
}
#------------------------------------------------------------------------------
sub help_cmdline { print(<<"END"); }
  gtfs2 ridetarc.org <URL> ...
  gtfs2 ridetarc.org update [<URL> ...]
  gtfs2 ridetarc.org update-realtime
  gtfs2 ridetarc.org realtime-status
  gtfs2 list-agencies
  gtfs2 ridetarc.org list-routes
END
sub is_agency_name {
    my ($self, $arg) = @_;
    return $arg =~ m{^
		     [A-Za-z0-9]+(-[A-Za-z0-9]+)*
		     (\.[A-Za-z0-9]+(-[A-Za-z0-9]+)*)+
		     $}xi
		       && !$self->is_ipv4_address($arg);
}
use List::MoreUtils;
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
sub run_cmdline {
    my ($self, @args) = @_;
    if (!@args) {
	$self->help_cmdline();
    } elsif ($self->is_agency_name($args[0])) {
	my $geo_gtfs_agency_name = shift(@args);
	if (!@args) {
	    my ($geo_gtfs_agency_id, $status) = $self->select_or_insert_geo_gtfs_agency_id($geo_gtfs_agency_name);
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
###############################################################################
sub ua {
    my ($self) = @_;
    return $self->{ua} //= LWP::UserAgent->new();
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
sub drop_tables {
    my ($self) = @_;
    my $dbh = $self->{dbh};
    print STDERR ("Dropping database tables...\n");
    $self->dbh->do(<<"END");
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
    my $dbh = $self->{dbh};

    my $sql = <<"END";
create table if not exists
             geo_gtfs (				name				varchar(32)	not null	primary key,
						value				text		null
);
delete from geo_gtfs where name = 'geo_gtfs.db.version';
insert into geo_gtfs (name, value) values('geo_gtfs.db.version', '0.1');

create table if not exists
             geo_gtfs_agency (			id				integer				primary key autoincrement,
						name				varchar(64)	not null	-- preferably the transit agency's domain name, without a www. prefix. - examples: 'ridetarc.org', 'ttc.ca'
);
create index if not exists  geo_gtfs_agency_01 on geo_gtfs_agency(name);

create table if not exists
             geo_gtfs_feed (			id				integer				primary key autoincrement,
						geo_gtfs_agency_id		integer		not null	references geo_gtfs_agency(id),
						url				text		not null,
						is_active			integer		not null	default 1	-- updated when feeds added, removed
);
create index if not exists  geo_gtfs_feed_01 on geo_gtfs_feed(is_active);

create table if not exists
             geo_gtfs_feed_instance (		id				integer				primary key autoincrement,
						geo_gtfs_feed_id		integer		not null	references geo_gtfs_feed(id),
						filename			text		not null,
						retrieved			integer		not null,
						last_modified			integer		null,		-- SHOULD be specified, but some servers omit.
						is_latest			integer		not null
);
create index if not exists  geo_gtfs_feed_instance_01 on geo_gtfs_feed_instance(is_latest);

create table if not exists
             geo_gtfs_realtime_feed (		id				integer				primary key,
						geo_gtfs_agency_id		integer		not null	references geo_gtfs_agency(id),
						url				text		not null,
						feed_type			varchar(16)	not null,	-- 'updates', 'positions', 'alerts', 'all'
						is_active			integer		not null default 1	-- updated when feeds added, removed
);
create index if not exists  geo_gtfs_realtime_feed_01 on geo_gtfs_realtime_feed(feed_type);
create index if not exists  geo_gtfs_realtime_feed_02 on geo_gtfs_realtime_feed(is_active);

create table if not exists
             geo_gtfs_realtime_feed_instance (	id				integer				primary key,
						geo_gtfs_realtime_feed_id	integer		not null	references geo_gtfs_realtime_feed(id),
						filename			text		not null,
						retrieved			integer		not null,
						last_modified			integer		null,
						header_timestamp		integer		null,
						is_latest			integer		not null default 1
);
create index if not exists  geo_gtfs_realtime_feed_instance_01 on geo_gtfs_realtime_feed_instance(is_latest);
-------------------------------------------------------------------------------
create table if not exists
             gtfs_agency (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						agency_id			text		null,		-- indexed -- for feeds containing only one agency, this can be NULL.
						agency_name			text		not null,
						agency_url			text		not null,
						agency_timezone			text		not null,
						agency_lang			varchar(2)	null,
						agency_phone			text		null,
						agency_fare_url			text		null
);
create unique index if not exists  gtfs_agency_01 on gtfs_agency(geo_gtfs_feed_instance_id, agency_id);

create table if not exists
             gtfs_stops (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						stop_id				text		not null,	-- indexed --
						stop_code			text		null,
						stop_name			text		not null,
						stop_desc			text		null,
						stop_lat			numeric		not null,
						stop_lon			numeric		not null,
						zone_id				text		null,		-- indexed --
						stop_url			text		null,
						location_type			integer		null,
						parent_station			text		null,
						stop_timezone			text		null,
						wheelchair_boarding		integer		null
);
create unique index if not exists  gtfs_stops_01 on gtfs_stops(geo_gtfs_feed_instance_id, stop_id);
create        index if not exists gtfs_stops_02 on gtfs_stops(geo_gtfs_feed_instance_id, zone_id);
create        index if not exists gtfs_stops_03 on gtfs_stops(geo_gtfs_feed_instance_id, location_type);
create        index if not exists gtfs_stops_04 on gtfs_stops(geo_gtfs_feed_instance_id, parent_station);
create        index if not exists gtfs_stops_05 on gtfs_stops(geo_gtfs_feed_instance_id, wheelchair_boarding);

create table if not exists
             gtfs_routes (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						route_id			text		not null,	-- indexed --
						agency_id			text		null		references gtfs_agency(id),
						route_short_name		text		not null,
						route_long_name			text		not null,
						route_desc			text		null,
						route_type			integer		not null,
						route_url			text		null,
						route_color			varchar(6)	null,
						route_text_color		varchar(6)	null
);
create unique index if not exists  gtfs_routes_01 on gtfs_routes (geo_gtfs_feed_instance_id, route_id, agency_id);
create        index if not exists gtfs_routes_02 on gtfs_routes (geo_gtfs_feed_instance_id, agency_id);
create        index if not exists gtfs_routes_03 on gtfs_routes (geo_gtfs_feed_instance_id, route_id);
create        index if not exists gtfs_routes_04 on gtfs_routes (geo_gtfs_feed_instance_id, route_type);

create table if not exists
             gtfs_trips (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						route_id			text		not null	references gtfs_routes(id),
						service_id			text		not null,	-- indexed --
						trip_id				text		not null,	-- indexed --
						trip_headsign			text		null,
						trip_short_name			text		null,
						direction_id			integer		null,		-- indexed --
						block_id			text		null,		-- indexed --
						shape_id			text		null		references gtfs_shapes(id),
						wheelchair_accessible		integer		null,
						bikes_allowed			integer		null
);
create unique index if not exists  gtfs_trips_01 on gtfs_trips (geo_gtfs_feed_instance_id, trip_id);
create        index if not exists gtfs_trips_02 on gtfs_trips (geo_gtfs_feed_instance_id, route_id);
create        index if not exists gtfs_trips_03 on gtfs_trips (geo_gtfs_feed_instance_id, service_id);
create        index if not exists gtfs_trips_04 on gtfs_trips (geo_gtfs_feed_instance_id, direction_id);
create        index if not exists gtfs_trips_05 on gtfs_trips (geo_gtfs_feed_instance_id, block_id);
create        index if not exists gtfs_trips_06 on gtfs_trips (geo_gtfs_feed_instance_id, shape_id);

create table if not exists
             gtfs_stop_times (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						trip_id				text		not null	references gtfs_trips(id),
						arrival_time			varchar(8)	not null,
						departure_time			varchar(8)	not null,
						stop_id				text		not null	references gtfs_stops(id),
						stop_sequence			integer		not null,
						stop_headsign			text		null,
						pickup_type			integer		null,
						drop_off_type			integer		null,
						shape_dist_traveled		numeric		null
);
create unique index if not exists  gtfs_stop_times_01 on gtfs_stop_times (geo_gtfs_feed_instance_id, stop_id);
create        index if not exists gtfs_stop_times_02 on gtfs_stop_times (geo_gtfs_feed_instance_id, trip_id);
create        index if not exists gtfs_stop_times_03 on gtfs_stop_times (geo_gtfs_feed_instance_id, stop_sequence);

create table if not exists
             gtfs_calendar (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						service_id			text		not null,	-- indexed --
						monday				integer		not null,
						tuesday				integer		not null,
						wednesday			integer		not null,
						thursday			integer		not null,
						friday				integer		not null,
						saturday			integer		not null,
						sunday				integer		not null,
						start_date			varchar(8)	not null,
						end_date			varchar(8)	not null
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

create table if not exists
             gtfs_calendar_dates (		geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						service_id			text		not null,	-- indexed --
						`date`				varchar(8)	not null,
						exception_type			integer		not null
);
create        index if not exists gtfs_calendar_dates_01 on gtfs_calendar_dates(geo_gtfs_feed_instance_id, service_id);
create        index if not exists gtfs_calendar_dates_02 on gtfs_calendar_dates(geo_gtfs_feed_instance_id, `date`);
create        index if not exists gtfs_calendar_dates_03 on gtfs_calendar_dates(geo_gtfs_feed_instance_id, exception_type);

create table if not exists
             gtfs_fare_attributes (		geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						fare_id				text		not null,	-- indexed --
						price				numeric		not null,
						currency_type			text		not null,
						payment_method			integer		not null,
						transfers			integer		not null,
						transfer_duration		integer		null
);
create        index if not exists gtfs_fare_attributes_01 on gtfs_fare_attributes(geo_gtfs_feed_instance_id, fare_id);

create table if not exists
             gtfs_fare_rules (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						fare_id				text		not null	references gtfs_fare_attributes(fare_id),
						route_id			text		null		references gtfs_routes(id),
						origin_id			text		null,		-- indexed --
						destination_id			text		null,		-- indexed --
						contains_id			text		null		-- indexed --
);
create        index if not exists gtfs_fare_rules_01 on gtfs_fare_rules(geo_gtfs_feed_instance_id, fare_id);
create        index if not exists gtfs_fare_rules_02 on gtfs_fare_rules(geo_gtfs_feed_instance_id, route_id);
create        index if not exists gtfs_fare_rules_03 on gtfs_fare_rules(geo_gtfs_feed_instance_id, origin_id);
create        index if not exists gtfs_fare_rules_04 on gtfs_fare_rules(geo_gtfs_feed_instance_id, destination_id);
create        index if not exists gtfs_fare_rules_05 on gtfs_fare_rules(geo_gtfs_feed_instance_id, contains_id);

create table if not exists
             gtfs_shapes (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						shape_id			text		not null,	-- indexed --
						shape_pt_lat			numeric		not null,
						shape_pt_lon			numeric		not null,
						shape_pt_sequence		integer		not null,	-- indexed --
						shape_dist_traveled		numeric		null
);
create        index if not exists gtfs_shapes_01 on gtfs_shapes(geo_gtfs_feed_instance_id, shape_id);
create        index if not exists gtfs_shapes_02 on gtfs_shapes(geo_gtfs_feed_instance_id, shape_id, shape_pt_sequence);

create table if not exists
             gtfs_frequencies (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						trip_id				text		null		references gtfs_trips(id),
						start_time			varchar(8)	null, --indexed
						end_time			varchar(8)	null, --indexed
						headway_secs			integer		null,
						exact_times			integer		null
);
create        index if not exists gtfs_frequencies_01 on gtfs_frequencies(geo_gtfs_feed_instance_id, trip_id);
create        index if not exists gtfs_frequencies_02 on gtfs_frequencies(geo_gtfs_feed_instance_id, start_time);
create        index if not exists gtfs_frequencies_03 on gtfs_frequencies(geo_gtfs_feed_instance_id, end_time);

create table if not exists
             gtfs_transfers (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						from_stop_id			text		not null	references gtfs_stops(id),
						to_stop_id			text		not null	references gtfs_stops(id),
						transfer_type			integer		not null,
						min_transfer_time		integer		null
);
create        index if not exists gtfs_transfers_01 on gtfs_transfers(from_stop_id);
create        index if not exists gtfs_transfers_02 on gtfs_transfers(to_stop_id);

create table if not exists
             gtfs_feed_info (			geo_gtfs_feed_instance_id	integer		not null	references geo_gtfs_feed(id),
						feed_publisher_name		text		not null,
						feed_publisher_url		text		not null,
						feed_lang			text		not null,
						feed_start_date			varchar(8)	null,
						feed_end_date			varchar(8)	null,
						feed_version			text		null
);

create index if not exists  geo_gtfs_agency_00			on gtfs_agency		(geo_gtfs_feed_instance_id);
create index if not exists  geo_gtfs_stops_00			on gtfs_stops		(geo_gtfs_feed_instance_id);
create index if not exists  geo_gtfs_routes_00			on gtfs_routes		(geo_gtfs_feed_instance_id);
create index if not exists  geo_gtfs_trips_00			on gtfs_trips		(geo_gtfs_feed_instance_id);
create index if not exists  geo_gtfs_stop_times_00		on gtfs_stop_times	(geo_gtfs_feed_instance_id);
create index if not exists  geo_gtfs_calendar_00		on gtfs_calendar	(geo_gtfs_feed_instance_id);
create index if not exists  geo_gtfs_calendar_dates_00		on gtfs_calendar_dates	(geo_gtfs_feed_instance_id);
create index if not exists  geo_gtfs_fare_attributes_00	on gtfs_fare_attributes	(geo_gtfs_feed_instance_id);
create index if not exists  geo_gtfs_fare_rules_00		on gtfs_fare_rules	(geo_gtfs_feed_instance_id);
create index if not exists  geo_gtfs_shapes_00			on gtfs_shapes		(geo_gtfs_feed_instance_id);
create index if not exists  geo_gtfs_frequencies_00		on gtfs_frequencies	(geo_gtfs_feed_instance_id);
create index if not exists  geo_gtfs_transfers_00		on gtfs_transfers	(geo_gtfs_feed_instance_id);
create index if not exists  geo_gtfs_feed_info_00		on gtfs_feed_info	(geo_gtfs_feed_instance_id);
END
    $sql =~ s{--.*?$}{}gsm;
    my @sql = split(qr{;$}m, $sql);
    foreach my $sql (@sql) {
	next unless $sql =~ m{\S};
	my $short = $sql;
	$short =~ s{\s+}{ }gsm;
	$short =~ s{\(.*}{};
	eval { $dbh->do($sql); };
	if ($@) {
	    my $error = $@;
	    print($sql);
	    die($error);
	}
    }
    $self->dbh->commit();
}

sub DESTROY {
    my ($self) = @_;
    my $dbh = $self->{dbh};
    if ($dbh) {
	$dbh->rollback();
    }
    # STFU: Issuing rollback() due to DESTROY without explicit disconnect() of DBD::SQLite::db handle /Users/dse/.geo-gtfs2/google_transit.sqlite.
}

sub json {
    my ($self) = @_;
    return $self->{json} //= JSON->new()->allow_nonref()->pretty()->convert_blessed();
}

1; # End of Geo::GTFS2
