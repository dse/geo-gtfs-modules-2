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

# [1] File::MMagic best detects .zip files, and allows us to add magic
# for Google Protocol Buffers files.

sub init {
    my ($self, %args) = @_;
    $self->SUPER::init(%args);

    my $dir = $self->{dir};
    $self->{http_cache_dir} //= "$dir/http-cache";
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
    HTTP::Cache::Transparent::init({ BasePath => $self->{http_cache_dir},
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
	return $self->process_protocol_buffers($request, $response);
    } elsif ($response->base =~ m{\.pb$}i) {
	return $self->process_protocol_buffers($request, $response);
    } elsif ($url =~ m{\.pb$}i) {
	return $self->process_protocol_buffers($request, $response);
    } else {
        return undef;
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
    HTTP::Cache::Transparent::init({ BasePath => $self->{http_cache_dir},
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

    my $gtfs2 = $self->{gtfs2};

    my $agency_dir_name;
    if ($gtfs2) {
        $agency_dir_name = $gtfs2->{geo_gtfs_agency_name};
    } else {
        $agency_dir_name = $self->{agency_name} // ".";
    }

    my $pb_filename     = sprintf("%s/data/%s/pb/%s/%s.pb",     $self->{dir}, $agency_dir_name, $feed_type, $base_filename);
    my $rel_pb_filename = sprintf(   "data/%s/pb/%s/%s.pb",                   $agency_dir_name, $feed_type, $base_filename);
    my $json_filename   = sprintf("%s/data/%s/json/%s/%s.json", $self->{dir}, $agency_dir_name, $feed_type, $base_filename);

    stat($pb_filename);
    if (!($cached && -e _ && defined $content_length && $content_length == (stat(_))[7])) {
        if ($self->{write_pb}) {
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

sub ua {
    my ($self) = @_;
    return $self->{ua} //= LWP::UserAgent->new();
}

sub json {
    my ($self) = @_;
    return $self->{json} //= JSON->new()->allow_nonref()->pretty()->convert_blessed();
}

1;                              # End of Geo::GTFS2::Realtime
