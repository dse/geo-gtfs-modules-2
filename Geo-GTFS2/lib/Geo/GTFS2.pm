package Geo::GTFS2;
use strict;
use warnings;

use base "Geo::GTFS2::Object";

use Geo::GTFS2::Realtime;

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

sub process_realtime_url {
    my ($self, $url) = @_;
    $self->realtime->process_url($url);
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
    } elsif ($response->base =~ m{\.zip$}i) {
	return $self->process_gtfs_feed($request, $response);
    } elsif ($url =~ m{\.zip$}i) {
	return $self->process_gtfs_feed($request, $response);
    } else {
        die("Sorry, but I can't process the content at $url\n");
    }
}

sub force_pull_gtfs_realtime_protocol {
    my ($self) = @_;
    return $self->realtime->pull_gtfs_realtime_protocol(force => 1);
}

sub pull_gtfs_realtime_protocol {
    my ($self, %options) = @_;
    return $self->realtime->pull_gtfs_realtime_protocol(%options);
}

sub process_gtfs_realtime_data {
    my ($self, $request, $response) = @_;
    return $self->realtime->process_gtfs_realtime_data($request, $response);
}

sub write_pb {
    my ($self, $filename, $cref) = @_;
    return $self->realtime->write_pb($filename, $cref);
}

sub write_json {
    my ($self, $filename, $o) = @_;
    return $self->realtime->write_json($filename, $o);
}

sub get_vehicle_feed {
    my ($self) = @_;
    return $self->realtime->get_vehicle_feed();
}

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

sub realtime {
    my ($self) = @_;
    return $self->{realtime} //= Geo::GTFS2::Realtime->new(
        gtfs2 => $self
    );
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
    return $self->{db} //= Geo::GTFS2::DB->new();
}

sub close_db {
    my ($self) = @_;
    delete $self->{db};
}

sub DESTROY {
    my ($self) = @_;
    warn("$self DESTROY");
    $self->close_db();
}

1;                              # End of Geo::GTFS2
