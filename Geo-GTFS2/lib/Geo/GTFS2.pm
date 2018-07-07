package Geo::GTFS2;
use strict;
use warnings;

use base "Geo::GTFS2::Object";

use Geo::GTFS2::Realtime;
use Geo::GTFS2::Util::Progress;

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
use feature qw(say);

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
                     no_auto_update

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
    warn(sprintf("GET %s ...\n", $url)) if $self->{verbose} || -t 2;
    my $response = $ua->request($request);
    $request->header("Date-Epoch", time());
    if (!$response->is_success) {
	warn(sprintf("%s => %s\n", $response->base, $response->status_line));
	return;
    }
    warn(sprintf("... %s\n", $response->status_line)) if $self->{verbose} || -t 2;
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
    my $base_url = $response->base->as_string;
    my $cached = ($response->code == 304 || ($response->header("X-Cached") && $response->header("X-Content-Unchanged")));
    my $cref = $response->content_ref;
    my $content = $response->content;

    my $retrieved     = $response->date;
    my $last_modified = $response->last_modified;
    my $content_length = $response->content_length;

    my $md5 = md5_hex($base_url);

    my $dh;
    open($dh, "+<", \$content);

    my $zip = Archive::Zip->new;
    $zip->readFromFileHandle($dh);

    my $agency_txt_member = $self->find_agency_txt_member($zip);
    if (!$agency_txt_member) {
        die("no .zip member matching */agency.txt");
    }
    my $agency_parsed = $self->parse_zip_member($zip, $agency_txt_member);
    if (!$agency_parsed) {
        die("no agency data");
    }
    my $agency_rows = $agency_parsed->{rows};
    print(Dumper($agency_rows));
    if (!scalar @$agency_rows) {
        die("no agency data");
    }
    if (scalar @$agency_rows > 1) {
        die("More than one agency? Eh?");
    }

    my $agency_name = $agency_rows->[0]->{agency_name};
    my $agency_id = $self->select_or_insert_geo_gtfs_agency_id($agency_name);

    my $zip_filename = sprintf("%s/data/%s/gtfs/%s-%s-%s-%s.zip",
                               $self->{dir},
                               $agency_name,
                               $md5,
                               $retrieved,
                               $last_modified,
                               $content_length);
    my $rel_zip_filename = File::Spec->abs2rel($zip_filename,
                                               $self->{dir});
    make_path(dirname($zip_filename));
    die("$!") unless $self->file_put_contents_binary($zip_filename, $content);

    my $geo_gtfs_feed_id =
        $self->{geo_gtfs_feed_id} =
        $self->db->select_or_insert_geo_gtfs_feed_id(
            $agency_id, $base_url
        );
    my $geo_gtfs_feed_instance_id =
        $self->db->select_or_insert_geo_gtfs_feed_instance_id(
            $geo_gtfs_feed_id, $rel_zip_filename, $retrieved, $last_modified
        );

    my $sth;

    my $save_select = select(STDERR);
    my $save_flush  = $|;
    $| = 1;

    $self->delete_feed_instance_data($geo_gtfs_feed_instance_id);

    foreach my $member ($zip->members) {
        my $member_parsed = $self->parse_zip_member($zip, $member);
        my $table_name = $member_parsed->{table_name};
        if (!$self->db->table_exists($table_name)) {
            warn("No such table: $table_name\n");
            next;
        }

	my $fh = Archive::Zip::MemberRead->new($zip, $filename);

	my $csv = Text::CSV->new ({ binary => 1 });
	my $line = $fh->getline();
	$line =~ s{[\r\n]+$}{};
	$csv->parse($line);
	my @fields = $csv->fields();
        my @field_info = map { $self->db->table_field_info($table_name, $_) } @fields;

        if (!scalar @fields) {
            warn("no fields in member $filename of $zip_filename\n");
            next;
        }

	$self->dbh->do("delete from $table_name where geo_gtfs_feed_instance_id = ?",
		       {}, $geo_gtfs_feed_instance_id);

	my $sql = sprintf("insert into $table_name(geo_gtfs_feed_instance_id, %s) values(?, %s);",
			  join(", ", @fields),
			  join(", ", ("?") x (scalar @fields)));
	$sth = $self->dbh->prepare($sql);

	print STDERR ("Populating $table_name ...\n");

        my $progress = Geo::GTFS2::Util::Progress->new(
            progress_message => "  %d rows",
            completion_message => "  Done.  Inserted %d rows."
        );

	my $rows = 0;
	while (defined(my $line = $fh->getline())) {
	    $line =~ s{\R\z}{}; # safer chomp
            next unless $line =~ m{\S}; # line must contain non-whitespace
	    $csv->parse($line);
	    my @data = $csv->fields();
            for (my $i = 0; $i < scalar @fields; $i += 1) {
                if ($data[$i] eq "" &&
                        ($field_info[$i]{type} eq "numeric" || $field_info[$i]{type} eq "integer") &&
                        $field_info[$i]{nullable}) {
                    $data[$i] = undef;
                }
            }
            $sth->execute($geo_gtfs_feed_instance_id, @data);
            $progress->tick();
	}
        $progress->done();
    }
    $self->dbh->commit();

    $| = $save_flush;
    select($save_select);
}

sub file_put_contents_binary {
    my ($self, $filename, $data) = @_;
    my $fh;
    if (!open($fh, ">", $filename)) {
        warn("open $filename: $!\n");
        return 0;
    }
    if (!binmode($fh)) {
        warn("binmode $filename: $!\n");
        return 0;
    }
    if (ref $data) {
        if (!print $fh ($$data)) {
            warn("print $filename: $!\n");
            return 0;
        }
    } else {
        if (!print $fh ($data)) {
            warn("print $filename: $!\n");
            return 0;
        }
    }
    if (!close($fh)) {
        warn("close: $!\n");
        return 0;
    }
    return 1;
}

sub find_agency_txt_member {
    my ($self, $zip) = @_;
    my $zip_filename = $zip->fileName;
    my $member = $zip->memberNamed("agency.txt");
    return $member if $member;
    my @members = $zip->membersMatching('(^|[/\\\\])agency.txt$');
    if (scalar @members < 1) {
        warn("$zip_filename; no member named */agency.txt\n");
        return;
    }
    return $members[0];
}

sub parse_zip_member {
    my ($self, $zip, $member) = @_;

    my $zip_filename = $zip->fileName;
    my $member_filename = $member->fileName;
    my $member_basename = basename($member_filename, ".txt");
    my $table_name = "gtfs_$member_basename";

    my $fh = Archive::Zip::MemberRead->new($zip, $member);

    my $csv = Text::CSV->new ({ binary => 1 });
    my $line = $fh->getline;
    $line =~ s{[\r\n]+$}{};
    $csv->parse($line);
    my $fields = [$csv->fields()];
    if (!($fields && scalar @$fields)) {
        warn("$zip_filename: $member_filename has no fields\n");
        return;
    }

    my @rows;
    while (defined(my $line = $fh->getline)) {
        $line =~ s{[\r\n]+$}{};
        next unless $line =~ m{\S};
        $csv->parse($line);
        my $data = [$csv->fields()];
        next unless $data && scalar @$data;
        my $row = {};
        for (my $i = 0; $i < scalar @$data || $i < scalar @$fields; $i += 1) {
            my $key   = $fields->[$i];
            my $value = $data->[$i];
            $row->{$key} = $value if defined $key && defined $value;
        }
        push(@rows, $row);
    }

    return {
        table_name => $table_name,
        fields     => $fields,
        rows       => \@rows
    };
}

###############################################################################
# AGENCIES
###############################################################################

sub select_or_insert_geo_gtfs_agency_id {
    my ($self, $geo_gtfs_agency_name) = @_;
    my $sth = $self->dbh->prepare(<<"END");
        select * from geo_gtfs_agency where name = ?;
END
    $sth->execute($geo_gtfs_agency_name);
    my $row = $sth->fetchrow_hashref;
    return $row->{id} if $row;
    my $hash = $self->create_geo_gtfs_agency($geo_gtfs_agency_name);
    return $hash->{id};
}

sub create_geo_gtfs_agency {
    my ($self, $geo_gtfs_agency_name) = @_;
    my $sth = $self->dbh->prepare(<<"END");
        insert into geo_gtfs_agency (name) values (?);
END
    $sth->execute($geo_gtfs_agency_name);
    $sth->finish;
    my $id = $self->dbh->last_insert_id("", "", "", "");
    $self->dbh->commit;
    return {
        id => $id
    };
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

sub list_feed_instances {
    my ($self) = @_;
    my $sth = $self->dbh->prepare(<<"END");
        select fi.id                 as id,
               fi.geo_gtfs_feed_id   as geo_gtfs_feed_id,
               fi.filename           as filename,
               fi.retrieved          as retrieved,
               fi.last_modified      as last_modified,
               fi.is_latest          as is_latest,
               f.geo_gtfs_agency_id  as geo_gtfs_agency_id,
               f.url                 as url,
               f.is_active           as is_active,
               a.name                as name
        from geo_gtfs_feed_instance fi
             join geo_gtfs_feed f on fi.geo_gtfs_feed_id = f.id
             join geo_gtfs_agency a on a.id = f.geo_gtfs_agency_id;
END
    $sth->execute();
    while (my $row = $sth->fetchrow_hashref) {
        printf("%4d. (%s) %s\n", $row->{name}, $row->{url});
        printf("     retrieved %s; last modified %s\n", localtime($row->{retrieved}), localtime($row->{last_modified}));
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
    warn $arg;
    return 0 unless $arg =~ m{^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$};
    my @octet = ($1, $2, $3, $4);
    warn @octet;
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

sub get_table_info {
    my ($self, $table_name) = @_;
    my $sth = $self->dbh->table_info(undef, "%", $table_name, "TABLE");
    $sth->execute();
    my $row = $sth->fetchrow_hashref();
    return $row;
}

sub sql_to_create_tables {
    my ($self) = @_;
    return $self->db->sql_to_create_tables;
}

sub sql_to_drop_tables {
    my ($self) = @_;
    return $self->db->sql_to_drop_tables;
}

sub sql_to_update_tables {
    my ($self) = @_;
    return $self->db->sql_to_update_tables;
}

sub delete_feed_instance {
    my ($self, $feed_instance_id) = @_;
    $self->db->delete_feed_instance($feed_instance_id);
}

sub delete_feed_instance_data {
    my ($self, $feed_instance_id) = @_;
    $self->db->delete_feed_instance_data($feed_instance_id);
}

sub update_tables {
    my ($self) = @_;
    $self->db->update_tables;
}

sub find_non_uniqueness {
    my ($self) = @_;
    $self->db->find_non_uniqueness;
}

sub delete_all_data {
    my ($self) = @_;
    $self->db->delete_all_data;
}

use Geo::GTFS2::DB;

sub dbh {
    my ($self) = @_;
    return $self->db->dbh;
}

sub db {
    my ($self) = @_;
    return $self->{db} //=
        Geo::GTFS2::DB->new(no_auto_update => $self->{no_auto_update});
}

sub close_db {
    my ($self) = @_;
    delete $self->{db};
}

sub DESTROY {
    my ($self) = @_;
    $self->close_db();
}

1;                              # End of Geo::GTFS2
