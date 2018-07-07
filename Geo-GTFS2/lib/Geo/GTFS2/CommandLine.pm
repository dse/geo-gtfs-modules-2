package Geo::GTFS2::CommandLine;
use warnings;
use strict;

use base "App::Thingy";
use Geo::GTFS2;

sub __init {
    my ($self) = @_;
}

sub gtfs2 {
    my ($self) = @_;
    return $self->{gtfs2} if $self->{gtfs2};
    return $self->{gtfs2} = Geo::GTFS2->new(no_auto_update => $self->{no_auto_update});
}

sub no_auto_update {
    my ($self, $no_auto_update) = @_;
    if (scalar @_ >= 2) {
        return $self->{no_auto_update} = $no_auto_update;
    }
    return $self->{no_auto_update};
}

# sub cmd__update_realtime {
#     my ($self, $geo_gtfs_agency_name) = @_;
#     $self->gtfs2->set_agency($geo_gtfs_agency_name);
#     $self->gtfs2->fetch_all_realtime_feeds();
# }
# sub help__update_realtime { {
#     required => "AGENCY_NAME"
# } }

sub cmd__realtime_status {
    my ($self, $geo_gtfs_agency_name) = @_;
    $self->gtfs2->set_agency($geo_gtfs_agency_name);
    $self->gtfs2->realtime_status();
}
sub help__realtime_status { {
    required => "AGENCY_NAME"
} }

sub cmd__trip_status {
    my ($self, $geo_gtfs_agency_name, $trip_id) = @_;
    $self->gtfs2->set_agency($geo_gtfs_agency_name);
    $self->gtfs2->trip_status($trip_id);
}
sub help__trip_status { {
    required => "AGENCY_NAME TRIP_ID"
} }

# sub cmd__realtime_status_raw {
#     my ($self, $geo_gtfs_agency_name) = @_;
#     $self->gtfs2->set_agency($geo_gtfs_agency_name);
#     $self->gtfs2->print_realtime_status_raw();
# }
# sub help__realtime_status_raw { {
#     required => "AGENCY_NAME"
# } }

# sub cmd__list_realtime_feeds {
#     my ($self, $geo_gtfs_agency_name) = @_;
#     $self->gtfs2->set_agency($geo_gtfs_agency_name);
#     $self->gtfs2->list_realtime_feeds();
# }
# sub help__list_realtime_feeds { {
#     required => "AGENCY_NAME"
# } }

# sub cmd__realtime_summary {
#     my ($self, $geo_gtfs_agency_name) = @_;
#     $self->gtfs2->set_agency($geo_gtfs_agency_name);
#     $self->gtfs2->realtime_status(summary => 1);
# }
# sub help__realtime_summary { {
#     required => "AGENCY_NAME"
# } }

# sub cmd__realtime_raw {
#     my ($self, $geo_gtfs_agency_name) = @_;
#     $self->gtfs2->set_agency($geo_gtfs_agency_name);
#     $self->gtfs2->realtime_status(raw => 1);
# }
# sub help__realtime_raw { {
#     required => "AGENCY_NAME",
#     optional => "A B C D"
# } }

# sub cmd__sqlite {
#     my ($self) = @_;
#     $self->gtfs2->exec_sqlite_utility();
# }
# sub help__sqlite { {
# } }

# sub cmd__list_routes {
#     my ($self, $geo_gtfs_agency_name) = @_;
#     $self->gtfs2->set_agency($geo_gtfs_agency_name);
#     $self->gtfs2->list_routes();
# }
# sub help__list_routes { {
#     required => "AGENCY_NAME"
# } }

sub cmd__list_agencies {
    my ($self) = @_;
    $self->gtfs2->list_agencies();
}

sub cmd__list_feeds {
    my ($self) = @_;
    $self->gtfs2->list_feeds();
}

sub cmd__list_feed_instances {
    my ($self) = @_;
    $self->gtfs2->list_feed_instances();
}

sub cmd__url {
    my ($self, @arguments) = @_;

    my $geo_gtfs_agency_name;
    my $url;

    foreach my $argument (@arguments) {
        if ($argument =~ m{^https?://}i) {
            $url = $argument;
        } else {
            $geo_gtfs_agency_name = $argument;
        }
    }

    $self->gtfs2->process_url($url);
}

sub cmd__realtime_url {
    my ($self, $geo_gtfs_agency_name, $url) = @_;
    $self->gtfs2->set_agency($geo_gtfs_agency_name);
    $self->gtfs2->process_realtime_url($url);
}

sub cmd__force_pull_gtfs_realtime_protocol {
    my ($self) = @_;
    $self->gtfs2->force_pull_gtfs_realtime_protocol();
}

sub cmd__print_sql_to_create_tables {
    my ($self) = @_;
    $self->no_auto_update(1);
    my @sql = $self->gtfs2->sql_to_create_tables;
    foreach my $sql (@sql) {
        print $sql;
    }
}

sub cmd__print_sql_to_drop_tables {
    my ($self) = @_;
    $self->no_auto_update(1);
    my @sql = $self->gtfs2->sql_to_drop_tables;
    foreach my $sql (@sql) {
        print $sql;
    }
}

sub cmd__print_sql_to_update_tables {
    my ($self) = @_;
    $self->no_auto_update(1);
    my @sql = $self->gtfs2->sql_to_update_tables;
    foreach my $sql (@sql) {
        print $sql;
    }
}

sub cmd__delete_feed_instance {
    my ($self, $feed_instance_id) = @_;
    $self->no_auto_update(1);
    $self->gtfs2->delete_feed_instance($feed_instance_id);
}
sub help__delete_feed_instance { {
    required => "FEED_INSTANCE_ID"
} }

sub cmd__update_tables {
    my ($self) = @_;
    $self->no_auto_update(1);
    $self->gtfs2->update_tables;
}

sub cmd__find_non_uniqueness {
    my ($self) = @_;
    $self->no_auto_update(1);
    $self->gtfs2->find_non_uniqueness;
}

sub cmd__delete_all_data {
    my ($self) = @_;
    $self->no_auto_update(1);
    $self->gtfs2->delete_all_data;
}

sub cmd__AUTOLOAD {
    my ($self, $agency_name, $command, @args) = @_;
    if (defined $agency_name &&
	  $self->gtfs2->is_agency_name($agency_name) &&
	    $self->is_subcommand($command, @args)) {
	return ($command, $agency_name, @args);
    } else {
	return undef;
    }
}

1;
