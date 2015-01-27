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
    return $self->{gtfs2} = Geo::GTFS2->new();
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
# sub help__list_agencies { {
# } }

sub cmd__url {
    my ($self, $geo_gtfs_agency_name, $url) = @_;
    $self->gtfs2->set_agency($geo_gtfs_agency_name);
    $self->gtfs2->process_url($url);
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
