package Geo::GTFS2::Object;
use warnings;
use strict;

use POSIX qw(uname);

sub new {
    my ($class, %args) = @_;
    my $self = bless({}, $class);
    $self->init(%args);
    return $self;
}

sub init {
    my ($self, %args) = @_;
    $self->init_args(%args);
    $self->init_dir();
}

sub init_args {
    my ($self, %args) = @_;
    while (my ($k, $v) = each(%args)) {
	$self->{$k} = $v;
    }
}

sub init_dir {
    my ($self) = @_;
    return if defined $self->{dir};
    my @pwent = getpwuid($>);
    my $username = $pwent[0];
    if ($username eq "_www") {  # special os x user
        $self->{dir} = "/Users/_www/.geo-gtfs2";
    } else {
	my $HOME = $ENV{HOME} // $pwent[7];
	$self->{dir} = "$HOME/.geo-gtfs2";
    }
}

BEGIN {
    my ($uname) = uname();
    if ($uname =~ m{^Darwin}) {
# 	my $ca_file = "/usr/local/opt/curl-ca-bundle/share/ca-bundle.crt";
# 	if (-e $ca_file) {
# 	    $ENV{HTTPS_CA_FILE} = $ca_file;
# 	} else {
# 	    warn(<<"END");

# Looks like you are using a Mac.  You should run:
#     brew install curl-ca-bundle.
# You may also need to run:
#     sudo cpan Crypt::SSLeay

# END
# 	    exit(1);
# 	}
    }
}

sub warn_1 {
    my ($self, $format, @args) = @_;
    chomp($format);
    if ($self->{verbose} >= 1) {
        warn(sprintf($format . "\n", @args));
    }
}

1;                              # End of Geo::GTFS2::Object
