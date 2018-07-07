package Geo::GTFS2::Util::Progress;
use warnings;
use strict;
use v5.10.0;

sub new {
    my ($class, %args) = @_;
    my $self = bless(\%args, $class);
    $self->{count} //= 0;
    $self->{frequency} //= 100;
    $self->{progress_message} //= "  %d";
    $self->{completion_message} //= "  %d";
    return $self;
}

sub tick {
    my ($self) = @_;
    $self->{count} += 1;
    if ($self->{count} % $self->{frequency} == 0) {
        $self->printf($self->{progress_message} . "\r", $self->{count});
    }
}

sub printf {
    my ($self, $format, @args) = @_;
    my $save_fh = select STDERR;
    my $save_autoflush = $|;
    $| = 1;
    printf STDERR ($format, @args);
    $| = $save_autoflush;
    select $save_fh;
};

sub done {
    my ($self) = @_;
    if ($self->{count}) {
        $self->printf($self->{completion_message} . "\n", $self->{count});
    }
    $self->{count} = 0;
}

sub DESTROY {
    my ($self) = @_;
    $self->done();
}

1;
