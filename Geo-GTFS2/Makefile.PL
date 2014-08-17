use 5.006;
use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'Geo::GTFS2',
    AUTHOR              => q{Darren Embry <dse@sdf.org>},
    VERSION_FROM        => 'lib/Geo/GTFS2.pm',
    ABSTRACT_FROM       => 'lib/Geo/GTFS2.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Geo-GTFS2-*' },
);
