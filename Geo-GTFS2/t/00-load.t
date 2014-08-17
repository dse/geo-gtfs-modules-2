#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'Geo::GTFS2' ) || print "Bail out!\n";
}

diag( "Testing Geo::GTFS2 $Geo::GTFS2::VERSION, Perl $], $^X" );
