#!perl -T
use 5.006;
use strict;
use warnings FATAL => 'all';
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'Geo::Modules2' ) || print "Bail out!\n";
}

diag( "Testing Geo::Modules2 $Geo::Modules2::VERSION, Perl $], $^X" );
