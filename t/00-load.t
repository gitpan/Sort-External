use Test::More tests => 1;

use lib 'lib';

BEGIN {
    use_ok('Sort::External');
}

diag(
    "Testing Sort::External $Sort::External::VERSION, Perl 5.008005, /usr/local/bin/perl"
);
