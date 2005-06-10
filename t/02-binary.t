use strict;
use warnings;

use Test::More tests => 1; 
use File::Spec;

use lib 'lib';
use Sort::External;

my @stuff = map { pack('n', (11_000 - $_)) } ( 0 .. 11_000 );

my $sortex = Sort::External->new(
    -cache_size => 1_000,
    -line_separator => 'random',
    );
$sortex->feed(@stuff);
$sortex->finish;
@stuff = reverse @stuff;
my $item;
my @sorted;
while (defined($item = $sortex->fetch)) {
    push @sorted, $item;
}
is_deeply(\@sorted, \@stuff, "Sorting binary items with random linesep...");

