use strict;
use warnings;

use Test::More tests => 2; 
use File::Spec;

use lib 'lib';
use Sort::External;

my ($sortex, $item, @sorted);

my @orig = map { sprintf("%05d", $_) } ( 0 .. 11_000);
unshift @orig, '';
my @reversed = reverse @orig;

$sortex = Sort::External->new(
    -cache_size => 1_000,
    -line_separator => 'random',
    );
$sortex->feed(@reversed);
$sortex->finish;
while (defined($item = $sortex->fetch)) {
    push @sorted, $item;
}
is_deeply(\@sorted, \@orig, "Sorting binary items with random linesep...");
use Data::Dumper;
undef $sortex;
@sorted = ();

my $linesep = 'hgfedcbxaabcdefgh';
$sortex = Sort::External->new(
    -cache_size => 1_000,
    -line_separator => $linesep,
    );
$sortex->feed(@reversed);
$sortex->finish;
while (defined($item = $sortex->fetch)) {
    push @sorted, $item;
}
is_deeply(\@sorted, \@orig, "Sorting binary items with custom linesep...");
