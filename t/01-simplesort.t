use strict;
use warnings;

use Test::More tests => 7; 
use File::Spec;

use Sort::External;

my @letters = map { "$_\n" } ( 'a' .. 'z' );
my @reversed_letters = reverse @letters;

my ($sortex, $sort_output);

$sortex = Sort::External->new;
$sort_output = &reverse_letter_test($sortex);
is_deeply($sort_output, \@letters, "sort z .. a as a .. z");
undef $sortex;

$sortex = Sort::External->new(
    -sortsub => sub { $Sort::External::a cmp $Sort::External::b }
    );
$sort_output = &reverse_letter_test($sortex);
is_deeply($sort_output, \@letters, "... with explicit sortsub");
undef $sortex;


$sortex = Sort::External->new(
    -working_dir => File::Spec->curdir,
    );
$sort_output = &reverse_letter_test($sortex);
is_deeply($sort_output, \@letters, "... with a different working directory");
undef $sortex;

$sortex = Sort::External->new(
    -cache_size => 2,
    );
$sort_output = &reverse_letter_test($sortex);
is_deeply($sort_output, \@letters, "... with an absurdly low cache setting");
undef $sortex;

$sortex = Sort::External->new;
$sortex->feed( $_ ) for @reversed_letters;
$sortex->finish( -outfile => 'sortfile.txt' );
open SORTFILE, "sortfile.txt" or die "Couldn't open file 'sortfile.txt': $!";
$sort_output = [ <SORTFILE> ];
close SORTFILE;
is_deeply($sort_output, \@letters, "... to an outfile without hitting temp");
undef $sortex;
unlink "sortfile.txt" or die "Couldn't unlink file 'sortfile.txt': $!";;

$sortex = Sort::External->new(
    -cache_size => 2,
    );
$sortex->feed( $_ ) for @reversed_letters;
$sortex->finish( -outfile => 'sortfile.txt' );
open SORTFILE, "sortfile.txt" or die "Couldn't open file 'sortfile.txt': $!";
$sort_output = [ <SORTFILE> ];
is_deeply($sort_output, \@letters, "... to an outfile with a low enough " .
    "cache setting to hit temp");
undef $sortex;
unlink "sortfile.txt" or die "Couldn't unlink file 'sortfile.txt': $!";;

$sortex = Sort::External->new;
$sortex->finish;
undef $sort_output;
$sort_output = $sortex->fetch;
is($sort_output, undef, "Sorting nothing returns nothing");
undef $sortex;

sub reverse_letter_test {
    my $sortex_object = shift;
    $sortex_object->feed( $_ ) for @reversed_letters;
    $sortex_object->finish;
    my @sorted;
    while( my $stuff = $sortex_object->fetch ) {
        push @sorted, $stuff;
    }
    return \@sorted;
}