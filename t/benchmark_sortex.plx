#!/usr/bin/perl
use strict;
use warnings;

use Benchmark qw( :hireswallclock );
use Sort::External;

my $version = $Sort::External::VERSION;
$version =~ s/_.*//;

my @sortable;
my @randstringchars = (('A' .. 'Z'),('a' .. 'z'),(0 .. 9)); 
for (1 .. 9999) {
    my $sortable_string ;
    $sortable_string .= $randstringchars[int(rand 62)] for (1 .. 60); 
    $sortable_string .= "\n";
    push @sortable, $sortable_string;
}

my %timeable = (
    cache_level_one_by_one => \&cache_level_one_by_one,
    cache_level_all_at_once => \&cache_level_all_at_once,
    cache_level_hit_disk => \&cache_level_hit_disk,
    );
if ($version > 0.06) { 
    $timeable{mem_thresh_one_by_one}  = \&mem_thresh_one_by_one;
    $timeable{mem_thresh_all_at_once}  = \&mem_thresh_all_at_once;
    $timeable{mem_thresh_hit_disk}  = \&mem_thresh_hit_disk;
}


timethese( 50, \%timeable);
print "Sort::External version $Sort::External::VERSION\n";

sub test_one_by_one {
    my $sortex = shift;
    $sortex->feed( @_ );
    $sortex->feed( $_ ) for @sortable;
    $sortex->finish;
    1 while defined($_ = $sortex->fetch);
}

sub test_all_at_once {
    my $sortex = shift;
    $sortex->feed( @_ );
    $sortex->feed( @sortable );
    $sortex->finish;
    1 while defined($_ = $sortex->fetch);

}

sub mem_thresh_one_by_one {
    my $sortex = Sort::External->new( -mem_threshold => 2**24 );
    &test_one_by_one($sortex);
}

sub mem_thresh_all_at_once {
    my $sortex = Sort::External->new( -mem_threshold => 2**24 );
    &test_all_at_once($sortex);
}

sub cache_level_one_by_one {
    my $sortex = Sort::External->new; 
    &test_one_by_one($sortex);
}

sub cache_level_all_at_once {
    my $sortex = Sort::External->new;
    &test_all_at_once($sortex);
}

sub mem_thresh_hit_disk {
    my $sortex = Sort::External->new( -mem_threshold => 2 ** 18 );
    &test_all_at_once($sortex, @sortable[0,1]);
}

sub cache_level_hit_disk {
    my $sortex = Sort::External->new;
    &test_all_at_once($sortex, @sortable[0,1]);
}

