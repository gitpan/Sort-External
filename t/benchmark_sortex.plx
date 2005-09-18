#!/usr/bin/perl
use strict;
use warnings;

use Carp;
use Benchmark qw( :hireswallclock );
use Sort::External;

croak("usage: ./benchmark_sortex.plx REPS")
    unless @ARGV;

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
    cache_level_hit_disk_one_by_one => \&cache_level_hit_disk_one_by_one,
    cache_level_hit_disk_all_at_once => \&cache_level_hit_disk_all_at_once,
    );
if ($version > 0.06) { 
    $timeable{mem_thresh_one_by_one}  = \&mem_thresh_one_by_one;
    $timeable{mem_thresh_all_at_once}  = \&mem_thresh_all_at_once;
    $timeable{mem_thresh_hit_disk_one_by_one}  
        = \&mem_thresh_hit_disk_one_by_one;
    $timeable{mem_thresh_hit_disk_all_at_once}  
        = \&mem_thresh_hit_disk_all_at_once;
}

timethese( $ARGV[0], \%timeable);
print "Sort::External version $Sort::External::VERSION\n";

sub test_one_by_one {
    my $sortex = shift;
    my $must_match = @_ + @sortable;
    $sortex->feed( @_ );
    $sortex->feed( $_ ) for @sortable;
    $sortex->finish;
    my $count = 0;
    $count++ while defined($_ = $sortex->fetch);
    confess ("mismatch: $must_match $count") unless $must_match == $count;
}

sub test_all_at_once {
    my $sortex = shift;
    my $must_match = @_ + @sortable;
    $sortex->feed( @_ );
    $sortex->feed( @sortable );
    $sortex->finish;
    my $count = 0;
    $count++ while defined($_ = $sortex->fetch);
    confess("mismatch: $must_match $count") unless $must_match == $count;
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

sub mem_thresh_hit_disk_all_at_once {
    my $sortex = Sort::External->new( -mem_threshold => 2 ** 18 );
    &test_all_at_once($sortex, @sortable[0,1]);
}

sub cache_level_hit_disk_all_at_once {
    my $sortex = Sort::External->new;
    &test_all_at_once($sortex, @sortable[0,1]);
}

sub mem_thresh_hit_disk_one_by_one {
    my $sortex = Sort::External->new( -mem_threshold => 2 ** 18 );
    &test_one_by_one($sortex, @sortable[0,1]);
}

sub cache_level_hit_disk_one_by_one {
    my $sortex = Sort::External->new;
    &test_one_by_one($sortex, @sortable[0,1]);
}

