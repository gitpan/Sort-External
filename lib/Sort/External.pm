package Sort::External;
use strict;
use warnings;

require 5.006_001;

our $VERSION = '0.11_1';

require XSLoader;
XSLoader::load('Sort::External', $VERSION);

use File::Temp 'tempdir';
use Fcntl qw( :DEFAULT );
use Carp;
use Clone 'clone';

use attributes 'reftype';
use bytes;
no  bytes; # import routines, but use character semantics

use constant MAX_SORTFILES => 10;

our %instance_vars = (
    -sortsub                => undef,
    -working_dir            => undef,
    -line_separator         => undef, # no-op, just there to stop crash
    -cache_size             => 10_000,
    -mem_threshold          => 0, 
    -verbosity              => 1,
    
    # cleaned up versions of -working_dir and -line_separator
    workdir                 => undef,
    # storage area used by both feed and fetch
    item_cache              => [],
    # a place for tempfile handles to accumulate
    sortfiles               => [ [] ],
    # a tally of the bytes used by the item_cache
    mem_bytes               => 0,
    # a Sort::External::Buffer object, created on the first call to fetch()
    out_buff                => undef,
);

sub new {
    my $class = shift; # leave the rest of @_ intact
    
    # clone the instance_vars hash and bless it
    $class = ref($class) || $class; 
    my $defaults;
    {
        no strict 'refs'; 
        $defaults = \%{ $class . '::instance_vars' };
    }
    my $self = clone($defaults);
    bless $self, $class; 

    $self->init_instance(@_);

    return $self;
}

sub init_instance {
    my $self = shift;

    # verify args
    while (@_) {
        my ($var, $val) = (shift, shift);
        croak("Illegal parameter: '$var'") 
            unless exists $self->{$var};
        $self->{$var} = $val;
    }

    # create a temp directory
    $self->{workdir} = $self->{-working_dir};
    unless (defined $self->{workdir}) {
        $self->{workdir} = tempdir( CLEANUP => 1 ); 
    }

    croak("custom -line_separator capabilities have been removed")
        if (defined $self->{-line_separator} 
            and $self->{-line_separator} ne 'random'); 
        
}

sub feed {
    my $self = shift;
    
    push @{ $self->{item_cache} }, @_;

    if ($self->{-mem_threshold}) {
        # keep a tally of the cache's approximate memory consumption
        $self->{mem_bytes} += _add_up_lengths(@_);
        $self->_write_item_cache_to_tempfile
            if ($self->{mem_bytes} > $self->{-mem_threshold});
    }
    # use the number of array elements in the cache as a flush-trigger
    elsif (@{ $self->{item_cache} } >= $self->{-cache_size}) {
        $self->_write_item_cache_to_tempfile;
    }
}

my %finish_defaults = (
    -outfile => undef,
    -flags   => (O_CREAT | O_EXCL | O_WRONLY),
);

sub finish {
    my $self = shift;
    
    $self->_consolidate_sortfiles('final');
        
    # if called with arguments, we must be printing everything to an outfile
    if (@_) {
        # verify args
        my %args = %finish_defaults;
        while (@_) {
            my ($var, $val) = (shift, shift);
            croak("Illegal parameter: '$var'") 
                unless exists $finish_defaults{$var};
            $args{$var} = $val;
        }

        # get an outfile
        croak('Argument -outfile is required') unless defined $args{-outfile};
        sysopen(OUTFILE, $args{-outfile}, $args{-flags})
            or croak ("Couldn't open outfile '$args{-outfile}': $!");
        print OUTFILE for @{ $self->{item_cache} };

        # print all sortfiles to the outfile, in order
        for my $fh (@{ $self->{sortfiles}[-1] }) {
            seek ($fh, 0, 0);
            my $out_buff = Sort::External::Buffer->_new(
                tf_handle => $fh,
            );
            while ($out_buff->_refill_buffer) {
                for(@{ $out_buff->{buffarray} }) {
                    print OUTFILE
                        or croak("Couldn't print to '$args{-outfile}': $!");
                }
                $out_buff->{buffarray} = [];
            }
        }
        close OUTFILE or croak("Couldn't close '$args{-outfile}': $!");
    }
}

sub fetch {
    my $self = shift;
    # if the cache has items in it, return one...
    if (@{ $self->{item_cache} }) {
        return shift @{ $self->{item_cache} };
    }
    # otherwise, try to refill the cache...
    elsif ($self->{out_buff}) {
        my $out_buff = $self->{out_buff};
        if ($out_buff->_refill_buffer) {
            return $self->fetch;
        }
        else {
            undef $self->{out_buff};
        }
    }

    # create a Sort::External::Buffer object to manage the cache
    if (!$self->{out_buff}) {
        my $tf_handle;
        
        # if this fails, we're done, so return undef
        return unless $tf_handle = shift @{ $self->{sortfiles}[-1] };

        seek($tf_handle, 0, 0);
        my $out_buff = Sort::External::Buffer->_new(
            tf_handle => $tf_handle,
        );
        $self->{out_buff} = $out_buff;
        $self->{item_cache} = $out_buff->{buffarray}; # HACK!

        # loop back, now that the buffer's set up
        return $self->fetch;
    }
}

# flush the items in the input cache to a tempfile, sorting as we go
sub _write_item_cache_to_tempfile {
    my $self = shift;
    my $item_cache = $self->{item_cache};
    my $sortsub = $self->{-sortsub};

    return unless @$item_cache;

    # get a new tempfile
    my $tmp = File::Temp->new(
        DIR => $self->{workdir},
        UNLINK => 1,
    );  
    push @{ $self->{sortfiles}[0] }, $tmp;

    # sort the cache
    @$item_cache = defined $sortsub ?
                   (sort $sortsub @$item_cache) :
                   (sort @$item_cache);

    # print the sorted cache to the tempfile
    _print_to_sortfile( $tmp, @$item_cache ); 
    seek($tmp, 0, 0);
    
    # reset variables
    $self->{mem_bytes} = 0;
    $self->{item_cache} = [];

    # consolidate once every MAX_SORTFILES tempfiles.
    $self->_consolidate_sortfiles
        if @{ $self->{sortfiles}[0] } >= MAX_SORTFILES;
}       

# consolidate sortfiles by interleaving their contents.
sub _consolidate_sortfiles {
    my $self = shift;
    my $final = shift || '';

    # if we've never flushed the cache, perform the final sort in memory
    if ($final and !@{ $self->{sortfiles}[-1] }) {
        my $item_cache = $self->{item_cache};
        my $sortsub = $self->{-sortsub};
        @$item_cache = $sortsub ?
                       sort $sortsub @$item_cache : 
                       sort @$item_cache;
        return;
    }
    
=begin comment

Each element of the sortfiles array is an array of filehandles.  Higher levels
of the have been through more stages of consolidation. 

If this is the last sort, we need to end up with one final group of sorted
files (which, if concatenated, would create one giant sortfile).  This final
sort is available as an array of filehandles: @{ $self->{sortfiles}[-1] }

=end comment
=cut

    $self->_write_item_cache_to_tempfile;
    for my $input_level (0 .. $#{$self->{sortfiles} }) {
        if ($final) {
            $self->_consolidate_one_level($input_level);
        }
        elsif ( @{ $self->{sortfiles}[$input_level] } > MAX_SORTFILES) {
            $self->_consolidate_one_level($input_level);
        }
    }
}

# merge multiple sortfiles
sub _consolidate_one_level {
    my $self = shift;
    my $input_level = shift;
    
    my $sortsub = $self->{-sortsub};
    
    # create a holder for the output filehandles if necessary.
    $self->{sortfiles}[$input_level + 1] ||= [];

    # if there's only one sortfile on this level, kick it up and return
    if (@{ $self->{sortfiles}[$input_level] } == 1) {
        push @{ $self->{sortfiles}[$input_level + 1] }, 
            @{ $self->{sortfiles}[$input_level] };
        $self->{sortfiles}[$input_level] = [];
        return;
    }

    # get a new outfile
    my $outfile = File::Temp->new(
        DIR => $self->{workdir}, 
        UNLINK  => 1
    );
    
    # build an array of Sort::External::Buffer objects, one per filehandle
    my @in_buffers;
    for my $tf_handle (@{ $self->{sortfiles}[$input_level] }) {
        seek($tf_handle, 0, 0);
        my $buff = Sort::External::Buffer->_new(
            tf_handle   => $tf_handle,
        );
        push @in_buffers, $buff;
    }
    
    BIGBUFF: while (@in_buffers) {
        my @on_the_bubble;

        # discard exhausted buffers, collect endposts
        my @buffer_buffer;
        for my $buff (@in_buffers) {
            next unless $buff->_refill_buffer;
            push @on_the_bubble, $buff->{buffarray}[-1];
            push @buffer_buffer, $buff;
        }
        last unless @buffer_buffer;
        @in_buffers = @buffer_buffer;

=begin comment

Our goal is to merge multiple sortfiles, but we don't have access to all the
elements in a given sortfile, only to Buffer objects which allow us to "see"
a limited subset of those elements.  

A traditional way to merge several files is to expose one element per file,
compare them, and pop one item per pass.  However, it is more efficient,
especially in Perl, if we sort batches.

It would be convenient if we could just dump all the buffered elements into a
batch and sort that, but we can't just pool all the buffers, because it's
possible that the elements in buffer B are all higher in sort order than all
the elements we can "see" in buffer A, plus some more elements in sortfile A
we can't "see" yet.  We need a gatekeeper algo which allows the maximum number
of elements into a sortbatch, while blocking elements which we can't be
certain belong in this sortbatch.

Our gatekeeper needs to know a cutoff point.  To find it, we need to examine
the last element we can "see" in each buffer, and figure out which of these
endposts is the lowest in sort order.  Once we know that cutoff, we can
compare it against items in the other buffers, and any element which is lower
than or equal to the cutoff in sort order gets let through into the batch.

=end comment
=cut

        @on_the_bubble = $sortsub ?
                         (sort $sortsub @on_the_bubble) :
                         (sort @on_the_bubble);
        my $cutoff = $on_the_bubble[-1];

        # create a closure sub that compares an arg to the cutoff
        my $comparecode;
        if (defined $sortsub) {
            $comparecode = sub {
                my $test = shift;
                return 0 if $test eq $cutoff;
                my $winner = (sort $sortsub($cutoff, $_[0]))[0];
                return $winner eq $cutoff ? 1 : -1;
            }
        }
        else {
            $comparecode = sub {
                return $_[0] cmp $cutoff;
            }
        }

        # build the batch
        my @batch;
        for my $buff (@in_buffers) {
            $buff->_define_range($comparecode);
            next if $buff->{max_in_range} == -1;
            my $num_to_splice = $buff->{max_in_range} + 1;
            push @batch, splice( @{ $buff->{buffarray} }, 0, $num_to_splice);

        }
        
        # sort the batch and print it to an outfile
        @batch = $sortsub ? (sort $sortsub @batch) : (sort @batch);
        _print_to_sortfile( $outfile, @batch ); 
    }
            
    # explicity close the filehandles that were consolidated; since they're
    # tempfiles, they'll unlink themselves
    close $_ for @{ $self->{sortfiles}[$input_level] };
    $self->{sortfiles}[$input_level] = [];
    
    # add the filehandle for the outfile to the next higher level of the
    # sortfiles array
    seek($outfile, 0, 0);
    push @{ $self->{sortfiles}[$input_level + 1] }, 
        $outfile;
}

package Sort::External::Buffer;
use strict;
use warnings;

=begin comment

This is a helper class with no public interface.  
Do not use it on its own.

=end comment
=cut

use Clone 'clone';

our %class_defaults = (
    buffarray    => [],
    max_in_range => -1,
    tf_handle    => undef,
    );

sub _new {
    my $class = shift;
    my $self = clone(\%class_defaults);
    %$self = (%$self, @_);
    bless $self, $class;
}

# record the highest index in the buffarray representing an element lower than
# a given cutoff.
sub _define_range {
    my $self = shift;
    my $comparecode = shift;
    my $buffarray = $self->{buffarray};
    my $top = 0;
    my $tail = $#$buffarray;

    # divide and conquer
    while ($tail - $top > 1) {
        my $test = int(($tail + $top + 1)/2);
        my $result = &$comparecode($buffarray->[$test]);
        if ($result == -1) {
            $top = $test;
        }
        elsif ($result == 1) {
            $tail = $test;
        }
        elsif ($result == 0) {
            $top = $tail = $test;
        }
    }

    # get that last item in...
    while ($top < $#$buffarray and &$comparecode($buffarray->[$top + 1]) < 1){
        $top++;
    }

    # check to see if ALL of the array elements are outside the range
    if ($top == 0) {
        if (&$comparecode($buffarray->[0]) == 1) {
            $top = -1;
        }
    }

    # store the index for the last item we'll allow through
    $self->{max_in_range} = $top;
}

1;

__END__

=head1 NAME

Sort::External - sort huge lists

=head1 SYNOPSIS

    my $sortex = Sort::External->new( -mem_threshold => 2**24 );
    while (<HUGEFILE>) {
        $sortex->feed( $_ );
    }
    $sortex->finish;
    while (defined($_ = $sortex->fetch)) {
        &do_stuff_with( $_ );
    }

=head1 DESCRIPTION

Problem: You have a list which is too big to sort in-memory.  

Solution: "feed, finish, and fetch" with Sort::External, the closest thing to
a drop-in replacement for Perl's sort() function when dealing with
unmanageably large lists.

=head2 Where's the sortex() function?

In a perfect world, Sort::External would export a sortex() function that you
could swap with sort() and be done.  That isn't possible, because it would
have to return a list which would, in all likelihood, be too large to fit in
memory -- otherwise, why use Sort::External in the first place?  

=head2 How it works

Cache sortable items in memory.  Periodically sort the cache and empty it into
a temporary sortfile.  As sortfiles accumulate, interleave them into larger
sortfiles.  Complete the sort by sorting the input cache and any existing
sortfiles into an output stream.

In the CS world, "internal sorting" refers to sorting data in RAM, while
"external sorting" refers to sorting data which is stored on disk, tape,
punchcards, or any storage medium except RAM -- hence, this module's name.

Note that if Sort::External hasn't yet flushed the cache to disk when finish()
is called, the whole operation completes in-memory.

=head2 undefs and refs

Perl's sort() function sorts undef values to the front of a list -- it will
complain if you have warnings enabled, but it preserves their undef-ness.
sort() also preserves references.  In contrast, Sort::External's behavior is
unpredictable and almost never desirable when you feed it either undefs or
refs.  If you really care about sorting lists containing undefs or refs,
you'll have to symbollically replace and restore them yourself.

=head2 Subtle changes to scalars e.g. utf8 flags get stripped

Once the input cache grows large enough, Sort::External writes items to disk
and throws them away, only to recreate them later by reading back from disk.
Provided that the sort does not complete in-memory, the stringified return
scalars you get back from Sort::External will have changed in one subtle
respect from their precursors: if they were tagged as utf8 before, they won't
be now. (There are other subtle changes, but they don't matter unless you're
working at the L<perlguts|perlguts> level, in which case you know what to
expect.)

=head2 Memory management

Sort::External functions best when it can accumulate a large input cache
before sorting the cache and flushing it to disk.  For backwards
compatibility, the default trigger for a buffer flush is the accumulation of
10,000 array items, which may be too high if your items are large.  However,
starting at version 0.10, Sort::External implements -mem_threshold, an
*experimental* feature which makes it possible to flush to disk when the
amount of memory consumed by the input cache exceeds a certain number of
bytes.

There are two important caveats to keep in mind about -mem_threshold.  First,
Sort::External uses an extremely crude algorithm to assess memory consumption:
the length of each scalar in bytes, plus an extra 15 per item to account for
the overhead of a typical scalar.  This is by no means accurate, but it
provides a meaningful level of control.  Second, the amount of memory consumed
by the Sort::External process will be no less than double what you set for
-mem_threshold, and probably considerably more.  Don't get too aggressive,
because the only time a very high -mem_threshold provides outsized benefits is
when it's big enough that it prevents a disk flush -- in which case, you might
just want to use sort(). 

=head1 METHODS

=head2 new()

    my $sortscheme = sub { $Sort::External::b <=> $Sort::External::a };
    my $sortex = Sort::External->new(
        -sortsub         => $sortscheme,      # default sort: standard lexical
        -working_dir     => $temp_directory,  # default: see below
        -mem_threshold   => 2 ** 24,          # default: 0 (inactive)
        -cache_size      => 100_000,          # default: 10_000
    );

Construct a Sort::External object.

=over

=item 

B<-sortsub> -- A sorting subroutine.  Be advised that you MUST use
$Sort::External::a and $Sort::External::b instead of $a and $b in your sub.
Before deploying a sortsub, consider using a packed-default sort instead,
as described in the L<Sort::External::Cookbook|Sort::External::Cookbook>. It's
probably faster.

=item 

B<-working_dir> -- The directory where the temporary sortfiles will reside.
By default, this directory is created using L<File::Temp|File::Temp>'s
tempdir() command.

=item 

B<-mem_threshold> -- EXPERIMENTAL FEATURE.  Allow the input cache to grow to
-mem_threshold bytes before sorting it and flushing to disk.  Suggested value:
Start somewhere between 2**20 and 2**24: 1-16 Mb.

=item 

B<-cache_size> -- The size for Sort::External's input cache, in sortable
items.  If -mem_threshold is enabled, -cache_size is ignored.

=back

=head2 feed()

    $sortex->feed( @items );

Feed one or more sortable items to your Sort::External object.  It is normal
for occasional pauses to occur as caches are flushed and sortfiles are merged.

=head2 finish() 

    # if you intend to call fetch...
    $sortex->finish; 
    
    # otherwise....
    use Fcntl;
    $sortex->finish( 
        -outfile => 'sorted.txt',
        -flags => (O_CREAT | O_WRONLY),
    );

Prepare to output items in sorted order.

If you specify the parameter -outfile, Sort::External will attempt to write
your sorted list to that location.  By default, Sort::External will refuse to
overwrite an existing file; if you want to override that behavior, you can
pass Fcntl flags to finish() using the optional -flags parameter.

Note that you can either finish() to an -outfile, or finish() then fetch()...
but not both.  

=head2 fetch()

    while (defined ($_ = $sortex->fetch)) {
        &do_stuff_with( $_ );
    }

Fetch the next sorted item.  

=head1 BUGS

Please report any bugs or feature requests to
C<bug-sort-external@rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Sort-External>.

=head1 ACKNOWLEDGEMENTS

Bug reports and patches: Ken Clarke, Chris Nandor.

=head2 SEE ALSO

The L<Sort::External::Cookbook|Sort::External::Cookbook>.

L<File::Sort|File::Sort>, L<File::MergeSort|File::MergeSort>, and 
L<Sort::Merge|Sort::Merge> as possible alternatives.

=head1 AUTHOR

Marvin Humphrey E<lt>marvin at rectangular dot comE<gt>
L<http://www.rectangular.com>

=head1 COPYRIGHT

Copyright (c) 2005 Marvin Humphrey.  All rights reserved.
This module is free software.  It may be used, redistributed and/or 
modified under the same terms as Perl itself.

=cut

