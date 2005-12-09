package Sort::External;
use strict;
use warnings;

use 5.006_001;

our $VERSION = '0.16';

use XSLoader;
XSLoader::load('Sort::External', $VERSION);

use File::Temp 'tempdir';
use Fcntl qw( :DEFAULT );
use Carp;
use Clone 'clone';

use constant MAX_SORTFILES => 10;

our %instance_vars = (
    -sortsub       => undef,
    -working_dir   => undef,
    -cache_size    => undef,
    -mem_threshold => 2**20,
    -line_separator => undef, # no-op, backwards compatibility only

    # cleaned up version of -working_dir
    workdir => undef,
    # storage area used by both feed and fetch
    item_cache => [],
    # a place for tempfile handles to accumulate
    sortfiles => [ [] ],
    # a tally of the bytes used by the item_cache
    mem_bytes => 0,
    # a Sort::External::Buffer object, created on the first call to fetch()
    out_buff => undef,
);

sub new {
    my $class = shift;    # leave the rest of @_ intact

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
        my ( $var, $val ) = ( shift, shift );
        croak("Illegal parameter: '$var'")
            unless exists $self->{$var};
        $self->{$var} = $val;
    }

    # create a temp directory
    $self->{workdir} = $self->{-working_dir};
    unless ( defined $self->{workdir} ) {
        $self->{workdir} = tempdir( CLEANUP => 1 );
    }
}

sub feed {
    my $self = shift;

    push @{ $self->{item_cache} }, @_;

    if ( defined $self->{-cache_size} ) {
        # use the number of array elements in the cache as a flush-trigger
        $self->_write_item_cache_to_tempfile
            if @{ $self->{item_cache} } >= $self->{-cache_size};
    }
    else {
        # keep a tally of the cache's approximate memory consumption
        $self->{mem_bytes} += _add_up_lengths(@_);
        $self->_write_item_cache_to_tempfile
            if ( $self->{mem_bytes} > $self->{-mem_threshold} );
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
            my ( $var, $val ) = ( shift, shift );
            croak("Illegal parameter: '$var'")
                unless exists $finish_defaults{$var};
            $args{$var} = $val;
        }

        # get an outfile
        croak('Argument -outfile is required') unless defined $args{-outfile};
        sysopen( OUTFILE, $args{-outfile}, $args{-flags} )
            or croak("Couldn't open outfile '$args{-outfile}': $!");
        print OUTFILE for @{ $self->{item_cache} };

        # print all sortfiles to the outfile, in order
        for my $fh ( @{ $self->{sortfiles}[-1] } ) {
            seek( $fh, 0, 0 );
            my $out_buff = Sort::External::Buffer->_new( tf_handle => $fh );
            while ( $out_buff->_refill_buffer ) {
                for ( @{ $out_buff->{buffarray} } ) {
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
    if ( @{ $self->{item_cache} } ) {
        return shift @{ $self->{item_cache} };
    }
    # otherwise, try to refill the cache...
    elsif ( $self->{out_buff} ) {
        my $out_buff = $self->{out_buff};
        if ( $out_buff->_refill_buffer ) {
            return $self->fetch;
        }
        else {
            undef $self->{out_buff};
        }
    }

    # create a Sort::External::Buffer object to manage the cache
    if ( !$self->{out_buff} ) {
        my $tf_handle;

        # if this fails, we're done, so return undef
        return unless $tf_handle = shift @{ $self->{sortfiles}[-1] };

        seek( $tf_handle, 0, 0 );
        my $out_buff
            = Sort::External::Buffer->_new( tf_handle => $tf_handle, );
        $self->{out_buff}   = $out_buff;
        $self->{item_cache} = $out_buff->{buffarray};    # HACK!

        # loop back, now that the buffer's set up
        return $self->fetch;
    }
}

# flush the items in the input cache to a tempfile, sorting as we go
sub _write_item_cache_to_tempfile {
    my $self       = shift;
    my $item_cache = $self->{item_cache};
    my $sortsub    = $self->{-sortsub};

    return unless @$item_cache;

    # get a new tempfile
    my $tmp = File::Temp->new(
        DIR    => $self->{workdir},
        UNLINK => 1,
    );
    push @{ $self->{sortfiles}[0] }, $tmp;

    # sort the cache
    @$item_cache =
        defined $sortsub
        ? ( sort $sortsub @$item_cache )
        : ( sort @$item_cache );

    # print the sorted cache to the tempfile
    _print_to_sortfile( $tmp, @$item_cache );
    seek( $tmp, 0, 0 );

    # reset variables
    $self->{mem_bytes}  = 0;
    $self->{item_cache} = [];

    # consolidate once every MAX_SORTFILES tempfiles.
    $self->_consolidate_sortfiles
        if @{ $self->{sortfiles}[0] } >= MAX_SORTFILES;
}       

# consolidate sortfiles by interleaving their contents.
sub _consolidate_sortfiles {
    my ($self, $final) = @_;

    # if we've never flushed the cache, perform the final sort in-memory
    if ( $final and !@{ $self->{sortfiles}[-1] } ) {
        my $item_cache = $self->{item_cache};
        my $sortsub    = $self->{-sortsub};
        @$item_cache = $sortsub
            ? sort $sortsub @$item_cache
            : sort @$item_cache;
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
    for my $input_level ( 0 .. $#{ $self->{sortfiles} } ) {
        if ($final) {
            $self->_consolidate_one_level($input_level);
        }
        elsif ( @{ $self->{sortfiles}[$input_level] } > MAX_SORTFILES ) {
            $self->_consolidate_one_level($input_level);
        }
    }
}

# merge multiple sortfiles
sub _consolidate_one_level {
    my ( $self, $input_level ) = @_;
    my $sortsub = $self->{-sortsub};

    # create a holder for the output filehandles if necessary.
    $self->{sortfiles}[ $input_level + 1 ] ||= [];

    # if there's only one sortfile on this level, kick it up and return
    if ( @{ $self->{sortfiles}[$input_level] } == 1 ) {
        push @{ $self->{sortfiles}[ $input_level + 1 ] },
            @{ $self->{sortfiles}[$input_level] };
        $self->{sortfiles}[$input_level] = [];
        return;
    }

    # get a new outfile
    my $outfile = File::Temp->new(
        DIR    => $self->{workdir},
        UNLINK => 1
    );

    # build an array of Sort::External::Buffer objects, one per filehandle
    my @in_buffers;
    for my $tf_handle ( @{ $self->{sortfiles}[$input_level] } ) {
        seek( $tf_handle, 0, 0 );
        my $buff = Sort::External::Buffer->_new( tf_handle => $tf_handle, );
        push @in_buffers, $buff;
    }
    
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

    while (@in_buffers) {
        my @on_the_bubble;

        # discard exhausted buffers, collect endposts
        my @buffer_buffer;
        for my $buff (@in_buffers) {
            if ( !@{ $buff->{buffarray} } ) {
                next unless $buff->_refill_buffer;
            }
            push @on_the_bubble, $buff->{buffarray}[-1];
            push @buffer_buffer, $buff;
        }
        last unless @buffer_buffer;
        @in_buffers = @buffer_buffer;

        # choose the cutoff from among the endpost candidates
        @on_the_bubble = $sortsub
            ? ( sort $sortsub @on_the_bubble )
            : ( sort @on_the_bubble );
        my $cutoff = $on_the_bubble[0];

        # create a closure sub that compares an arg to the cutoff
        my $comparecode;
        if ( defined $sortsub ) {
            $comparecode = sub {
                my $test = shift;
                return 0 if $test eq $cutoff;
                my $winner = ( sort $sortsub( $cutoff, $test ) )[0];
                return $winner eq $cutoff ? 1 : -1;
            };
        }
        else {
            $comparecode = sub {
                return $_[0] cmp $cutoff;
            };
        }

        # build the batch
        my @batch;
        for my $buff (@in_buffers) {
            $buff->_define_range($comparecode);
            next if $buff->{max_in_range} == -1;
            my $num_to_splice = $buff->{max_in_range} + 1;
            push @batch, splice( @{ $buff->{buffarray} }, 0, $num_to_splice );

        }

        # sort the batch and print it to an outfile
        @batch = $sortsub ? ( sort $sortsub @batch ) : ( sort @batch );
        _print_to_sortfile( $outfile, @batch );
    }
            
    # explicity close the filehandles that were consolidated; since they're
    # tempfiles, they'll unlink themselves
    close $_ for @{ $self->{sortfiles}[$input_level] };
    $self->{sortfiles}[$input_level] = [];

    # add the outfile to the next higher level of the sortfiles array
    seek( $outfile, 0, 0 );
    push @{ $self->{sortfiles}[ $input_level + 1 ] }, $outfile;
}

package Sort::External::Buffer;
use strict;
use warnings;

use Clone 'clone';

our %class_defaults = (
    buffarray    => [],
    max_in_range => -1,
    tf_handle    => undef,
);

sub _new {
    my $class = shift;
    my $self  = clone( \%class_defaults );
    %$self = ( %$self, @_ );
    bless $self, $class;
}

# record the highest index in the buffarray representing an element lower than
# a given cutoff.
sub _define_range {
    my ( $self, $comparecode ) = @_;
    my $buffarray = $self->{buffarray};
    my ( $lo, $mid, $hi ) = ( 0, 0, $#$buffarray );

    # divide and conquer
    while ( $hi - $lo > 1 ) {
        $mid = ( $lo + $hi ) >> 1;
        my $delta = &$comparecode( $buffarray->[$mid] );
        if    ( $delta == -1 ) { $lo = $mid }
        elsif ( $delta == 1 )  { $hi = $mid }
        elsif ( $delta == 0 )  { $lo = $hi = $mid }
    }

    # get that last item in...
    while ( $mid < $#$buffarray
        and &$comparecode( $buffarray->[ $mid + 1 ] ) < 1 )
    {
        $mid++;
    }
    while ( $mid >= 0 and $comparecode->( $buffarray->[$mid] ) == 1 ) {
        $mid--;
    }

    # store the index for the last item we'll allow through
    $self->{max_in_range} = $mid;
}

1;

__END__

=head1 NAME

Sort::External - sort huge lists

=head1 SYNOPSIS

    my $sortex = Sort::External->new( -mem_threshold => 2**24 );
    while (<HUGEFILE>) {
        $sortex->feed($_);
    }
    $sortex->finish;
    while ( defined( $_ = $sortex->fetch ) ) {
        &do_stuff_with($_);
    }

=head1 DESCRIPTION

Problem: You have a list which is too big to sort in-memory.  

Solution: "feed, finish, and fetch" with Sort::External, the closest thing to
a drop-in replacement for Perl's sort() function when dealing with
unmanageably large lists.

=head2 How it works

Cache sortable items in memory.  Periodically sort the cache and empty it into
a temporary sortfile.  As sortfiles accumulate, interleave them into larger
sortfiles.  Complete the sort by sorting the input cache and any existing
sortfiles into an output stream.

Note that if Sort::External hasn't yet flushed the cache to disk when finish()
is called, the whole operation completes in-memory.

In the CompSci world, "internal sorting" refers to sorting data in RAM, while
"external sorting" refers to sorting data which is stored on disk, tape,
punchcards, or any storage medium except RAM -- hence, this module's name.

=head2 Stringification

Items fed to Sort::External will be returned in stringified form (assuming
that the cache gets flushed at least once): C<$foo = "$foo">.  Since this is
unlikely to be desirable when objects or deep data structures are involved,
Sort::External throws an error if you feed it anything other than simple
scalars.   

=head2 Taint and UTF-8 flags

Expert: Sort::External does a little extra bookkeeping to sustain each item's
taint and UTF-8 flags through the journey to disk and back.

=head1 METHODS

=head2 new()

    my $sortscheme = sub { $Sort::External::b <=> $Sort::External::a };
    my $sortex = Sort::External->new(
        -mem_threshold   => 2**24,            # default: 2**20 (1Mb)
        -cache_size      => 100_000,          # default: undef (disabled) 
        -sortsub         => $sortscheme,      # default sort: standard lexical
        -working_dir     => $temp_directory,  # default: see below
    );

Construct a Sort::External object.

=over

=item 

B<-mem_threshold> -- Allow the input cache to consume approximately
-mem_threshold bytes before sorting it and flushing to disk.  Experience
suggests that the optimum setting is somewhere between 2**20 and 2**24:
1-16Mb.

=item 

B<-cache_size> -- Specify a hard limit for the input cache in terms of
sortable items.  If set, overrides -mem_threshold. 

=item 

B<-sortsub> -- A sorting subroutine.  Be advised that you MUST use
$Sort::External::a and $Sort::External::b instead of $a and $b in your sub.
Before deploying a sortsub, consider using a GRT instead, as described in
the L<Sort::External::Cookbook|Sort::External::Cookbook>. It's probably
a lot faster.

=item 

B<-working_dir> -- The directory where the temporary sortfiles will reside.
By default, this directory is created using L<File::Temp|File::Temp>'s
tempdir() command.

=back

=head2 feed()

    $sortex->feed( @items );

Feed one or more sortable items to your Sort::External object.  It is normal
for occasional pauses to occur during feeding as caches are flushed and
sortfiles are merged.

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

    while ( defined( $_ = $sortex->fetch ) ) {
        &do_stuff_with($_);
    }

Fetch the next sorted item.  

=head1 BUGS

Please report any bugs or feature requests to
C<bug-sort-external@rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Sort-External>.

=head1 ACKNOWLEDGEMENTS

Bug reports and patches: Ken Clarke, Chris Nandor, Alessandro Ranellucci.

=head1 SEE ALSO

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

