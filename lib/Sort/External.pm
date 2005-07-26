package Sort::External;
use strict;
use warnings;

require 5.006_001;

our $VERSION = '0.10_3';

use File::Temp 'tempdir';
use Devel::Size qw( size total_size );
use Fcntl qw( :DEFAULT );
use Carp;
use Clone 'clone';

use bytes;
no bytes; # Import routines, but use character semantics.

### Coding convention:
### Public methods use hash style parameters except when to do so would cause 
### a significant performance penalty.  The parameter names are prepended with
### a dash, e.g. '-foo'.  When it is necessary to modify a parameter value
### for use, it is copied into a similarly named variable without a dash: e.g.
### $self->{-working_dir} gets copied to $self->{workdir}.

### The maximum size that temp files are allowed to attain.
use constant MAX_FS => 2 ** 31; # 2 Gbytes;

##############################################################################
### Class defaults 
##############################################################################
our %class_defaults = (
    -sortsub                => undef,
    -working_dir            => undef,
    -line_separator         => undef,
    -cache_size             => 10_000,
    -mem_threshold          => 0, 
    -verbosity              => 1,
    
    ### Cleaned up versions of -working_dir and -line_separator
    workdir                 => undef,
    linesep                 => undef,
    ### Storage area used by both feed and fetch.
    ### May become shared with $self->{out_buff}{buffarray}
    item_cache              => [],
    ### A place for tempfile handles to accumulate.
    sortfiles               => [ [] ],
    ### The number of sortfiles at one level.  Can grow.  See further comments
    ### in _consolidate_one_level() 
    max_sortfiles           => 10,
    ### A tally of the bytes used by the item_cache.
    mem_bytes               => 0,
    ### A Sort::External::Buffer object, created on the first call to fetch()
    out_buff                => undef,
    );

##############################################################################
### Constructor
##############################################################################
sub new {
    my $class = shift;
    $class = ref($class) || $class;
    my $self = clone(\%class_defaults);
    bless $self, $class;
    $self->_init_sort_external(@_);
    return $self;
}

##############################################################################
### Initialize a Sort::External object.
##############################################################################
sub _init_sort_external {
    my $self = shift;
    while (@_) {
        my ($var, $val) = (shift, shift);
        croak("Illegal parameter: '$var'") 
            unless exists $class_defaults{$var};
        $self->{$var} = $val;
    }

    ### Create a temp directory.
    $self->{workdir} = $self->{-working_dir};
    unless (defined $self->{workdir}) {
        $self->{workdir} = tempdir( CLEANUP => 1 ); 
    }
    
    ### If a 'random' -line_separator was called for, generate it.
    if (!defined $self->{-line_separator}) {
        $self->{linesep} = undef;
    }
    elsif ($self->{-line_separator} eq 'random') {
        my @randstringchars = (('A' .. 'Z'),('a' .. 'z'),(0 .. 9));
        my $linesep = '';
        $linesep .= $randstringchars[rand @randstringchars] for (1 .. 16);
        $self->{linesep} = $linesep;
    }
    else {
        $self->{linesep} = $self->{-line_separator};
    }
}

##############################################################################
### Feed one or more items to the Sort::External object.
##############################################################################
sub feed {
    my $self = shift;
    ### Keep a running tally of the cache's approximate memory consumption.
    if ($self->{-mem_threshold}) {
        $self->{mem_bytes} += total_size(\@_);
        push @{ $self->{item_cache} }, @_;
        $self->_write_item_cache_to_tempfile
            if ($self->{mem_bytes} > $self->{-mem_threshold});
        return;
    }

    ### If -mem_threshold wasn't specified, use the number of array elements
    ### in the cache as a buffer flush-trigger instead.
    push @{ $self->{item_cache} }, @_;
    return unless @{ $self->{item_cache} } >= $self->{-cache_size};
    $self->_write_item_cache_to_tempfile;
}

my %finish_defaults = (
    -outfile => undef,
    -flags   => (O_CREAT | O_EXCL | O_WRONLY),
    );

##############################################################################
### Sort all items, to an outfile if desired.
##############################################################################
sub finish {
    my $self = shift;
    
    $self->_consolidate_sortfiles('final');
        
    ### If called with arguments, we must be printing everything to an
    ### outfile.
    if (@_) {
        my %args = %finish_defaults;
        while (@_) {
            my ($var, $val) = (shift, shift);
            croak("Illegal parameter: '$var'") 
                unless exists $finish_defaults{$var};
            $args{$var} = $val;
        }

        ### Get an outfile.
        croak('Argument -outfile is required') unless defined $args{-outfile};
        sysopen(OUTFILE, $args{-outfile}, $args{-flags})
            or croak ("Couldn't open outfile '$args{-outfile}': $!");
        print OUTFILE for @{ $self->{item_cache} };

        ### Print all sortfiles to the outfile, in order.
        for my $fh (@{ $self->{sortfiles}[-1] }) {
            seek ($fh, 0, 0);
            my $entry;
            if (defined $self->{linesep}) {
                local $/ = $self->{linesep};
                while (defined($entry = (<$fh>))) {
                    chomp $entry;
                    print OUTFILE $entry
                        or croak("Couldn't print to '$args{-outfile}': $!");
                }
            }
            else {
                while (defined($entry = (<$fh>))) {
                    print OUTFILE $entry
                        or croak("Couldn't print to '$args{-outfile}': $!");
                }
            }
        }
        close OUTFILE or croak("Couldn't close '$args{-outfile}': $!");
    }
}

##############################################################################
### Retrieve items in sorted order.
##############################################################################
sub fetch {
    my $self = shift;
    ### If the cache has items in it, return one...
    if (@{ $self->{item_cache} }) {
        return shift @{ $self->{item_cache} };
    }
    ### otherwise, try to refill the cache...
    elsif ($self->{out_buff}) {
        my $out_buff = $self->{out_buff};
        if ($out_buff->_refill_buffer) {
            return $self->fetch;
        }
        else {
            undef $self->{out_buff};
        }
    }
    ### Create a Sort::External::Buffer object to manage the cache.
    if (!$self->{out_buff}) {
        my $tf_handle;
        return unless $tf_handle = shift @{ $self->{sortfiles}[-1] };
        seek($tf_handle, 0, 0);
        my $out_buff = Sort::External::Buffer->_new(
            tf_handle => $tf_handle,
            linesep   => $self->{linesep},
            );
        
        $self->{out_buff} = $out_buff;
        ### HACK!
        $self->{item_cache} = $out_buff->{buffarray};
        return $self->fetch;
    }
}

##############################################################################
### Flush the items in the input cache to a tempfile, sorting as we go.
##############################################################################
sub _write_item_cache_to_tempfile {
    my $self = shift;
    my $item_cache = $self->{item_cache};
    my $sortsub = $self->{-sortsub};
    my $linesep = $self->{linesep};

    return unless @$item_cache;

    ### Get a new tempfile
    my $tmp = File::Temp->new(
        DIR => $self->{workdir},
        UNLINK => 1,
    );  
    push @{ $self->{sortfiles}[0] }, $tmp;

    ### Sort the cache.
    @$item_cache = defined $sortsub ?
                   (sort $sortsub @$item_cache) :
                   (sort @$item_cache);

    ### Print the sorted cache to the tempfile.
    if (defined $linesep) {
        my $printbuff = join $linesep, @$item_cache;
        print $tmp $printbuff, $linesep 
            or croak("Print to $tmp failed: $!");
    }
    else {
        for (@$item_cache) {
            print $tmp $_
                or croak("Print to $tmp failed: $!");
        }
    }
    
    ### Reset variables.
    $self->{mem_bytes} = 0;
    $self->{item_cache} = [];

    ### Consolidate once every 10 tempfiles.
    $self->_consolidate_sortfiles
        if @{ $self->{sortfiles}[0] } >= 10;
}       

##############################################################################
### Consolidate sortfiles by interleaving their contents.
##############################################################################
sub _consolidate_sortfiles {
    my $self = shift;
    my $final = shift || '';

    ### If we've never flushed the cache to disk, perform the final sort in
    ### memory.
    if ($final and !@{ $self->{sortfiles}[-1] }) {
        my $item_cache = $self->{item_cache};
        my $sortsub = $self->{-sortsub};
        @$item_cache = $sortsub ?
                       sort $sortsub @$item_cache : 
                       sort @$item_cache;
        return;
    }
    
    ### Higher levels of the sortfiles array have been through more
    ### stages of consolidation.  
    ###
    ### If this is the last sort, we need to end up with one final group of 
    ### sorted files (which, if concatenated, would create one giant 
    ### sortfile).
    ### 
    ### The final sort is available as an array of filehandles: 
    ### @{ $self->{sortfiles}[-1] }
    $self->_write_item_cache_to_tempfile;
    for my $input_level (0 .. $#{$self->{sortfiles} }) {
        if ($final) {
            $self->_consolidate_one_level($input_level);
        }
        elsif ( @{ $self->{sortfiles}[$input_level] } >
            $self->{max_sortfiles}) 
        {
            $self->_consolidate_one_level($input_level);
        }
    }
}

##############################################################################
### Do the hard work for _consolidate_sortfiles().
##############################################################################
sub _consolidate_one_level {
    my $self = shift;
    my $input_level = shift;
    
    my $sortsub = $self->{-sortsub};
    my $linesep = $self->{linesep};

    local $/ = $linesep 
        if defined $linesep;
    
    ### Create a holder for the output filehandles if necessary.
    $self->{sortfiles}[$input_level + 1] ||= [];

    my @outfiles;
    ### Get a new outfile
    my $outfile = File::Temp->new(
        DIR => $self->{workdir}, 
        UNLINK  => 1
        );
    push @outfiles, $outfile;
    my $outfile_length = 0;
    
    ### Build an array of Sort::External::Buffer objects, one per filehandle.
    my @in_buffers;
    for my $tf_handle (@{ $self->{sortfiles}[$input_level] }) {
        seek($tf_handle, 0, 0);
        my $buff = Sort::External::Buffer->_new(
            linesep     => $self->{linesep},
            tf_handle   => $tf_handle,
        );
        push @in_buffers, $buff;
    }
    
    my $joiner = defined $linesep ? $linesep : '';
    BIGBUFF: while (@in_buffers) {
        my @on_the_bubble;

        ### Discard exhausted buffers.
        my @buffer_buffer;
        for my $buff (@in_buffers) {
            next unless $buff->_refill_buffer;
            push @on_the_bubble, $buff->{buffarray}[-1];
            push @buffer_buffer, $buff;
        }
        last unless @buffer_buffer;
        @in_buffers = @buffer_buffer;

        ### Each Buffer object holds a partial slice of the items in its
        ### sortfile.  
        ### We're going to put as many items into the batch to be sorted this
        ### loop iteration as we can.  To do so, we need to examine the end of
        ### each slice, and figure out which of these endposts is the lowest
        ### in sort order.  Once we know that endpost, we can compare all the
        ### other items in the other buffers, and if they are lower in sort
        ### order than the endpost, they get let through into the batch.
        @on_the_bubble = $sortsub ?
                         (sort $sortsub @on_the_bubble) :
                         (sort @on_the_bubble);
        my $thresh = $on_the_bubble[-1];
        ### Produce a sub on the fly that can be used to compare elements to
        ### the endpost.
        my $comparecode;
        if (defined $sortsub) {
            $comparecode = sub {
                my $test = shift;
                return 0 if $test eq $thresh;
                my $winner = (sort $sortsub($thresh, $_[0]))[0];
                return $winner eq $thresh ? 1 : -1;
            }
        }
        else {
            $comparecode = sub {
                return $_[0] cmp $thresh;
            }
        }

        ### Build the batch.
        my @batch;
        for my $buff (@in_buffers) {
            $buff->_define_range($comparecode);
            next if $buff->{max_in_range} == -1;
            my $num_to_splice = $buff->{max_in_range} + 1;
            push @batch, splice( @{ $buff->{buffarray} }, 0, $num_to_splice);

        }
        
        ### Sort the batch and print it to an outfile, opening a new outfile
        ### if necessary.
        @batch = $sortsub ? (sort $sortsub @batch) : (sort @batch);
        my $outbuffer = join $joiner, @batch;
        $outfile_length += bytes::length($outbuffer) + bytes::length($joiner);
        if ($outfile_length > MAX_FS) {
            $outfile = File::Temp->new(
                DIR => $self->{workdir},
                UNLINK  => 1
                );
            push @outfiles, $outfile;
            $outfile_length = 
                bytes::length($outbuffer) + bytes::length($joiner);
        }
        print $outfile $outbuffer, $joiner;
    }
            
    ### Explicity close the filehandles that were consolidated; since they're
    ### tempfiles, they'll unlink themselves.
    close $_ for @{ $self->{sortfiles}[$input_level] };
    $self->{sortfiles}[$input_level] = [];
    
    ### Add the filehandle for the outfile(s) to the next higher level of the
    ### sortfiles array.
    push @{ $self->{sortfiles}[$input_level + 1] }, 
        @outfiles;
    ### If there's more than one outfile, we're exceeding the 
    ### max filesize limit.  Increase the maximum number of files required 
    ### to trigger a sort, so that we don't sort endlessly.
    $self->{max_sortfiles} += ($#outfiles);
}

package Sort::External::Buffer;
use strict;
use warnings;

### This is a helper class with no public interface.  
### Do not use it on its own.

use Clone 'clone';

##############################################################################
### Class defaults
##############################################################################

our %class_defaults = (
    buffarray    => [],
    max_in_range => -1,
    read_buffer  => undef,
    read_pos     => 0,
    tf_handle    => undef,
    linesep      => undef,
    );

##############################################################################
### Constructor
##############################################################################
sub _new {
    my $class = shift;
    my $self = clone(\%class_defaults);
    %$self = (%$self, @_);
    bless $self, $class;
}

##############################################################################
### Read items out a temp file and into an array.
##############################################################################
sub _refill_buffer {
    my $self = shift;
    my $tf_handle = $self->{tf_handle};
    my $buffarray = $self->{buffarray};

    ### Read data in 32k chunks.
    ### Strangely, bigger chunks can hinder performance.
    ###
    ### Keep reading in 32k chunks until at least one complete line is in the
    ### buffarray (in case the lines are really long). 
    my $starter = 1; 
    while (@$buffarray < 2 or $starter) {
        if ($starter or defined ($self->{read_buffer})) {
            seek($tf_handle, $self->{read_pos}, 0);
            my $offset = defined $self->{read_buffer} ? 
                bytes::length($self->{read_buffer}) : 0;
            if ($offset < 2**15) {
                read($tf_handle, $self->{read_buffer}, 2**15, $offset);
                $self->{read_pos} += 2**15;
            }
            ### split() is nice and efficient, if we know a fixed linesep...
            if (defined $self->{linesep}) {
                ### The -1 is crucial.
                push @$buffarray, 
                    (split $self->{linesep}, $self->{read_buffer}, -1);
                ### If the end of the read buffer was aligned with a linesep, 
                ### The last item is an empty string.  If it wasn't, then it's
                ### a partial item.  In either case, we 
                $self->{read_buffer} = pop @$buffarray;
            }
            ### ... but if we don't, we have to go into lexer mode, and
            ### account for various line ending configurations.
            ### This  platform-neutral regex matches one line of text.
            else {
                while ($self->{read_buffer} 
                    =~ m/\G              # Bump along.

                       (\C*?              # Capture everything up to and
                                         # including the line ending.
                                         
                         [\015\012]      # Either a CR or LF
                           (?:
                             (?<=\015)   # If it was a CR...
                             \012        # ... match an LF.
                           )?
                       )
                     /xgc)
                {
                    push @$buffarray, $1;                 
                }
                ### Eliminate everything we've matched from the scalar read
                ### buffer.  If there's an unmatch partial line, keep that 
                ### in the read buffer.
                my $pos = pos($self->{read_buffer});
                if (defined ($self->{read_buffer}) and defined($pos)) {
                    $self->{read_buffer} =
                         bytes::substr($self->{read_buffer}, $pos);
                }
                ### Reset the read buffer's pos, so that the lexer will work.
                pos($self->{read_buffer}) = 0;
            }
            $offset = defined $self->{read_buffer} ? 
                bytes::length($self->{read_buffer}) : 0;
            seek($tf_handle, $self->{read_pos}, 0);
            if(read($tf_handle, $self->{read_buffer}, 2**15, $offset)) {
                $self->{read_pos} += 2**15;
            }
            ### If the read failed, but there's data in the buffer, it's
            ### the last line, so move it to the cache.
            elsif ($offset) {
                if (defined $self->{linesep}) {
                    local $/ = $self->{linesep};
                    chomp $self->{read_buffer};
                }
                push @$buffarray, $self->{read_buffer};
                $self->{read_buffer} = undef;
                last;
            }
            else {
                last;
            }
        }
        $starter = 0; 
    }
    ### Indicate whether the attempt to refill the buffer was successful.
    return scalar @$buffarray;
}

##############################################################################
### Determine the highest item in the buffarray that is still lower than a
### given endpost.
##############################################################################
sub _define_range {
    my $self = shift;
    my $comparecode = shift;
    my $buffarray = $self->{buffarray};
    my $top = 0;
    my $tail = $#$buffarray;
    ### work outwards from the middle.  Divide and conquer.
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
    ### Get that last item in...
    while ($top < $#$buffarray and &$comparecode($buffarray->[$top + 1]) < 1){
        $top++;
    }
    ### Check to see if ALL of the array elements are outside the range.
    if ($top == 0) {
        if (&$comparecode($buffarray->[0]) == 1) {
            $top = -1;
        }
    }
    $self->{max_in_range} = $top;
}

1;

__END__

=head1 NAME

Sort::External - sort huge lists

=head1 SYNOPSIS

    my $sortex = Sort::External->new( -mem_threshold = 2 ** 24 );
    while (<HUGEFILE>) {
        $sortex->feed( $_ );
    }
    $sortex->finish;
    while (defined($_ = $sortex->fetch)) {
        &do_stuff_with( $_ );
    }

=head1 DESCRIPTION

Problem: You have a list which is too big to sort in-memory.  Solution:  Use
Sort::External, the closest thing to a drop-in replacement for
Perl's sort() function when dealing with unmanageably large lists.

=head2 Where's the sortex() function?

In a perfect world, Sort::External would export a sortex() function that you
could swap with sort() and be done.  That isn't possible, because it would
have to return a list which would, in all likelihood, be too large to fit in
memory -- otherwise, why use Sort::External in the first place?  

=head2 Replacing sort() with Sort::External

When you replace sort() with the "feed, finish, fetch" cycle of a
Sort::External object, there are two things to watch out for.

=over

=item 

B<-line_separator> -- Sort::External uses temp files to cache sortable items.
If each item is terminated by LF/CRLF/CR, as would be the case for lines
from a text file, then Sort::External has no problem figuring out where items
begin and end when reading back from disk.  If that's not the case, you need
to set a -line_separator, as documented below. 

=item 

B<undef values and references> -- Perl's sort() function sorts undef values to
the front of a list -- it will complain if you have warnings enabled, but it
preserves their undef-ness.  sort() also preserves references.  In contrast,
Sort::External's behavior is unpredictable and almost never desirable when you
feed it either undef values or references.  If you really care about sorting
lists containing undefs or refs, you'll have to symbollically replace and
restore them yourself.

=back

=head2 Memory management

Sort::External functions best when it can accumulate a large input cache
before sorting the cache and flushing it to disk.  For backwards compatibility
reasons, the default threshold is 10,000 array items, which may be too high if
your items are large.  However, starting at version .10, Sort::External
implements -mem_threshold, an *experimental* feature which makes it possible
to flush to disk when the amount of memory consumed by the input cache exceeds
a certain number of bytes.

There are two important caveats to keep in mind about -mem_threshold.  First,
Sort::External uses L<Devel::Size|Devel::Size> to assess memory consumption,
which tends to underestimate real memory consumption somewhat -- see the docs
for details.  Second, the amount of memory consumed by the Sort::External
process will be no less than double what you set for -mem_threshold, and
probably considerably more.  Don't get too aggressive, because the only time
a very high -mem_threshold provides outsized benefits is when it's big enough
that it prevents a disk flush -- in which case, you might just want to use
sort(). 

=head2 Compatibility

Sort::External works with...

=over

=item * 

ASCII, all flavors of ISO-8859, and any other single-byte encoding scheme that
terminates lines using either LF, CRLF, or CR.  Sort::External is platform-
neutral with regards to line-endings.

=item *

arbitrary binary data, in conjunction with a -line_separator.

=item * 

anything else... so long as you treat the "anything else" like arbitrary
binary data.

=back

For instance, Sort::External can sort utf8 strings, but only if you specify a
-line_separator -- and regardless of what you feed() your sortex object, the 
scalars that you get back from fetch() will NOT be marked with a utf8 flag.  

=head1 METHODS

=head2 new()

    my $sortscheme = sub { $Sort::External::b <=> $Sort::External::a };
    my $sortex = Sort::External->new(
        -sortsub         => $sortscheme,      # default sort: standard lexical
        -working_dir     => $temp_directory,  # default: see below
        -line_separator  => 'random',         # default: see below
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

B<-line_separator> -- By default, Sort::External assumes that your items are
already terminated with either LF, CRLF, or CR (it can handle all three, even
mixed in the same file).  If that's not true, you have two options: 1) specify
your own value for -line_separator (which Sort::External will append to each
item when storing and chomp() away upon retrieval) or 2) specify 'random', in
which case Sort::External will use a random 16-byte string suitable for
delimiting arbitrary binary data.

=item 

B<-mem_threshold> -- EXPERIMENTAL FEATURE.  Allow the input cache to grow to
-mem_threshold bytes before sorting it and flushing to disk.  Suggested value:
Start somewhere between 2**20 and 2**24: 1-16 Mb.

=item 

B<-cache_size> -- The size for each of Sort::External's caches, in sortable
items.  If -mem_threshold is enabled, -cache_size is ignored.

=back

=head2 feed()

    $sortex->feed( @items );

Feed one or more sortable items to your Sort::External object.  It is normal
for occasional pauses to occur as caches are flushed and sortfiles are merged.

=head2 finish() 

    ### if you intend to call fetch...
    $sortex->finish; 
    
    ### otherwise....
    use Fcntl;
    $sortex->finish( 
        -outfile => 'sorted.txt',
        -flags => (O_CREAT | O_WRONLY),
        );

Prepare to output items in sorted order.  If you haven't yet exceeded the
cache size, Sort::External never writes to disk -- it just sorts the items
in-memory.

If you specify the parameter -outfile, Sort::External will attempt to write
your sorted list to that outfile.  By default, Sort::External will refuse to
overwrite an existing file; if you want to override that behavior, you can
pass Fcntl flags to finish() using the optional -flags parameter.

Note that you can either finish() to an -outfile, or finish() then fetch()...
but not both.  

=head2 fetch()

    while (defined ($_ = $sortex->fetch)) {
        &do_stuff_with( $_ );
    }

Fetch the next sorted item.  

=head1 DISCUSSION

=head2 "internal" vs. "external" sorting 

In the CS world, "internal sorting" refers to sorting data in RAM, while
"external sorting" refers to sorting data which is stored on disk, tape, or
any storage medium except RAM.  The main goal when implementing an external
sorting algorithm is to minimize disk I/O.  Sort::External's routine can be
summarized like so:

Cache sortable items in memory.  Every X items, sort the cache and empty it
into a temporary sortfile.  As sortfiles accumulate, interleave them
periodically into larger sortfiles.  Complete the sort by emptying the input
cache then interleaving the contents of all existing sortfiles into an output
stream.

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

