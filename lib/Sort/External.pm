package Sort::External;
use strict;
use warnings;

require 5.006_001;

our $VERSION = '0.10_1';

use File::Temp 'tempdir';
use Devel::Size 'size';
use Fcntl qw( :DEFAULT );
use Carp;

use bytes;

### Coding convention:
### Public methods use hash style parameters except when to do so would cause 
### a significant performance penalty.  The parameter names are prepended with
### a dash, e.g. '-foo'.  When it is necessary to modify a parameter value
### for use, it is copied into a similarly named variable without a dash: e.g.
### $self->{-working_dir} gets copied to $self->{workdir}.

### The maximum size that temp files are allowed to attain.
use constant MAX_FS => 2 ** 31; # 2 Gbytes;

##############################################################################
### Constructor
##############################################################################
sub new {
    my $class = shift;
    my $self = bless {}, ref($class) || $class;
    $self->_init_sort_external(@_);
    return $self;
}

my %init_defaults = (
    -sortsub                => undef,
    -working_dir            => undef,
    -line_separator         => undef,
    -cache_size             => 10_000,
    -mem_threshold          => 0, 
    -verbosity              => 1,
    ### The number of sortfiles at one level.  Can grow.  See further comments
    ### in _consolidate_one_level() 
    max_sortfiles           => 10,
    ### Keep track of position when reading back from tempfiles.
    outplaceholder          => 0,
    mem_bytes               => 0,
    out_fh                  => undef,
    );

##############################################################################
### Initialize a Sort::External object.
##############################################################################
sub _init_sort_external {
    my $self = shift;
    %$self = (%init_defaults, %$self);
    while (@_) {
        my ($var, $val) = (shift, shift);
        croak("Illegal parameter: '$var'") 
            unless exists $init_defaults{$var};
        $self->{$var} = $val;
    }

    $self->{workdir} = $self->{-working_dir};
    unless (defined $self->{workdir}) {
        $self->{workdir} = tempdir( CLEANUP => 1 ); 
    }
    
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

    ### Items are stored in the item_cache until
    ### _write_item_cache_to_tempfile() is called.
    $self->{item_cache}  = [];
    ### A place for tempfile filehandles to accumulate. 
    $self->{sortfiles}    = [];
    $self->{sortfiles}[0] = [];
}

##############################################################################
### Feed one or more items to the Sort::External object.
##############################################################################
sub feed {
    my $self = shift;
    if ($self->{-mem_threshold}) {
        my $item_cache = $self->{item_cache};
        my $threshold = $self->{-mem_threshold};
        for (@_) {
            push @$item_cache, $_;
            $self->{mem_bytes} += size($item_cache->[-1]);
            $self->_write_item_cache_to_tempfile
                if ($self->{mem_bytes} > $threshold);
        }
        return;
    }
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
        croak('Argument -outfile is required') unless defined $args{-outfile};
        sysopen(OUTFILE, $args{-outfile}, $args{-flags})
            or croak ("Couldn't open outfile '$args{-outfile}': $!");
        print OUTFILE for @{ $self->{item_cache} };
        for my $fh (@{ $self->{sortfiles}[-1] }) {
            seek ($fh, 0, 0);
            local $/ = $self->{linesep} 
                if defined $self->{linesep};
            my $entry;
            while (defined($entry = (<$fh>))) {
                print OUTFILE $entry
                    or croak("Couldn't print to '$args{-outfile}': $!");
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
    my $out_fh = $self->{out_fh};
    if (@{ $self->{item_cache} }) {
        return shift @{ $self->{item_cache} };
    }
    elsif ($out_fh and $self->{linesep}) {
        local $/ = $self->{linesep};
        while (<$out_fh>) {
            chomp;
            return $_;
        }
    }
    elsif ($out_fh) {
        while (<$out_fh>) {
            return $_;
        }
    }
    if ($self->{out_fh} = shift @{ $self->{sortfiles}[-1] }) {
        $out_fh = $self->{out_fh};
        seek($out_fh, 0, 0);
        return $self->fetch;
    }
    else {
        return undef;
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
    my $tmp = File::Temp->new(
        DIR => $self->{workdir},
        UNLINK => 1,
    );  
    push @{ $self->{sortfiles}[0] }, $tmp;

    @$item_cache = defined $sortsub ?
                   (sort $sortsub @$item_cache) :
                   (sort @$item_cache);

    if (defined $linesep) {
        for (@$item_cache) {
            print $tmp "$_$linesep"
                or croak("Print to $tmp failed: $!");
        }
    }
    else {
        for (@$item_cache) {
            print $tmp $_
                or croak("Print to $tmp failed: $!");
        }
    }
    
    $self->{mem_bytes} = 0;
    $self->{item_cache} = [];
    $self->_consolidate_sortfiles
        if @{ $self->{sortfiles}[0] } >= 10;
}       

##############################################################################
### Consolidate sortfiles by interleaving their contents.
##############################################################################
sub _consolidate_sortfiles {
    my $self = shift;
    my $final = shift || '';
    
    $self->_write_item_cache_to_tempfile
        unless $final and !@{ $self->{sortfiles}[-1] };
    
    ### Higher levels of the sortfiles array have been through more
    ### stages of consolidation.  
    ###
    ### If this is the last sort, we need to end up with one final group of 
    ### sorted files (which, if concatenated, would create one giant 
    ### sortfile).
    ### 
    ### The final sort is available as an array of filehandles: 
    ### @{ $self->{sortfiles}[-1] }
    for my $input_level (0 .. $#{$self->{sortfiles} }) {
        if ($final) {
            if (@{ $self->{sortfiles}[-1] }) {
                $self->_consolidate_one_level($input_level);
            }
            else { 
                my $item_cache = $self->{item_cache};
                my $sortsub = $self->{-sortsub};
                if (defined $self->{linesep}) {
                    local $/ = $self->{linesep};
                    chomp @$item_cache;
                }
                @$item_cache = $sortsub ?
                               sort $sortsub @$item_cache : 
                               sort @$item_cache;
            }
        }
        elsif ( @{ $self->{sortfiles}[$input_level] } >
            $self->{max_sortfiles}) 
        {
            $self->_consolidate_one_level($input_level);
        }
    }
    
    if ($final and defined $self->{linesep}) {
        local $/ = $self->{linesep};
        chomp @{ $self->{item_cache} };
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
    
    ### Build an array of buffers.  
    ### 
    ### Each buffer is scalar blessed into its own unique package.
    ### The filehandle used to refill the scalar is stored in the
    ### next_line() closure within that package.
    ### next_line() sets the value of the blessed scalar, returning undef if
    ### the filehandle has exhausted itself.
    ### 
    ### The rationale behind this somewhat esoteric technique: as few
    ### dereference ops as possible.
    my @in_buffers;
    my $num_packages = 0;
    for my $tf_handle (@{ $self->{sortfiles}[$input_level] }) {
        seek($tf_handle, 0, 0);
        my $holder;
        my $packagename = "Sort::External::Buffer$num_packages";
        my $buff = bless \$holder, $packagename; 
        no strict 'refs';
        no warnings 'redefine';
        $num_packages++;
        *{ $packagename . "::next_line" } = defined $linesep ?
            sub { 
                return unless defined(${ $_[0] } = <$tf_handle>); 
                chomp $_[0]; 
                } :
            sub { 
                return unless defined(${ $_[0] } = <$tf_handle>); 
                };
        next unless defined $buff->next_line;
        
        my $place = 0;    
        if (defined $sortsub) {
            for my $prev_buff (@in_buffers) {
                last unless (sort $sortsub($$buff, $prev_buff))[0] eq $$buff;
                $place++;
            }
        }
        else {
            for my $prev_buff (@in_buffers) {
                last unless $$buff gt $$prev_buff;
                $place++;
            }
        }
        splice(@in_buffers, $place, 0, $buff);
    }
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
    my $bubblebuff_code = <<'EOSTUFF';
        BUBBLEBUFF: while (@in_buffers) {
            my $bubblebuff = shift @in_buffers;
            $outfile_length += bytes::length($$bubblebuff);
            if ($outfile_length > MAX_FS) {
                undef $outfile;
                $outfile = File::Temp->new(
                    DIR => $self->{workdir},
                    UNLINK  => 1
                    );
                push @outfiles, $outfile;
                $outfile_length = bytes::length($$bubblebuff);
            }
            print $outfile $$bubblebuff;
            ### If the filehandle filling this buffer has been exhausted,
            ### remove the buffer from the array.
            next BUBBLEBUFF unless defined $bubblebuff->next_line;
            my $place = 0;    
            ### bubblesort the buffer to the proper place in the queue.
            for my $buff (@in_buffers) {
EOSTUFF
    $bubblebuff_code .= $sortsub ?
                q#last unless (sort $sortsub ($$buff, $$bubblebuff))[0] 
                        eq $$bubblebuff;# :
                q#last unless $$buff lt $$bubblebuff;#;
    $bubblebuff_code .= <<'EOSTUFF';
                $place++;
            }
            splice(@in_buffers, $place, 0, $bubblebuff);
        }
EOSTUFF
    eval $bubblebuff_code;
    die $@ if $@;

    ### Explicity close the filehandles that were consolidate; since they're
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
    no strict 'refs';
    no warnings 'redefine';
    ### Clear out the closures so that we don't leak memory.
    for (0 .. $num_packages) {
        *{ "Sort::External::Buffer$_" . "::next_line" } = sub {};
    }
}

1;

__END__

=head1 NAME

Sort::External - sort huge lists

=head1 VERSION

0.05

=head1 SYNOPSIS

    my $sortex = Sort::External->new( -mem_threshold = 2 ** 24 );
    while (<HUGEFILE>) {
        $sortex->feed( $_ );
    }
    $sortex->finish;
    my $stuff;
    while (defined($stuff = $sortex->fetch)) {
        &do_stuff_with( $stuff );
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
If each item is terminated by a newline/CRLF, as would be the case for lines
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
which tends to underestimate real memory consumption somewhat -- set the docs
for details.  Second, the amount of memory consumed by the Sort::External
process will be no less than double what you set for -mem_threshold, and
probably considerably more.  Don't get too aggressive.  A good starting
point is 2**24: 16 megabytes.

=head1 METHODS

=head2 new()

    my $sortscheme = sub { $Sort::External::b <=> $Sort::External::a };
    my $sortex = Sort::External->new(
        -sortsub         => $sortscheme,      # default sort: standard lexical
        -working_dir     => $temp_directory,  # default: see below
        -line_separator  => 'random',         # default: $/
        -mem_threshold   => 2 ** 24,          # default: 0 (inactive);
        -cache_size      => 100_000,          # default: 10_000;
        );

Construct a Sort::External object.

=over

=item 

B<-sortsub> -- A sorting subroutine.  Be advised that you MUST use
$Sort::External::a and $Sort::External::b instead of $a and $b in your sub.  

=item 

B<-working_dir> -- The directory where the temporary sortfiles will reside.
By default, this directory is created using L<File::Temp|File::Temp>'s
tempdir() command.

=item 

B<-line_separator> -- By default, Sort::External assumes that your items are
already terminated with a newline/CRLF or whatever your system considers to be
a line ending (see the perlvar documentation for $/).  If that's not true, you
have two options: 1) specify your own value for -line_separator (which
Sort::External will append to each item when storing and chomp() away upon
retrieval) or 2) specify 'random', in which case Sort::External will use a
random 16-byte string suitable for delimiting arbitrary binary data.

=item 

B<-mem_threshold> -- EXPERIMENTAL FEATURE.  Allow the input cache to grow to
-mem_threshold bytes before sorting it and flushing to disk.  

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

    while (my $stuff = $sortex->fetch) {
        &do_stuff_with( $stuff );
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

