package Sort::External;
use strict;
use warnings;

require 5.006_001;

our $VERSION = '0.03';

use File::Temp 'tempdir';
use Fcntl qw( :DEFAULT );
use Carp;

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
    ### The number of sortfiles at one level.  Can grow.  See further comments
    ### in _consolidate_one_level() 
    max_sortfiles           => 10,
    ### Keep track of position when reading back from tempfiles.
    outplaceholder          => 0,
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
    
    $self->{sortsub} = defined $self->{-sortsub} ?
        $self->{-sortsub} : sub { $a cmp $b };

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

    ### Items are stored in the input_cache until
    ### _write_input_cache_to_tempfile() is called.
    $self->{input_cache}  = [];
    ### A place for tempfile filehandles to accumulate. 
    $self->{sortfiles}    = [];
    $self->{sortfiles}[0] = [];
}

##############################################################################
### Feed one or more items to the Sort::External object.
##############################################################################
sub feed {
    my $self = shift;
    if (defined $self->{linesep}) {
        my $input_cache = $self->{input_cache};
        my $linesep = $self->{linesep};
        for (@_) {
            push @$input_cache, "$_$linesep";
        }
    }
    else {
        push @{ $self->{input_cache} }, @_;
    }
    return unless @{ $self->{input_cache} } >= $self->{-cache_size};
    $self->_write_input_cache_to_tempfile;
}

my %finish_defaults = (
    -outfile => 'sorted.txt',
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
        my %args;
        while (@_) {
            my ($var, $val) = (shift, shift);
            croak("Illegal parameter: '$var'") 
                unless exists $finish_defaults{$var};
            $args{$var} = $val;
        }
        sysopen(OUTFILE, $args{-outfile}, O_CREAT | O_EXCL | O_WRONLY )
            or croak ("Couldn't open outfile '$args{-outfile}': $!");
        for my $source_fh (@{ $self->{sortfiles}[-1] }) {
            while (<$source_fh>) {
                print OUTFILE 
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
    
    ### While there's nothing in the output buffer...
    while (!@{ $self->{output_cache} }) {
        my $output_cache = $self->{output_cache};
        ### Eject from the loop if there aren't any more sortfiles.
        last unless my $fh = $self->{sortfiles}[-1][0];

        seek($fh, $self->{outplaceholder}, 0);
        local $/ = $self->{linesep} 
            if defined $self->{linesep};
        for (1 .. $self->{-cache_size}) {
            if (my $entry = (<$fh>)) {
                push @$output_cache, $entry;
            }
            else {
                close $fh or croak("Couldn't close file '$fh': $!");
                shift @{ $self->{sortfiles}[-1] };
                $self->{outplaceholder} = 0;
                undef $fh;
                last;
            }
        }
        chomp @$output_cache if defined $self->{linesep};
        $self->{outplaceholder} = tell $fh 
            if defined $fh;
    }

    ### Return a sorted item, if there are any.
    if (@{ $self->{output_cache} }) {
        return shift @{ $self->{output_cache} };
    }
    else {
        return undef;
    }
}

##############################################################################
### Flush the items in the input cache to a tempfile, sorting as we go.
##############################################################################
sub _write_input_cache_to_tempfile {
    my $self = shift;
    return unless @{ $self->{input_cache} };
    my $tmp = File::Temp->new(
        DIR => $self->{workdir},
        UNLINK => 1,
    );  
    push @{ $self->{sortfiles}[0] }, $tmp;
    if ($self->{-sortsub}) {
        my $sortsub = $self->{sortsub};
        for (sort $sortsub @{ $self->{input_cache} }) {
            print $tmp $_
                or croak("Print to $tmp failed: $!");
        }
    }
    else {
        for (sort @{ $self->{input_cache} }) {
            print $tmp $_
                or croak("Print to $tmp failed: $!");
        }
    }
    $self->{input_cache} = [];
    $self->_consolidate_sortfiles
        if @{ $self->{sortfiles}[0] } >= 10;
}       

##############################################################################
### Consolidate sortfiles by interleaving their contents.
##############################################################################
sub _consolidate_sortfiles {
    my $self = shift;
    my $final = shift || '';
    
    $self->_write_input_cache_to_tempfile
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
                my $input_cache = $self->{input_cache};
                my $sortsub = $self->{sortsub};
                @$input_cache = $self->{-sortsub} ?
                                sort $sortsub @$input_cache : 
                                sort @$input_cache;
            }
        }
        elsif ( @{ $self->{sortfiles}[$input_level] } >
            $self->{max_sortfiles}) 
        {
            $self->_consolidate_one_level($input_level);
        }
    }
    
    if ($final) {
        ### re-cycle the input cache as an output cache.
        $self->{output_cache} = delete $self->{input_cache};
    }
}

##############################################################################
### Do the hard work for _consolidate_sortfiles().
##############################################################################
sub _consolidate_one_level {
    my $self = shift;
    my $input_level = shift;
    
    my $sortsub = $self->{sortsub};
    local $/ = $self->{-line_separator} if $self->{-line_separator};
    
    ### Offload filehandles destined for consolidation onto a 
    ### lexically scoped array.  When it goes out of scope, the temp files
    ### are supposed to delete themselves (but don't, for some reason, so we
    ### close them explicitly).
    my $filehandles_to_sort = $self->{sortfiles}[$input_level];
    $self->{sortfiles}[$input_level] = [];
    
    ### Create a holder for the output filehandles if necessary.
    if (!defined $self->{sortfiles}[$input_level + 1]) {
        $self->{sortfiles}[$input_level + 1] = [];
    }
    
    my %in_buffers;
    my %bookmarks;
    my @outfiles;
    ### Get a new outfile
    my $outfile = File::Temp->new(
        DIR => $self->{workdir}, 
        UNLINK  => 1
        );
    push @outfiles, $outfile;
    
    my $num_filehandles = @$filehandles_to_sort;
    my $max_lines = $num_filehandles ?
                    int($self->{-cache_size} / $num_filehandles) :
                    $self->{-cache_size};
    $max_lines ||= 1; # testing purposes only.

    $in_buffers{$_} = [] for (0 .. $#$filehandles_to_sort);
    $bookmarks{$_}  = 0  for (0 .. $#$filehandles_to_sort);
    
    while (%in_buffers) {
        ### Read $max_lines (typically 1000) entries per input file into 
        ### buffers. 
        foreach my $file_number (keys %in_buffers) {
            my $fh = $filehandles_to_sort->[$file_number];
            seek($fh, $bookmarks{$file_number}, 0);
            for (1 .. $max_lines) {
                if (my $entry = (<$fh>)) {
                    push @{ $in_buffers{$file_number} }, $entry;
                }
                else {
                    last;
                }
            }
            $bookmarks{$file_number} = tell $fh;
            ### We've attempted to fill the buffer.
            ### If there's nothing in the buffer, we've exhausted the source
            ### file, so make the buffer go away. 
            delete $in_buffers{$file_number} 
                unless @{ $in_buffers{$file_number} };
        }
    
        ### Get the lowest of all the last lines from the input_buffers.
        ### All items less than or equal to this value in sort order are 
        ### guaranteed to reside in the input_buffers, so we can safely 
        ### sort them.
        my @high_candidates;
        for (values %in_buffers) {
            push @high_candidates, $_->[-1] if defined $_->[-1];
        }
        @high_candidates = $self->{-sortsub} ?
                           (sort $sortsub @high_candidates) :
                           (sort @high_candidates); 
        my $highest_allowed = $high_candidates[0];
        
        ### To conserve memory, reuse the array assigned to the input 
        ### cache (which is currently empty) as a holder for this batch of 
        ### sortable items.
        my $batch = $self->{input_cache};

        ### Capture a batch of items from the input_buffers.  All the items in
        ### the batch will be lower in sorted order than items yet to enter
        ### the buffer. 
        ### Note: The only difference between these two blocks is the
        ### performance-killing conditional in the first.
        if ($self->{-sortsub}) {
            foreach my $input_buffer (values %in_buffers) {
                LINE: while (my $line = shift @$input_buffer) {
                    ### If true, the item lies outside of the allowable
                    ### boundaries.  Put it back in the queue and skip to 
                    ### the next input_buffer.
                    if (
                        (sort $sortsub ($line, $highest_allowed))[0] 
                            eq $highest_allowed
                        and $line ne $highest_allowed) 
                    {
                        unshift @$input_buffer, $line;
                        last LINE;
                    }
                    push @$batch, $line;
                }
            }
        }
        else {
            foreach my $input_buffer (values %in_buffers) {
                LINE: while (my $line = shift @$input_buffer) {
                    if ($line gt $highest_allowed) {
                        unshift @$input_buffer, $line;
                        last LINE;
                    }
                    push @$batch, $line;
                }
            }
        }
        
        @$batch = $self->{-sortsub} ? 
                 (sort $sortsub @$batch) :
                 (sort @$batch);
        my $write_buffer = join '', @$batch;
        @$batch = ();
        
        ### Start a new outfile if writing the contents of the buffer to the
        ### existing outfile would cause it to grow larger than the maximum
        ### allowed filesize.
        seek($outfile, 0, 2); # EOF
        my $filelength = tell $outfile;
        if ((length($write_buffer) + $filelength) > MAX_FS ) {
            undef $outfile;
            $outfile = File::Temp->new(
                DIR => $self->{workdir},
                UNLINK  => 1
                );
            push @outfiles, $outfile;
        }
        
        print $outfile $write_buffer;
    }
    
    ### Explicity close the filehandles that were consolidate; since they're
    ### tempfiles, they'll unlink themselves.
    close $_ for @$filehandles_to_sort;
    
    ### Add the filehandle for the outfile(s) to the next higher level of the
    ### sortfiles array.
    push @{ $self->{sortfiles}[$input_level + 1] }, 
        @outfiles;
    ### If there's more than one outfile, we're exceeding the 
    ### max filesize limit.  Increase the maximum number of files required 
    ### to trigger a sort, so that we don't sort endlessly.
    $self->{max_sortfiles} += ($#outfiles);
}

1;

__END__

=head1 NAME

Sort::External - sort huge lists

=head1 VERSION

0.03

This is ALPHA release software.  The interface may change.  However, it's
simple enough that it probably won't stay in alpha very long.  Please drop a
line to the author if you are using it successfully -- a couple happy
customers and we'll move from alpha to beta.

=head1 SYNOPSIS

    my $sortex = Sort::External->new;
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

By default, Sort::External's cache size is 10,000 items.  If your items are
large, you may need to decrease the cache size; if they are small, you
might improve Sort::External's performance somewhat by increasing the cache
size. 

Because Perl doesn't give you the kind of responsibility for memory management
that C does (thank goodness), there isn't a reliable way to flush the cache
automatically based on analyzing memory consumption that doesn't impose a
severe penalty on performance.  If you want to max out the speed of
Sort::External, here are two tips: 1) maximum memory usage will likely
occur when finish() is called, and 2) don't cut things close, because there
isn't that much to gain and there's a lot to lose if you go over the edge and 
start hitting swap.

=head1 METHODS

=head2 new()

    my $sortscheme = sub { $Sort::External::b <=> $Sort::External::a };
    my $sortex = Sort::External->new(
        -sortsub         => $sortscheme,      # default sort: standard lexical
        -working_dir     => $temp_directory,  # default: see below
        -line_separator  => 'random',         # default: $/
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

B<-cache_size> -- The size for each of Sort::External's caches, in sortable
items.  

=back

=head2 feed()

    $sortex->feed( @items );

Feed one or more sortable items to your Sort::External object.  It is normal
for occasional pauses to occur as sortfiles are merged.

=head2 finish() 

    $sortex->finish( -outfile => 'sorted.txt' );
    ### or, if you intend to call fetch...
    $sortex->finish; 

Prepare to output items in sorted order.  If you haven't yet exceeded the
cache size, Sort::External never writes to disk -- it just sorts the items
in-memory.

If you specify the parameter -outfile, Sort::External will attempt to write
your sorted list to that outfile (it will croak() if the file already exists).

Note that you can either have finish() write to an outfile, or finish() then
fetch()...  but not both.  

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
periodically into larger sortfiles.  Use caching extensively during the
interleaving process to minimize I/O.  Complete the sort by emptying the input
cache then interleaving the contents of all existing sortfiles into an output
stream.

=head1 BUGS

Please report any bugs or feature requests to
C<bug-sort-external@rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Sort-External>.

=head1 ACKNOWLEDGEMENTS

The code in Sort::External was originally developed as part of the
L<Search::Kinosearch|Search::Kinosearch> suite.  Chris Nandor helped debug
that early version and made some excellent suggestions which have been
incorporated into the present distribution.

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

