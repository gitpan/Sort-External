package Sort::External::Cookbook;
use Sort::External;
our $VERSION = $Sort::External::VERSION;

1;

__END__

=head1 NAME

Sort::External::Cookbook - sort huge lists efficiently

=head1 DESCRIPTION

Sort::External has a special affinity for the packed-default sort, which first
came to the author's attention via a paper by Uri Guttman and Larry Rosler,
"A Fresh Look at Efficient Perl Sorting", located as of this writing at
L<http://www.sysarch.com/perl/sort_paper.html>.

For most applications, the packed-default is the most efficient Perl sorting
transform.  This document explores how to use it to solve common coding
problems in conjunction with either sort() or Sort::External.

=head1 The Packed-Default Sort 

=head2 Eliminate the sortsub!

Perl's sortsubs are extremely flexible and powerful, but they are also
computationally expensive.  If you can eliminate the sortsub entirely from
your algo, major optimizations kick in as Perl decides that it can
perform the whole sort at the C level without leaving to execute Perl code.  

Like Perl's native sort() function, Sort::External I<really, really> likes to
use the default ascending lexical sort.  If you are chasing efficiency, you
should do everything you can to avoid supplying Sort::External with a sortsub.

=head2 Encode, sort, decode.

The standard technique for sorting a hash by value in Perl is to use a sortsub:

    my %colors = (
        "Granny Smith"     => "green",
        "Golden Delicious" => "yellow",
        "Pink Lady"        => "pink",
        );
    my @sorted_by_color = sort { $colors{$a} cmp $colors{$b} } keys %colors;
    
    ### Result:
    ### @sorted_by_color = ("Granny Smith", "Pink Lady", "Golden Delicious");

To accomplish the same goal using a packed-default sort, we use a three-step
process:

    ### 1. Encode
    my @sortkeys;
    while (my ($variety, $color) = each %colors) {
        push @sortkeys, sprintf("%-6s%-16s", $color, $variety);
    }
    ### @sortkeys = (
    ###     "green Granny Smith    ",
    ###     "yellowGolden Delicious",
    ###     "pink  Pink Lady       ",
    ###     );
    
    ### 2. Sort
    my @sorted_by_color = sort @sortkeys; # no sortsub!
    
    ### 3. Decode
    @sorted_by_color = map { substr($_, 6) } @sorted_by_color;
    
Using Sort::External, steps 2 and 3 are modified:

    ### 1. Encode
    my @sortkeys;
    while (my ($variety, $color) = each %colors) {
        push @sortkeys, sprintf("%-6s%-16s", $color, $variety);
    }
    
    ### 2. Sort
    my $sortex = Sort::External->new( -mem_threshold => 2**24 );
    $sortex->feed( @sortkeys );
    $sortex->finish;
    
    ### 3. Decode
    my @sorted_by_color; 
    while (defined($_ = $sortex->fetch)) {
        push @sorted_by_color, substr($_, 6);
    }

For the sake of brevity, we'll use sort() instead of Sort::External for most
of our examples.

=head1 Encoding techniques

=head2 sprintf

The sprintf technique for encoding sortkeys is the most straightforward and
flexible.  In addition to preparing multi-field text data for a lexical sort,
it can be used to put numerical data into a string form where the lexical sort
doubles as a numerical sort.

    my %prices = (
        "Granny Smith"     => 1.69,
        "Pink Lady"        => 1.99,
        "Golden Delicious" =>  .79, # on sale!
        );
    my @sortkeys;
    while (my ($variety, $price) = each %prices) {
        push @sortkeys, sprintf("%1.2f%-16s", $price, $variety);
    }
    ### @sortkeys = (
    ###     "1.69Granny Smith    ",
    ###     "0.79Golden Delicious",
    ###     "1.99Pink Lady       ",
    ###     );

There are two potential problems with the sprintf technique.  

First, you must know the maximum width of all fields in advance.  If you
don't, you'll have to run through your data in a wasteful "characterization
pass" solely for the purpose of measuring field widths.

Second, if the fields have a long maximum width but the content is sparse,
sprintf-generated sortkeys take up a lot of unnecessary space.  With
Sort::External, that means extra disk i/o and reduced performance.

=head2 null-termination

An alternative to using sprintf to create fixed-width data is to separate
fields with nulls.  Use the two-byte Unicode null unless you are absolutely
sure you don't need it:

    while (my ($variety, $color) = each %colors) {
        push @sortkeys, "$color\0\0$variety";
    }
    my @sorted = sort @sortkeys;
    s/.*?\0\0// for @sorted;
    
Null-termination often saves space over sprintf -- however, if your data may
contain null bytes, you have to be very careful about how you use it.

=head2 pack

While pack() can be used as a less-flexible version of sprintf for encoding
fixed-width strings, it can also encode positive integers into a
sortable form. The four integer pack formats which sort correctly are:
    
    ### c => signed char           maximum value 127
    ### C => unsigned char         maximum value 255
    ### n => 16-bit network short  maximum value 32_767
    ### N => 32-bit network long   maximum value 2_147_483_647
    
    my %first_introduced = (
        "Granny Smith"     => 1868,
        "Pink Lady"        => 1970,
        "Golden Delicious" => 1890,
        );
    while (my ($variety, $year) = each %first_introduced) {
        push @sortkeys, (pack('n', $year) . $variety);
    }
    my $sortex = Sort::External->new( -mem_threshold  => 2**24 );
    $sortex->feed(@sortkeys);
    $sortex->finish;
    my @sorted; 
    while (defined ($_ = $sortex->fetch)) {
        push @sorted, substr($_, 2);
    }
    
=head2 Combining encoding techniques

It is often the case that a combination of techniques yields the most
efficient sortkey encoding and decoding.  Here is a contrived example:

    my %apple_stats = (
        "Granny Smith"     => {
                color => "green", 
                price => 1.69, 
                year  => 1868,
            },
        "Pink Lady"        => {
                color => "pink",
                price => 1.99, 
                year  => 1970,
            },
        "Golden Delicious" => {
                color => "yellow",
                price =>  .79, 
                year  => 1890,
            },
        );
    my @sortkeys;
    while (my $variety, $stats) = each %apple_stats) {
        push @sortkeys, (
                $stats->{color} . "\0\0" .
                sprintf("%1.2f", $stats->{price}) .
                pack('n', $stats->{year}) .
                "\0\0" . $variety
            );
    }
    my @sorted = sort @sortkeys;
    my @sorted_varieties = map { s/.*\0\0//; $_ } @sorted;

sprintf would be a much better choice in this particular case, since it makes
it easier to recover the all the fields using unpack:
    
    while (my ($variety, $stats) = each %apple_stats) {
        push @sortkeys, (
            sprintf("%-6s%1.2f%04d%-16s", 
                @{ $stats }{'color','price','year'}, $variety
            );
    }
    my @sorted = sort @sortkeys;
    for (@sorted) {
        my (%result, $variety); 
        (@result{'color','price','year'}, $variety) 
            = unpack('a6 a4 a4 a16', $_);
        $_ = { $variety => \%result };
    }

=head1 Extended sorting techniques

=head2 Descending sort order using the "bitwise not" operator

You can reverse the order of a lexical sort by flipping all the bits on your
sortkeys.

    while (my ($variety, $color) = each %colors) {
        push @sortkeys, sprintf("%-6s%-16s", $color, $variety);
    }
    @sortkeys = map { ~ $_ } @sortkeys;
    my @sorted = sort @sortkeys;
    @sorted = map { ~ $_ } @sorted;
    @sorted = map { substr($_, 5) } @sorted;

=head2 Multi-field sorting with mixed asc/desc order

Multi-field packed-default sorts are usually as straightforward as
concatenating the fields using either sprintf or null-termination.  However,
if you need I<both> ascending and descending sub-sorts within the same sort,
special care is required.  Null-termination is generally not an option,
because bit-flipped fields may contain null bytes.

    ### sort by:
    ###     color descending,
    ###     price ascending,
    ###     year  descending
    while (my $variety, $stats) = each %apple_stats) {
        push @sortkeys, (
                (~ sprintf("%-6s", stats->{color}) ) .
                sprintf("%1.2f", $stats->{price}) .
                (~ sprintf("%04d", stats->{year}) ) .
                sprintf("%-16s", $variety);
            );
    }
    my @sorted = sort @sortkeys;

=head2 Case-insensitive sorting

Case-insensitive sorting is something of a weakness for the packed-default
sort.  The only way to avoid a sortsub is to double-encode:

    while (my ($variety, $color) = each %colors) {
        push @sortkeys, ( lc($variety) . "\0\0" . $variety );
        push @sortkeys, ( lc($color)   . "\0\0" . $color   );
    }
    my @sorted = sort @sortkeys;
    @sorted = map { s/.*?\0\0//; $_ } @sorted;
    ### @sorted = ( 
    ###     "Golden Delicious",
    ###     "Granny Smith",
    ###     "green",
    ###     "pink",
    ###     "Pink Lady",
    ###     "yellow",
    ###     );

=head1 A challenge

Ken Clarke has identified a case where there seems to be no elegant
packed-default sort solution:

    ### Each row contains multiple fields
    ### AND is impossible to know the field lengths in advance
    ### AND there are both ascending and descending subsorts
    ### AND (the sort must be (case-insensitive AND case-preserving))

There doesn't appear to be a way to perform this sort without a
characterization pass.

If you come up with an efficient solution, please write the author.

=head1 ACKNOWLEDGEMENTS

This document arose out of discussions with Ken Clarke.  

Uri Guttman and Larry Rosler, as mentioned above.

=head1 SEE ALSO

L<Sort::External|Sort::External>

=head1 AUTHOR

Marvin Humphrey E<lt>marvin at rectangular dot comE<gt>
L<http://www.rectangular.com>

=head1 COPYRIGHT

Copyright (c) 2005 Marvin Humphrey.  All rights reserved.
This module is free software.  It may be used, redistributed and/or 
modified under the same terms as Perl itself.

=cut

