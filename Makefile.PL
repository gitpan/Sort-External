use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'Sort::External',
    AUTHOR              => 'Marvin Humphrey <marvin at rectangular dot com>',
    VERSION_FROM        => 'lib/Sort/External.pm',
    ABSTRACT_FROM       => 'lib/Sort/External.pm',
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More'  => 0,
        'File::Temp'  => 0.14,
        'Devel::Size' => 0.63,
        'Clone'       => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Sort-External-*' },
);
