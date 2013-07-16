use 5.006;
use strict;
use warnings FATAL => 'all';
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'Records::Split',
    license             => 'Artistic_2_0',
    dist_author         => q{Ankur Gupta <ankur2012@inbox.com>},
    dist_version_from   => 'lib/Records/Split.pm',
    release_status      => 'stable',
    configure_requires => {
        'Module::Build' => 0,
    },
    build_requires => {
        'Test::More' => 0,
    },
    requires => {
    },
    add_to_cleanup     => [ 'Records-Split-*' ],
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
