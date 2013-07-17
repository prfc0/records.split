package Records::Group;

use strict;
use warnings;
use Carp;

our $AUTOLOAD;

=head1 NAME

Records::Group - Store data about a group of Regress::Split

=head1 VERSION

0.01

=cut

our $VERSION = 0.01;

=head1 SYNOPSIS

    my $group = Records::Group->new(
                                     split_into_records => $split_into_records,
                                     split_into_files => $split_into_files,
                                     split_by_pattern => $split_by_pattern,
                                     group_by_pattern => $group_by_pattern,
                                     records => [ @records ],
                                     identifier => $identifier
                                   );
    my $identifier = $group->identifier;
    my @records = $group->records;
    $group->records(@new_records);

=cut

=head1 DESCRIPTION

This module does not do much right now other than accessing and setting some data. It is used by Records::Split in grouping the records so you may never want to use this module directly. But in the future it may have better methods.

=cut

=head1 new

Constructor

=cut

sub new {
    my ($self, %params) = @_;

    return bless { %params }, $self;
}

=head1 METHODS

Following are simple methods of this class.

=head2 AUTOLOAD methods

Simple accessors such as 

    my $num_records = $group->split_into_records are autoloaded.

=cut

sub AUTOLOAD {
    my ($self) = @_;

    (my $key = $AUTOLOAD) =~ s#.*:##;

    croak "$key does not exist" unless $self->exists($key);

    return $self->{$key};
}

=head2 exists

Not all the arguments are mandatory in a group. So before accessing them, you may want to check if they exists or not

    if ( $group->exists('split_into_records') ) {
        my $num_records = $group->split_into_records;
    }

=cut

sub exists {
    my ($self, $key) = @_;

    croak "Please provide the key to check the existence of\n" unless defined $key;

    return unless exists $self->{$key};

    return $self->{$key};
}

=head2 patterns

Returns a list of patterns which is used in grouping the records.

=cut

sub patterns {
    my ($self) = @_;

    return unless exists $self->{group_by_pattern};

    return @{$self->{group_by_pattern}};
}

=head2 records ( @records )

Can be used to access and set the records to the group object

=cut

sub records {
    my ($self, @records) = @_;

    if ( @records ) {
        $self->{records} = [ @records ];
    }

    return unless exists $self->{records};
    return @{$self->{records}};
}

sub total_weight {
    return $_[0]->{total_weight} = $_[1] if defined $_[1];
    return $_[0]->{total_weight};
}

sub DESTROY {}

=head1 AUTHOR

Ankur Gupta L<ankur2012@inbox.com>

=cut

1;
