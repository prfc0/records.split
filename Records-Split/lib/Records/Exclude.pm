package Records::Exclude;

use strict;
use warnings;
use Carp;

=head1 NAME

Records::Exclude - Exclude some records from a list of records

=head1 VERSION

0.01

=cut

our $VERSION = 0.01;

=head1 SYNOPSIS

    my $exclude_records = Records::Exclude->new(
                                                recs => [ @recs ],
                                                files => [ @files ],
                                                exclude_recs => [ @exclude_recs ]
                                                exclude_files => [ @exclude_files ],
                                                exclude_patterns => [ @exclude_patterns ],
                                               );
    my @records = $exclude_records->exclude;
    my @excluded_records = $exclude_records->excluded;

=head1 DESCRIPTION

This module does a simple task of excluding records from a list of records.

There are two inputs. One is the records and the other which records to exclude.

=over 4

=item 1 A list of records which you can input as.

    a. a list of records ( `recs' )
    b. a file of records ( `files' )

=item 2 A list of records to exclude which you can input as.

    a. a list of records ( `exclude_recs' )
    b. files of records ( `exclude_files' )
    c. pattern of records ( `exclude_patterns' )

=back

=head1 CONSTRUCTOR

=head2 new ( %params )

The constructor takes the following paramters

=over 4

=item 1 recs => [ @recs ]

=item 2 files => [ @files ]

=item 3 exclude_recs => [ @exclude_recs ]

=item 4 exclude_files => [ @exclude_files ]

=item 5 exclude_patterns => [ @exclude_patterns ]

=back

=cut

sub new {
    my ($self, %hash) = @_;

    $self->_check_args(%hash);
    return bless { %hash }, $self;
}

=head1 METHODS

Following are the public methods

=head2 exclude

Does the primary taks of excluding the records and returns the list of records after exclusion

=cut

sub exclude {
    my ($self) = @_;

    my @records = $self->records;

    my %exclude_records;
    $exclude_records{$_} = undef foreach $self->exclude_records;
    my @exclude_patterns = $self->exclude_patterns;

    my @excluded;
    my @after_exclude;

    LOOP:
    foreach my $record ( @records ) {
        if ( exists $exclude_records{$record} ) {
            push @excluded, $record;
            next;
        }
        foreach my $exclude_pattern ( @exclude_patterns ) {
            if ( $record =~ /$exclude_pattern/ ) {
                push @excluded, $record;
                next LOOP;
            }
        }
        push @after_exclude, $record;
    }

    $self->{excluded} = [ @excluded ];
    return @after_exclude;
};

=head2 recs ( @recs )

Get or set records which you want to exclude from.

=cut

sub recs {
    my ($self, @recs) = @_;

    if ( @recs ) {
        $self->{recs} = [ @recs ];
    }

    return unless exists $self->{recs};
    return @{$self->{recs}};
}

=head2 files ( @files )

Set or get the list of files. The records will be in these files.

=cut

sub files {
    my ($self, @files) = @_;

    if ( @files ) {
        $self->{files} = [ @files ];
    }

    return unless exists $self->{files};
    return @{$self->{files}};
}

=head2 exclude_recs

List of records to exclude

=cut

sub exclude_recs {
    my ($self, @exclude_recs) = @_;

    if ( @exclude_recs ) {
        $self->{exclude_recs} = [ @exclude_recs ];
    }

    return unless exists $self->{exclude_recs};
    return @{$self->{exclude_recs}};
}

=head2 exclude_files ( @exclude_files )

Records in the files to exclude

=cut

sub exclude_files {
    my ($self, @exclude_files) = @_;

    if ( @exclude_files ) {
        $self->{exclude_files} = [ @exclude_files ];
    }

    return unless exists $self->{exclude_files};
    return @{$self->{exclude_files}};
}

=head2 exclude_patterns ( @exclude_patterns )

Set or get the list of exclude_patterns

=cut

sub exclude_patterns {
    my ($self, @exclude_patterns) = @_;
    
    if ( @exclude_patterns ) {
        $self->{exclude_patterns} = [ @exclude_patterns ];
    }

    return unless exists $self->{exclude_patterns};
    return @{$self->{exclude_patterns}};
}

=head2 records

Returns the list of records combined from `recs' and `files' parameters

=cut

sub records {
    my ($self) = @_;

    my @records;
    if ( $self->recs ) {
        push @records, $self->recs;
    }

    if ( $self->files ) {
        push @records, $self->recs_from_files($self->files);
    }

    return @records;
}

=head2 exclude_records

Returns the list of records to be excluded combined from `exclude_recs' and `exclude_files' parameters

=cut

sub exclude_records {
    my ($self) = @_;

    my @exclude_records;

    if ( $self->exclude_recs ) {
        push @exclude_records, $self->exclude_recs;
    }

    if ( $self->exclude_files ) {
        push @exclude_records, $self->recs_from_files($self->exclude_files);
    }

    return @exclude_records;
}

=head2 recs_from_files ( @files )

Returns the list of records from the list of files

=cut

sub recs_from_files {
    my ($self, @files) = @_;

    my @recs_from_files;

    foreach my $file ( @files ) {
        open(FILE, $file) or die "$file: $!";
        while(<FILE>){
            chomp;
            next if (/^\s*\#/ or /^\s*$/);
            push @recs_from_files, $1 if (/^\s*(\S+)/);
        }
        close(FILE);
    }

    return @recs_from_files;
}

=head2 excluded

Returns a list of excluded records. Makes sense only aftre you have called exclude method.

=cut

sub excluded {
    my ($self) = @_;

    return unless exists $self->{excluded};
    return @{$self->{excluded}};
}

=head1 PRIVATE METHODS

=head2 _check_args

Checks the validity of constructor arguments

=cut

sub _check_args {
    my ($self, %params) = @_;

    my %allowed_params = (
                        exclude_patterns => undef,
                        recs => undef,
                        exclude_recs => undef,
                        files => undef,
                        exclude_files => undef,
                    );

    foreach my $key ( keys %params ) {
        croak "$key is not a valid paramter" unless exists $allowed_params{$key};
    }
}

=head1 AUTHOR

Ankur Gupta <ankur2012@inbox.com>

=cut

1;
