package Records::Split;

use strict;
use warnings;
use POSIX qw/ceil/;
use Records::Group;
use Carp;
use Cwd;

=head1 NAME

Records::Split - Splits a file or a list of records into smaller sets.

=head1 VERSION

0.01

=cut

our $VERSION = 0.01;

=head1 SYNOPSIS

    # Split the records into multiple sets and store them into smaller files under log directory.
    # In the following example, each file would contain max of 100 records.
    my $split = Records::Split->new( records => [ @records ], identifier => 'set', split_into_records => 100 );
    $split->dump;

    # Split the records given in the file and dump them under log directory.
    # There would be max 20 files.
    # The number of records is equal to the number of records divided by number of files.
    my $split = Records::Split->new( file => $file, split_into_files => 20 );
    $split->dump;
 
    # Split the records given in the file and dump them under log directory.
    # Based on the weight of each record, each set will contain the
    # records whose cumulative sum does not exceeds 500.
    my $split = Records::Split->new( records => [ @records ], wrecords => { %records_weight }, split_by_weight => 500 );
    $split->dump;

=head1 DESCRIPTION

This module will split a file/records into smaller sets based on the arguments passed to the constructor.

It first creates one or more groups, one default group(also called parent group) is always present.

If split_by_pattern is present in any of the groups, then that group is broken into smaller sub-groups.

For each sub-group and using the group's configuration, the sub-groups is split into sets.

If no split_by_pattern is present then a sub group is equal to the group.

The sub groups can be split into sets in the following different ways.

=over 4

=item * split_into_records

Split based on the number of records each set can hold.

=item * split_into_files

Split based on the number of sets.

=item * split_by_weight

Split based on the maximum weight each set can hold.
Weight of a set being the sum of each weight of the set's records.

=back

=head1 CONSTRUCTOR

=head2 new ( CONSTRUCTOR ARGUMENTS )

Following are the constructor arguments

Top level arguments

=over 4

=item identifier

Name for the parent group. By default, it is B<set>.

=item records

List of records in a list.

=item file

List of records in a file. 

Note: Only file or records should be specified in a group. But it is not a mandatory parameter.

=item wrecords

A list of record, weight pairs. Used by split_by_weight parameter.

=item split_by_pattern

A regular expression. Sub groups will be created based on the different values of $1.

=item split_into_records

Each set should not hold more than 'split_into_records' records.

=item split_into_files

There should not be more than 'split_into_files' sets.

=item split_by_weight

Each set's total weight should not exceed 'split_by_weight'.
Total weight of each set being the sum of the weight of each records in that set.

=item max_records

Valid only for split_by_weight. Records in a set should not exceed ma_records no matter what is the total_weight of each set.

=item delimeter

Valid only if file is specified. By default the record separator is "\n". If it is something else then specify a different delimeter.

=back

group arguments

One of group_by_pattern, file, or records should be specified to each group.

All the records specified above can be used in each of the group's configuration as well.

=over 4

=item * identifier

If not specified then the parent group's identifier is taken and the group number is suffixed to it.

=back

=cut

sub new {
    my ($self, %params) = @_;

    my %defaults = ( identifier => 'set' );

    %params = ( %defaults, %params );

    $self->_check_params(%params);

    return bless { %params }, $self;
}

=head1 METHODS

Following are the public methods

=head2 groups

Will return the list of Records::Group objects which is created based on the arguments provided.
Look at the documentation of Records::Group for more details.

Note that number of groups created is number of groups defined in the groups parameter plus the default(parent) group.

=cut

sub groups {
    my ($self) = @_;

    unless ( exists $self->{_groups} ) {
        $self->_groups;
    }

    return @{$self->{_groups}};
}

=head2 group

Returns a list of Records::Group objects. These group objects also have the records assigned to them.

While the method `groups' just create a Records::Group object, method `group' groups the records into each group.

=cut

sub group {
    my ($self) = @_;

    unless ( exists $self->{_group} ) {
        $self->_group;
    }

    return @{$self->{_group}};
}

=head2 split

Does the actual task of splitting the groups after the groups are created. 
It returns a hash whose key is the identifer for each subset and its value is reference to a list.

If you want to dump the sets into the files, then it would be better if you use the dump method which internally calls this before dumping the subsets into smaller fies.

You can use the hash in whatever way you wish. For eg. I am using it in a simple way to print its contents.

    my %subsets = $split->split;
    while( my ($identifer, $record) = each %subsets ) {
        print " $identifier ---> \n";
        foreach my $rec ( @$record ) {
            print $rec, "\n";
        }
        print "\n";
    }

=cut

sub split {
    my ($self) = @_;

    my %sub_sets;
    foreach my $group ( $self->group ) {

        my %sub_groups = $self->_create_sub_groups($group);

        if ( $group->exists('split_by_weight') ) {
            my $max_weight = $group->split_by_weight;
            my $max_record = $group->max_records;
            foreach my $identifier ( keys %sub_groups ) {
                my $total_weight = $sub_groups{$identifier}{total_weight};
                my $num_files = ceil($total_weight/$max_weight);
                $num_files ||= 1;
                #next unless $total_weight;
                my $num_suffix = ceil( log($num_files) / log(26) );
                $num_suffix++ unless $num_suffix;
                my $suffix = 'a' x $num_suffix;
                my @records = @{$sub_groups{$identifier}{records}};
                my $ident = $identifier . '.' . $suffix++;
                my $records = 0;
                my %tmp_sub_sets;
                my @sub_set_records;
                my $weight = 0;
                for ( my $i=0; $i < @records; $i++ ) {
                    my $record = $records[$i];
                    my $record_weight = ( 
                                          exists $group->{wrecords}{$record}
                                          and defined $group->{wrecords}{$record}
                                        )
                                        ? $group->{wrecords}{$records[$i]}
                                        : 0;
                    if ( $record_weight >= $max_weight ) {
                        push @{$tmp_sub_sets{$record_weight}}, [ $record ];
                        next;
                    }
                    $weight += $record_weight;
                    $records++;
                    push @sub_set_records, $record;
                    my $next_record_weight = (
                                        $i < @records-1
                                        and exists $group->{wrecords}{$records[$i+1]}
                                        and defined $group->{wrecords}{$records[$i+1]}
                                      )
                                      ? $group->{wrecords}{$records[$i+1]}
                                      : 0;
                    if ( $records >= $max_record or ( $weight + $next_record_weight ) > $max_weight ) {
                        push @{$tmp_sub_sets{$weight}}, [ @sub_set_records ];
                        @sub_set_records = ();
                        $weight = 0;
                        $records = 0;
                    }
                }

                push @{$tmp_sub_sets{$weight}}, [ @sub_set_records ] if @sub_set_records;

                foreach my $count ( sort { $b <=> $a } keys %tmp_sub_sets ) {
                    foreach my $set ( @{$tmp_sub_sets{$count}} ) {
                        $sub_sets{$ident . '.' . $count} = $set;
                        $ident = $identifier . '.' . $suffix++;
                    }
                }
            }
        } elsif ( $group->exists('split_into_records') ) {
            my $num_records = $group->split_into_records;
            $self->_create_sub_sets(
                                    num_records => $num_records,
                                    sub_groups => \%sub_groups,
                                    sub_sets => \%sub_sets
                                );
        } elsif ( $group->exists('split_into_files') ) {
            my $num_files = $group->split_into_files;
            $self->_create_sub_sets(
                                    num_files => $num_files,
                                    sub_groups => \%sub_groups,
                                    sub_sets => \%sub_sets
                                );
        } else {
            foreach my $identifier ( keys %sub_groups ) {
                push @{$sub_sets{$identifier}}, @{$sub_groups{$identifier}{records}};
            }
        }
    }
    return %sub_sets;
}

=head2 dump

Dumps the subsets under the dumpdir/dump_sets by default.
If you do not like the dump_sets name, then pass your favorite sub-directory name,
For eg.

    $split->dump('test_gen')

It will delete the dump dir before dumping the subsets into it.
Each record in the file will be separated by the value of the delimeter. 

=cut

sub dump {
    my ($self, $dump_area, $disable_cleanup) = @_;

    my $dumpdir = $self->dumpdir;
    if ( defined $dump_area ) {
        croak "$dump_area cannot be blank" if $dump_area =~ /^\s*$/;
        croak "$dump_area cannot have spaces" if $dump_area =~ /\s/;
        $dumpdir .= '/' . $dump_area;
    } else {
        $dumpdir .= '/dump_sets';
    }
    system("rm -rf $dumpdir") unless (defined $disable_cleanup);
    system("mkdir -p $dumpdir") unless (-d $dumpdir);
    
    #, 0755 or die "Cannot create $dumpdir directory: $!";
    my %groups = $self->split;
    foreach my $identifier ( keys %groups ) {
        next unless defined $identifier;
        (my $i = $identifier) =~ s#/#__#g;
        next unless defined $groups{$identifier};
        next unless @{$groups{$identifier}};
        my $file = $dumpdir . '/' . $i;
        open(FILE, ">$file") or die "$file: $!";
        local $\ = $self->delimeter;
        foreach my $record ( @{$groups{$identifier}} ) {
            print FILE $record;
        }
        close(FILE);
    }
}

=head2 dumpdir

Returns the dumpdir which is going to be used to dump the subsets.
By default, it is the current working directory if no argument is specified.
You can edither set the dumpdir in the constructor or change it by passing the new dumpdir to the method

    $split->dumpdir('/my/log/dir/for/dumping/of/subsets')

=cut

sub dumpdir {
    my ($self, $dumpdir) = @_;

    if ( defined $dumpdir ) {
        $self->{dumpdir} = $dumpdir;
    } elsif ( not exists $self->{dumpdir} ) {
        $self->{dumpdir} = cwd;
    }

    return $self->{dumpdir};
}

=head2 records ( @records )

Returns the list of records which are going to split.
Pass the list of records to set new list of records.

=cut

sub records {
    my ($self, @records) = @_;

    if ( @records ) {
        $self->{records} = [ @records ];
    } elsif ( exists $self->{file} ) {
        my @records = $self->_records_by_file;
        $self->{records} = [ @records ];
    } elsif ( not exists $self->{records} ) {
        $self->{records} = [ ];
    }

    return @{$self->{records}};
}

=head2 file ( $filename )

Returns the filename which stores the record. If filename is passed then sets the filename.

=cut

sub file {
    my ($self, $file) = @_;

    if ( defined $file ) {
        $self->{file} = $file;
        my @records = $self->_records_by_file;

    }

    return $self->{file};
}

=head2 delimeter

Returns the delimeter used by the module. Is read-only so always pass it with the constructor.
The default value is the default value of C<$/> which is C<"\n">

=cut

sub delimeter {
    my ($self) = @_;

    unless ( exists $self->{delimeter} ) {
        $self->{delimeter} = $/;
    }

    return $self->{delimeter};
}

=head1 PRIVATE METHODS

Of interest to the developers

=head2 _groups

Internal method which is called by groups to create the Records::Group objects.
For now, group is created only once for each call to groups.

=cut

sub _groups {
    my ($self) = @_;

    my @grp_objects;

    my %parent_group;
    $parent_group{identifier} = $self->{identifier};

    foreach my $key ( keys %$self ) {
        next unless $key =~ /^split_/;
        next unless exists $self->{$key};
        $parent_group{$key} = $self->{$key};
    }

    $parent_group{wrecords} = $self->{wrecords} if exists $self->{wrecords};
    $parent_group{max_records} = $self->{max_records} if exists $self->{max_records};

    push @grp_objects, Records::Group->new(%parent_group);

    my %groups;
    my $identifier_count = 1;
    foreach my $group ( @{$self->{groups}} ) {
        my $identifier = exists $group->{identifier}
                              ? $group->{identifier}
                              : $parent_group{identifier} . $identifier_count++;
        $groups{$identifier} = $group;
    }

    my @exclusive_keys = qw/split_into_records split_into_files split_by_weight/;

    while( my ($identifier, $group) = each %groups ) {
        my %sub_group;
        $sub_group{identifier} = $identifier;

        my $found = 0;
        foreach my $exclusive_key ( @exclusive_keys ) {
            next unless exists $group->{$exclusive_key};
            $found = 1;
            last;
        }

        if ( $found ) {
            foreach my $exclusive_key ( @exclusive_keys ) {
                next unless exists $group->{$exclusive_key};
                $sub_group{$exclusive_key} = $group->{$exclusive_key};
            }
        } else {
            foreach my $exclusive_key ( @exclusive_keys ) {
                next unless exists $parent_group{$exclusive_key};
                $sub_group{$exclusive_key} = $parent_group{$exclusive_key};
            }
        }

        if ( exists $sub_group{split_by_weight} ) {
            if ( exists $group->{wrecords} ) {
                my %parent_wrecords = exists $parent_group{wrecords} ? %{$parent_group{wrecords}} : ();
                my %group_wrecords = %{$group->{wrecords}};
                $sub_group{wrecords} = { %parent_wrecords, %group_wrecords };
            } elsif ( exists $parent_group{wrecords} ) {
                $sub_group{wrecords} = $parent_group{wrecords};
            }
            if ( exists $group->{max_records} ) {
                $sub_group{max_records} = $group->{max_records};
            } elsif ( exists $parent_group{max_records} ) {
                $sub_group{max_records} = $parent_group{max_records};
            }
        }

        if ( exists $group->{split_by_pattern} ) {
            $sub_group{split_by_pattern} = $group->{split_by_pattern};
        } elsif ( exists $parent_group{split_by_pattern} ) {
            $sub_group{split_by_pattern} = $parent_group{split_by_pattern};
        }

        foreach my $record_source ( qw/group_by_pattern file records/ ) {
            $sub_group{$record_source} = $group->{$record_source} if exists $group->{$record_source};
        }
        push @grp_objects, Records::Group->new(%sub_group);
    }

    $self->{_groups} = [ @grp_objects ];
}

=head2 _group

Does the task of setting records to each group. 

=cut

sub _group {
    my ($self) = @_;

    my @records = $self->records;
    my %records;
    $records{$_} = undef foreach @records;

    my @groups = $self->groups;
    my $parent_group = shift @groups;
    my $identifier = $parent_group->identifier;

    foreach my $group ( @groups ) {
        next if $group->exists('records');

        if ( $group->exists('file') ) {
            my @records = $self->_records_by_file($group->file);
            $group->records(@records);
        } else { 
            my @patterns = $group->patterns;
            my $identifier = $group->identifier;
            my @recs;
            foreach my $record ( keys %records ) {
                my $found = 0;
                foreach my $pattern ( @patterns ) {
                    next unless $record =~ /$pattern/;
                    $found = 1;
                    last;
                }
                next unless $found;

                if ( $found ) {
                    push @recs, $record;
                    delete $records{$record};
                }
            }
            $group->records(@recs);
        }
        ## A hack for now, skip the creation of records part if records are given for the group
    }

    my @recs;
    foreach my $record ( keys %records ) {
        push @recs, $record;
    }
    $parent_group->records(@recs);

    $self->{_group} =  [ $parent_group, @groups ];
}

=head2 _create_sub_groups

For each group, if split_by_pattern exists then the records in the group by split into more sub-groups based on the pattern.

=cut

sub _create_sub_groups {
    my ($self, $group) = @_;

    my %subgroups;

    my @records = $group->records;
    my $identifier = $group->identifier;
    my %records;
    $records{$_} = undef foreach @records;

    if ( $group->exists('split_by_pattern') ) {
        my $split_by_pattern = $group->split_by_pattern;
        foreach my $record ( keys %records ) {
            next unless $record =~ /($split_by_pattern)/;
            push @{$subgroups{"$identifier.$1"}{records}}, $record;
            delete $records{$record};
        }
    }

    foreach my $record ( keys %records ) {
        push @{$subgroups{$identifier}{records}}, $record;
    }

    if ( $group->exists('split_by_weight') ) {
        foreach my $ident ( keys %subgroups ) {
            my $total_weight = 0;
            foreach my $record ( @{$subgroups{$ident}{records}} ) {
                my $weight = (
                                exists $group->{wrecords}{$record}
                                and defined $group->{wrecords}{$record}
                            )
                            ? $group->{wrecords}{$record}
                            : 0;
                $total_weight += $weight;
            }
            $subgroups{$ident}{total_weight} = $total_weight;
        }
    }

    return %subgroups;
}

=head2 _create_sub_sets

For each sub groups(subgroups of a group), the records are split based on the value of split_into_files or split_into_records

=cut

sub _create_sub_sets {
    my ($self, %hash) = @_;

    foreach my $identifier ( keys %{$hash{sub_groups}} ) {
        my @records = @{$hash{sub_groups}{$identifier}{records}};
        if ( exists $hash{num_files} ) {
            $hash{num_records} = ceil(@records/$hash{num_files});
        } else {
            $hash{num_files} = ceil(@records/$hash{num_records});
        }
        my $num_suffix = ceil( log($hash{num_files}) / log(26) );
        $num_suffix++ unless $num_suffix;
        my $suffix = 'a' x $num_suffix;
        my $count = 0;
        my $ident = $identifier . '.' . $suffix++;
        foreach my $record ( @records ) {
            push @{$hash{sub_sets}{$ident}}, $record;
            $count++;
            if ( $count == $hash{num_records} ) {
                $count = 0;
                $ident = $identifier . '.' . $suffix++;
            }
        }
    }
}

=head2 _records_by_file

Reads the file and sets the records. Uses the value of the delimeter to create records.

=cut

sub _records_by_file {
    my ($self, $file) = @_;

    my @records;

    $file ||= $self->file;
    open(FILE, $file) or die "$file: $!";
    local $/ = $self->delimeter;
    while(<FILE>){
        chomp;
        push @records, $_;
    }
    close(FILE);

    return @records;
}

=head2 check_params

Called by the construtor to check the number of value of the arguments

=cut

sub _check_params {
    my ($self, %params) = @_;

    # following can be in both parent and in groups
    my %allowed_keys_both = (
                            split_into_records => undef,
                            split_into_files => undef,
                            split_by_pattern => undef,
                            split_by_weight => undef,
                            identifier => undef,
                            delimeter => undef,
                            records => undef,
                            file => undef,
                            wrecords => undef,
                            max_records => undef,
                         );

    # following should be only in child
    my %allowed_keys_child = (
                            group_by_pattern => undef,
                         );

    # following should only be in parent
    my %allowed_keys_parent = (
                            groups => undef,
                            dumpdir => undef
                         );
    
    #my @exclusive_parent = ( [ qw/file records/ ] );
    my @exclusive_parent = ( [ qw/file records/ ] );
    my @exclusive_both = ( [ qw/split_into_records split_into_files split_by_weight/ ] );

    my @keys_number = qw/split_into_records split_into_files split_by_weight max_records/;
    # check valid keys
    $self->_check_keys( keys => { %params }, allowed_keys => { %allowed_keys_both, %allowed_keys_parent } );

    # _check_exclusive
    $self->_check_exclusive( keys => [ @exclusive_parent, @exclusive_both ], params => { %params } );

    # _check_number and not 0
    $self->_is_number( keys => { %params }, keys_number => [ @keys_number ] );

    # _check_file
    croak "file $params{file} does not exists" if ( exists $params{file} and not -f $params{file} );

    # _check_reference_to_array
    croak "records should be a reference to an array" if ( exists $params{records}  and ref($params{records}) ne 'ARRAY' );
    croak "records should be a reference to a hash" if ( exists $params{wrecords}  and ref($params{wrecords}) ne 'HASH' );

    return unless exists $params{groups};

    my @groups;
    if ( ref($params{groups}) eq 'ARRAY' ) {
        @groups = @{$params{groups}};
    } else {
        croak "`groups' should be a reference to an array. Look at the documentation";
    }

    foreach my $group ( @groups ) {
        $self->_check_keys(keys => { %$group }, allowed_keys => { %allowed_keys_both, %allowed_keys_child } );
        croak "records should be a reference to an array" if ( exists $group->{records}  and ref($group->{records}) ne 'ARRAY' );
        croak "records should be a reference to a hash" if ( exists $group->{wrecords}  and ref($group->{wrecords}) ne 'HASH' );
        croak "file ", $group->{file}, " does not exists" if ( exists $group->{file} and not -f $group->{file} );
        $self->_check_exclusive( keys => [ @exclusive_parent ], params => { %$group } );
        my $found = 0;
        foreach my $key ( @{$exclusive_parent[0]}, 'group_by_pattern' ) {
            next unless exists $group->{$key};
            $found = 1;
        }
        croak "Atleast one of these keys should be used in the group" unless $found;
        $self->_check_exclusive( keys => [ @exclusive_both ], params => { %$group } );
        $self->_is_number( keys => { %params }, keys_number => [ @keys_number ] );
    }
}

=head2 _check_keys

=cut

sub _check_keys {
    my ($self, %hash) = @_;

    foreach my $key ( keys %{$hash{keys}} ) {
        unless ( exists $hash{allowed_keys}{$key} ) {
            croak "`$key' is not a valid parameter. Look at the documentation for the allowed parameters";
        }
    }
}

=head2 _check_exclusive

=cut

sub _check_exclusive {
    my ($self, %hash) = @_;

    my %params = %{$hash{params}};
    foreach my $ref_array ( @{$hash{keys}} ) {
        if ( exists $params{$ref_array->[0]} and exists $params{$ref_array->[1]} ) {
            croak "`$ref_array->[0]' and `$ref_array->[1]' parameters are mutually exclusive. Use any one of them";
        }
    }
}

=head2 _is_number

=cut

sub _is_number {
    my ($self, %hash) = @_;

    foreach my $key ( @{$hash{keys_number}} ) {
        next unless exists $hash{keys}{$key};
        croak "`$key' parameter should be a positive non-zero integer" unless $hash{keys}{$key} =~ /^\d+$/;
        croak "`$key' parameter cannot be zero" if $hash{keys}{$key} == 0;
    }
}

=head1 AUTHOR

Ankur Gupta L<ankur2012@inbox.com>

=cut

1;
