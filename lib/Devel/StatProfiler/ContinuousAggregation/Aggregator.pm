package Devel::StatProfiler::ContinuousAggregation::Aggregator;

use strict;
use warnings;
use parent 'Devel::StatProfiler::Aggregator';

sub handle_section_change {
    my ($self, $sc, $metadata) = @_;
    my $section_metadata = $self->section_metadata_key;
    my $group_name = $metadata->{$section_metadata} || $self->default_section;

    $sc->delete_custom_metadata([$section_metadata]) if $group_name;
    return $group_name ? ['__main__', $group_name] : ['__main__'];
}

sub section_metadata_key { '' }
sub default_section { }

1;
