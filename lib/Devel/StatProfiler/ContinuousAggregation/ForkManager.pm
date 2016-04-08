package Devel::StatProfiler::ContinuousAggregation::ForkManager;

use strict;
use warnings;
use parent 'Parallel::ForkManager';

sub run_on_before_start {
    my ($self, $code) = @_;
    $self->{on_before_start} = $code;
}

sub start {
    my $self = shift;
    $self->{on_before_start}->() if $self->{on_before_start};
    $self->SUPER::start(@_);
}

1;
