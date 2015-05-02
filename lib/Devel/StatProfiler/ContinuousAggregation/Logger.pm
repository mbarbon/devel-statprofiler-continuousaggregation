package Devel::StatProfiler::ContinuousAggregation::Logger;

use strict;
use warnings;

sub new {
    my ($class, $logger) = @_;
    my $self = bless {
        logger  => $logger // 'Devel::StatProfiler::ContinuousAggregation::Logger::Stdout',
    }, $class;

    return $self;
}

sub info {
    shift->{logger}->info(_format(@_));
}

sub warn {
    shift->{logger}->warn(_format(@_));
}

sub _format {
    if (@_ > 1) {
        my $format = shift;

        return sprintf $format, @_;
    } elsif (@_ == 1) {
        return $_[0] =~ s{%%}{%}gr;
    } else {
        Carp::confess("Please pass something to the logger...");
    }
}

package Devel::StatProfiler::ContinuousAggregation::Logger::Stdout;

sub info {
    print $_[1], "\n";
}

sub warn {
    print $_[1], "\n";
}

1;
