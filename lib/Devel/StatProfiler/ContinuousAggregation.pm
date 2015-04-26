package Devel::StatProfiler::ContinuousAggregation;
# ABSTRACT: continuous aggregation for Devel::StatProfiler reports

use strict;
use warnings;

use List::MoreUtils qw(uniq);

use Devel::StatProfiler::ContinuousAggregation::Spool;
use Devel::StatProfiler::ContinuousAggregation::Collector;
use Devel::StatProfiler::ContinuousAggregation::Generator;
use Devel::StatProfiler::ContinuousAggregation::Housekeeper;

sub new {
    my ($class, %args) = @_;
    my $self = bless {
        root_directory  => $args{root_directory},
        processes       => $args{processes},
        shard           => $args{shard} // 'local',
        compress        => $args{compress},
    }, $class;

    return $self;
}

sub move_to_spool {
    my ($self, %args) = @_;
    my $all_ok = 1;

    for my $file (@{$args{files}}) {
        my $ok = Devel::StatProfiler::ContinuousAggregation::Spool::to_spool(
            root_directory  => $self->{root_directory},
            kind            => $args{kind},
            file            => $file,
        );
        unlink $file if $ok;
        $all_ok &&= $ok;
    }

    return $all_ok;
}

sub process_profiles {
    my ($self) = @_;
    my $files = Devel::StatProfiler::ContinuousAggregation::Spool::read_spool(
        root_directory      => $self->{root_directory},
    );
    my @aggregation_ids = uniq map $_->[0], @$files;

    Devel::StatProfiler::ContinuousAggregation::Collector::process_profiles(
        root_directory      => $self->{root_directory},
        processes           => $self->{processes},
        shard               => $self->{shard},
        files               => $files,
    );
    Devel::StatProfiler::ContinuousAggregation::Collector::merge_parts(
        root_directory      => $self->{root_directory},
        processes           => $self->{processes},
        shard               => $self->{shard},
        aggregation_ids     => \@aggregation_ids,
    );
}

sub generate_reports {
    my ($self) = @_;
    my $aggregation_ids = Devel::StatProfiler::ContinuousAggregation::Collector::changed_aggregation_ids(
        root_directory      => $self->{root_directory},
    );

    Devel::StatProfiler::ContinuousAggregation::Generator::generate_reports(
        root_directory      => $self->{root_directory},
        processes           => $self->{processes},
        reports             => [map {
            [$_, undef],
        } @$aggregation_ids]
    );
}

sub collect_sources {
    my ($self) = @_;

    Devel::StatProfiler::ContinuousAggregation::Housekeeper::collect_sources(
        root_directory      => $self->{root_directory},
        processes           => $self->{processes},
    );
}

sub expire_data {
    my ($self) = @_;

    Devel::StatProfiler::ContinuousAggregation::Housekeeper::expire_data(
        root_directory      => $self->{root_directory},
        processes           => $self->{processes},
    );
}

1;

__END__

=head1 SYNOPSIS

Create the directory structure:

  /path/to/aggregation
      ... /aggregation/html
      ... /aggregation/processed
      ... /aggregation/processing
      ... /aggregation/reports
      ... /aggregation/sources
      ... /aggregation/spool

then run

  my $aggregator = Devel::StatProfiler::ContinuousAggregation->new(
      root_directory => '/path/to/aggregation',
  );

  $aggregator->move_to_spool(files => [glob 't/out/statprof.out.*']);
  $aggregator->process_profiles;
  $aggregator->generate_reports;

to aggregate profile files and generate HTML reports
