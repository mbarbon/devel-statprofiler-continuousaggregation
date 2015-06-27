package Devel::StatProfiler::ContinuousAggregation;
# ABSTRACT: continuous aggregation for Devel::StatProfiler reports

use strict;
use warnings;

use List::MoreUtils qw(uniq);

use Devel::StatProfiler::ContinuousAggregation::Spool;
use Devel::StatProfiler::ContinuousAggregation::Collector;
use Devel::StatProfiler::ContinuousAggregation::Generator;
use Devel::StatProfiler::ContinuousAggregation::Housekeeper;
use Devel::StatProfiler::ContinuousAggregation::Logger;

sub new {
    my ($class, %args) = @_;
    my $simple_logger = $args{simple_logger};
    my $formatted_logger =
        $args{formatted_logger} // Devel::StatProfiler::ContinuousAggregation::Logger->new($simple_logger);
    my $self = bless {
        root_directory  => $args{root_directory},
        parts_directory => $args{parts_directory},
        aggregator_class=> $args{aggregator_class},
        processes       => $args{processes} // { default => 1 },
        shard           => $args{shard} // 'local',
        compress        => $args{compress},
        serializer      => $args{serializer},
        logger          => $formatted_logger,
    }, $class;

    return $self;
}

sub _processes_for {
    my ($self, $topic) = @_;

    return $self->{processes}{$topic} // $self->{processes}{default} // 1;
}

sub move_to_spool {
    my ($self, %args) = @_;
    my $all_ok = 1;

    for my $file (@{$args{files}}) {
        my $ok = Devel::StatProfiler::ContinuousAggregation::Spool::to_spool(
            logger          => $self->{logger},
            root_directory  => $self->{root_directory},
            kind            => $args{kind},
            aggregation_id  => $args{aggregation_id},
            file            => $file,
        );
        unlink $file if $ok;
        $all_ok &&= $ok;
    }

    return $all_ok;
}

sub process_profiles {
    my ($self, %args) = @_;
    my $files = Devel::StatProfiler::ContinuousAggregation::Spool::read_spool(
        logger              => $self->{logger},
        root_directory      => $self->{root_directory},
    );
    my @aggregation_ids = uniq map $_->[0], @$files;

    Devel::StatProfiler::ContinuousAggregation::Collector::process_profiles(
        logger              => $self->{logger},
        root_directory      => $self->{root_directory},
        parts_directory     => $self->{parts_directory},
        processes           => $self->_processes_for('collection'),
        shard               => $self->{shard},
        files               => $files,
        aggregator_class    => $self->{aggregator_class},
        serializer          => $self->{serializer},
    );
    Devel::StatProfiler::ContinuousAggregation::Collector::merge_parts(
        logger              => $self->{logger},
        root_directory      => $self->{root_directory},
        parts_directory     => $self->{parts_directory},
        processes           => $self->_processes_for('merging'),
        shard               => $self->{shard},
        aggregation_ids     => \@aggregation_ids,
        aggregator_class    => $self->{aggregator_class},
        serializer          => $self->{serializer},
        merge_prefixes      => $args{merge_prefixes},
        merge_prefixes_again=> $args{merge_prefixes_again},
    );
}

sub generate_reports {
    my ($self, %args) = @_;
    my $aggregation_ids = Devel::StatProfiler::ContinuousAggregation::Collector::changed_aggregation_ids(
        logger              => $self->{logger},
        root_directory      => $self->{root_directory},
    );

    Devel::StatProfiler::ContinuousAggregation::Generator::generate_reports(
        logger              => $self->{logger},
        root_directory      => $self->{root_directory},
        processes           => $self->_processes_for('aggregation'),
        aggregation_ids     => $aggregation_ids,
        make_fetchers       => $args{make_fetchers},
        aggregator_class    => $self->{aggregator_class},
        serializer          => $self->{serializer},
        compress            => $self->{compress},
    );
}

sub collect_sources {
    my ($self) = @_;

    Devel::StatProfiler::ContinuousAggregation::Housekeeper::collect_sources(
        logger              => $self->{logger},
        root_directory      => $self->{root_directory},
        processes           => $self->_processes_for('source_collection'),
    );
}

sub expire_data {
    my ($self) = @_;

    Devel::StatProfiler::ContinuousAggregation::Housekeeper::expire_data(
        logger              => $self->{logger},
        root_directory      => $self->{root_directory},
        processes           => $self->_processes_for('data_expiration'),
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
