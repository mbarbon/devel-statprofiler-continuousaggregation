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
        timebox         => $args{timebox},
        timebox_periods => $args{timebox_periods},
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
            parts_directory => $self->{parts_directory},
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
    my $root_files = Devel::StatProfiler::ContinuousAggregation::Spool::read_spool(
        logger              => $self->{logger},
        root_directory      => $self->{root_directory},
        local_spool         => 0,
    );
    my $local_files = $self->{parts_directory} ? Devel::StatProfiler::ContinuousAggregation::Spool::read_spool(
        logger              => $self->{logger},
        root_directory      => $self->{root_directory},
        parts_directory     => $self->{parts_directory},
        local_spool         => 1,
    ) : [];
    my @aggregation_ids = uniq map $_->[0], (@$root_files, @$local_files);

    for my $files ($root_files, $local_files) {
        Devel::StatProfiler::ContinuousAggregation::Collector::process_profiles(
            logger              => $self->{logger},
            root_directory      => $self->{root_directory},
            parts_directory     => $self->{parts_directory},
            processes           => $self->_processes_for('collection'),
            shard               => $self->{shard},
            files               => $files,
            local_spool         => $files == $local_files,
            aggregator_class    => $self->{aggregator_class},
            serializer          => $self->{serializer},
            timebox             => $self->{timebox},
            map_names           => $args{map_names},
        );
    }
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
        timebox             => $self->{timebox},
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
        serializer          => $self->{serializer},
        compress            => $self->{compress},
        timebox             => $self->{timebox},
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

sub expire_stale_data {
    my ($self) = @_;

    Devel::StatProfiler::ContinuousAggregation::Housekeeper::expire_stale_data(
        logger              => $self->{logger},
        root_directory      => $self->{root_directory},
        processes           => $self->_processes_for('data_expiration'),
    );
}

sub expire_stale_local_data {
    my ($self) = @_;

    Devel::StatProfiler::ContinuousAggregation::Housekeeper::expire_stale_local_data(
        logger              => $self->{logger},
        root_directory      => $self->{root_directory},
        parts_directory     => $self->{parts_directory},
        processes           => $self->_processes_for('data_expiration'),
    );
}

sub expire_timeboxed_data {
    my ($self) = @_;

    Devel::StatProfiler::ContinuousAggregation::Housekeeper::expire_timeboxed_data(
        logger              => $self->{logger},
        root_directory      => $self->{root_directory},
        shard               => $self->{shard},
        timebox             => $self->{timebox},
        timebox_periods     => $self->{timebox_periods},
    );
}

sub cleanup_old_reports {
    my ($self) = @_;

    Devel::StatProfiler::ContinuousAggregation::Housekeeper::cleanup_old_reports(
        logger              => $self->{logger},
        root_directory      => $self->{root_directory},
        processes           => $self->_processes_for('data_expiration'),
    );
}

sub move_to_global_spool {
    my ($self, %args) = @_;

    Devel::StatProfiler::ContinuousAggregation::Spool::move_to_global_spool(
        logger              => $self->{logger},
        root_directory      => $self->{root_directory},
        parts_directory     => $self->{parts_directory},
        local_spool_life    => $args{local_spool_life},
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
