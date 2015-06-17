package Devel::StatProfiler::ContinuousAggregation::Collector;

use strict;
use warnings;

use File::Basename qw(basename);
use File::Glob qw(bsd_glob);

use Parallel::ForkManager;

use Devel::StatProfiler::Aggregator;

sub _log_fatal_subprocess_error {
    my ($logger) = @_;

    return sub {
        my ($pid, $exit, $id, $signal, $core) = @_;
        return unless $exit || $signal;

        if ($id) {
            $logger->warn("Process %s (PID %d) exited with exit code %d, signal %d", $id, $pid, $exit, $signal);
        } else {
            $logger->warn("PID %d exited with exit code %d, signal %d", $pid, $exit, $signal);
        }
    }
}

sub process_profiles {
    my (%args) = @_;
    my $files = $args{files};
    my $processes = $args{processes} // 1;
    my $max_batch_size = $args{batch_size} // 5_000_000;
    my $logger = $args{logger} // die "Logger is mandatory";
    my $root_directory = $args{root_directory} // die "Root directory is mandatory";
    my $parts_directory = $args{parts_directory} // $args{root_directory};
    my $shard = $args{shard} // die "Shard is mandatory";
    my $aggregator_class = $args{aggregator_class} // 'Devel::StatProfiler::Aggregator';
    my $serializer = $args{serializer};
    my $pm = Parallel::ForkManager->new($processes);

    $pm->run_on_finish(_log_fatal_subprocess_error($logger));

    File::Path::mkpath([$root_directory . '/processing/' . $shard]);

    my %batches;
    {
        my ($batch, $current_process, $batch_size);

        for my $file (@$files) {
            my ($aggregation_id, $process, $path, $reader) = @$file;

            if (!$current_process || ($current_process ne $process && $batch_size > $max_batch_size)) {
                $batch = $process;
                $batch_size = 0;
            }
            $current_process = $process;
            $batch_size += -s $path;
            push @{$batches{$aggregation_id}{$batch}}, [$process, $path, $reader];
        }
    }

    for my $aggregation_id (sort keys %batches) {
        for my $batch (sort keys %{$batches{$aggregation_id}}) {
            $pm->start and next; # do the fork

            my @items = @{$batches{$aggregation_id}{$batch}};
            my $report_directory = $root_directory . '/reports/' . $aggregation_id;
            my $report_parts_directory = $parts_directory . '/reports/' . $aggregation_id;

            my ($aggregator, $current_process);

            for my $item (@items) {
                my ($process, $path, $reader) = @$item;
                if (!$aggregator || $current_process ne $process) {
                    $aggregator->save_part if $aggregator;
                    $aggregator = $aggregator_class->new(
                        root_directory      => $report_directory,
                        parts_directory     => $report_parts_directory,
                        shard               => $shard,
                        flamegraph          => 1,
                        serializer          => $serializer,
                    );
                    $current_process = $process;
                }

                next unless $aggregator->can_process_trace_file($reader // $path);

                my $intermediate = $root_directory . '/processing/' . $shard . '/' . basename($path);
                my $final = $root_directory . '/processed/' . basename($path);

                next unless rename($path, $intermediate);
                $logger->info("Going to process %s", $path);

                eval {
                    $aggregator->process_trace_files($reader // $intermediate);
                    rename $intermediate, $final;

                    1;
                } or do {
                    my $error = $@ || "Zombie error";

                    # ignore this since there isn't much we can do about it
                    die if $error !~ /^Unexpected end-of-file /;
                    $logger->info("Silencing error '%s'", $@);
                };
            }

            $aggregator->save_part;

            _touch($report_directory . '/changed');

            $pm->finish;
        }
    }

    $pm->wait_all_children;
}

sub merge_parts {
    my (%args) = @_;
    my $aggregation_ids = $args{aggregation_ids};
    my $processes = $args{processes} // 1;
    my $logger = $args{logger} // die "Logger is mandatory";
    my $root_directory = $args{root_directory} // die "Root directory is mandatory";
    my $parts_directory = $args{parts_directory} // $args{root_directory};
    my $shard = $args{shard} // die "Shard is mandatory";
    my $merge_prefixes = $args{merge_prefixes};
    my $aggregator_class = $args{aggregator_class} // 'Devel::StatProfiler::Aggregator';
    my $serializer = $args{serializer};
    my $pm = Parallel::ForkManager->new($processes);

    $pm->run_on_finish(_log_fatal_subprocess_error($logger));

    my %aggregators;
    for my $aggregation_id (sort @$aggregation_ids) {
        $logger->info("Preparing to merge metadata for report %s", $aggregation_id);

        my $report_directory = $root_directory . '/reports/' . $aggregation_id;
        my $report_parts_directory = $parts_directory . '/reports/' . $aggregation_id;
        my $aggregator = $aggregator_class->new(
            root_directory      => $report_directory,
            parts_directory     => $report_parts_directory,
            shard               => $shard,
            flamegraph          => 1,
            serializer          => $serializer,
        );

        $aggregators{$aggregation_id} = $aggregator;

        $pm->start($aggregation_id) and next; # do the fork

        $logger->info("Merging metadata for report %s", $aggregation_id);
        $aggregator->merge_metadata;
        $logger->info("Merged metadata for report %s", $aggregation_id);

        $pm->finish;
    }

    for my $aggregation_id (sort @$aggregation_ids) {
        my $report_parts_directory = $parts_directory . '/reports/' . $aggregation_id;
        my $aggregator = delete $aggregators{$aggregation_id};

        for my $report_id ($aggregator->all_unmerged_reports) {
            {
                # TODO encapsulate
                my $parts_dir = $report_parts_directory . '/' . $report_id;
                next unless () = bsd_glob $parts_dir . '/parts/*';
            }

            $pm->start("$aggregation_id/$report_id") and next; # do the fork

            $logger->info("Merging report for %s of rollout %s", $report_id, $aggregation_id);

            my $merged = $aggregator->merge_report(
                $report_id,
                !$merge_prefixes ? () : (
                    remap         => [undef, $merge_prefixes],
                ),
            );
            $aggregator->add_report_metadata($report_id, {
                ('samples.' . $shard) => $merged->{aggregate}{total},
            });

            $pm->finish;
        }
    }

    $pm->wait_all_children;
}

sub changed_aggregation_ids {
    my (%args) = @_;
    my $logger = $args{logger} // die "Logger is mandatory";
    my $root_directory = $args{root_directory} // die "Root directory is mandatory";

    return [map  File::Basename::basename($_),
            grep -f "$_/changed",
                 bsd_glob $args{root_directory} . '/reports/*'];
}

sub _touch { open my $fh, '>', $_[0] or die "Unable to touch timestamp of '$_[0]': $!" }

1;
