package Devel::StatProfiler::ContinuousAggregation::Generator;

use strict;
use warnings;
use autodie qw(symlink rename);

use File::Basename qw(basename);
use File::Glob qw(bsd_glob);

use Parallel::ForkManager;

use Devel::StatProfiler::Aggregator;

sub generate_reports {
    my (%args) = @_;
    my $aggregation_ids = $args{aggregation_ids};
    my $processes = $args{processes} // 1;
    my $logger = $args{logger} // die "Logger is mandatory";
    my $root_directory = $args{root_directory} // die "Root directory is mandatory";
    my $parts_directory = $args{parts_directory} // $args{root_directory};
    my $aggregator_class = $args{aggregator_class} // 'Devel::StatProfiler::Aggregator';
    my $make_fetchers = $args{make_fetchers};
    my $compress = $args{compress};
    my $pm = Parallel::ForkManager->new($processes);

    my %pending;

    my $move_symlink = sub {
        my ($pid, $exit, $id, $signal, $core, $data) = @_;

        # this condition means that we don't move the symlink unless all subreports
        # are successful
        if ($data->{aggregation_id} && --$pending{$data->{aggregation_id}} == 0) {
            my ($aggregation_id, $output_base, $output_final) =
                @{$data}{qw(aggregation_id output_base output_final)};

            $logger->info("Report for %s generated", $aggregation_id);

            if (-d $output_base) {
                # this dance is to have atomic symlink replacement
                unlink "$output_final.tmp";
                symlink File::Basename::basename($output_base), "$output_final.tmp";
                rename "$output_final.tmp", $output_final;
            }
        }

        return unless $exit || $signal;

        if ($id) {
            $logger->warn("Process %s (PID %d) exited with exit code %d, signal %d", $id, $pid, $exit, $signal);
        } else {
            $logger->warn("PID %d exited with exit code %d, signal %d", $pid, $exit, $signal);
        }
    };
    $pm->run_on_finish($move_symlink);

    for my $aggregation_id (@$aggregation_ids) {
        my $aggregation_directory = $root_directory . '/reports/' . $aggregation_id;
        my $changed = unlink $aggregation_directory . '/changed';

        next unless $changed;

        my @shards = $aggregator_class->shards($aggregation_directory);
        my $aggregator = $aggregator_class->new(
            root_directory => $aggregation_directory,
            shards         => \@shards,
            flamegraph     => 1,
            serializer     => 'sereal',
        );
        $aggregator->_load_all_metadata; # so it does not happen for each child
        my $output_final = $root_directory . '/html/' . $aggregation_id;
        my $output_base = $output_final . "." . $$ . "." . time;

        $logger->info("Processing aggregation %s", $aggregation_id);

        my @report_ids = $aggregator->all_reports;
        $pending{$aggregation_id} = scalar @report_ids;

        for my $report_id (@report_ids) {
            $pm->start("$aggregation_id/$report_id") and next; # do the fork

            $logger->info("Generating report for %s/%s", $aggregation_id, $report_id);

            my $report = $aggregator->merged_report($report_id, 'map_source');
            my $fetchers = !$make_fetchers ? undef :
                $make_fetchers->(
                    aggregation_id  => $aggregation_id,
                    metadata        => $report->metadata
                );
            my $report_dir = $output_base . '/' . $report_id;
            my $diagnostics = $report->output($report_dir, $compress, $fetchers);
            for my $diagnostic (@$diagnostics) {
                $logger->info('%s', $diagnostic);
            }

            $pm->finish(0, {
                aggregation_id => $aggregation_id,
                output_base    => $output_base,
                output_final   => $output_final,
            });
        }
    }

    $pm->wait_all_children;

    # delete old reports
    my @paths = bsd_glob $root_directory . '/html/*';
    my %directories; @directories{grep -d $_ && !-l $_, @paths} = ();
    my @symlinks = grep -l $_, @paths;

    for my $symlink (@symlinks) {
        my $target = readlink $symlink;

        delete $directories{$target};
    }

    for my $dead (sort keys %directories) {
        my @info = stat($dead);
        next unless @info; # somebody else was faster

        if ($info[9] > time - 1800) {
            $logger->info("Not pruning recent report directory '%s'", $dead);
        } else {
            $logger->info("Pruning report directory '%s'", $dead);

            File::Path::rmtree($dead);
        }
    }
}

1;
