package Devel::StatProfiler::ContinuousAggregation::Housekeeper;

use strict;
use warnings;

use File::Glob qw(bsd_glob);

use Parallel::ForkManager;

sub collect_sources {
    my (%args) = @_;
    my $processes = $args{processes} // 1;
    my $logger = $args{logger} // die "Logger is mandatory";
    my $root_directory = $args{root_directory} // die "Root directory is mandatory";

    my $target = $root_directory . '/sources';
    my @source_dirs = bsd_glob $root_directory . '/reports/*/__source__';
    my $hex = '[0-9a-fA-F]';

    my $pm = Parallel::ForkManager->new($processes);

    File::Path::mkpath([$target]);

    $logger->info("Getting the list of shared source code files");

    my (%existing, %dead);

    $pm->run_on_finish(sub {
        my ($pid, $exit_code, $ident, $signal, $cored, $undead) = @_;

        delete $dead{$_} for keys %$undead;
    });

    for my $file (bsd_glob $target . '/??/??/*') {
        my ($dir, $hash) = $file =~ m{[/\\](..[/\\]..[/\\])($hex+)$}
            or die "Unable to parse '$file'";

        $existing{"$dir/$hash"} = $dead{"$dir/$hash"} = 1;
    }

    for my $source_dir (@source_dirs) {
        $pm->start and next; # do the fork

        $logger->info("Compacting source code for %s", File::Basename::basename(File::Basename::dirname($source_dir)));

        my %undead;
        for my $file (bsd_glob "$source_dir/??/??/*") {
            next if index($file, '.') != -1;
            my ($dir, $hash) = $file =~ m{[/\\](..[/\\]..[/\\])($hex+)$}
                or die "Unable to parse '$file'";
            my ($nlink) = (stat($file))[3];

            if ($nlink == 1 && -f $file && !-l $file) {
                my $final_file = "$target/$dir/$hash";
                my $temp_file = "$final_file.$$." . int(rand(2 ** 30));
                my $temp_link = "$file.$$." . int(rand(2 ** 30));

                File::Path::mkpath(["$target/$dir"]);
                unless ($existing{"$dir/$hash"}) {
                    File::Copy::copy($file, $temp_file) or die "Unable to copy to temporary file '$temp_file': $!";
                    rename $temp_file, $final_file or die "Unable to rename '$temp_file' to '$final_file': $!";
                }
                # this dance is to have atomic link replacement
                link $final_file, $temp_link or die "Unable to link '$temp_link': $!";
                rename $temp_link, $file or die "Unable to rename '$temp_link' to '$file': $!";
            }

            $undead{"$dir/$hash"} = undef;
        }

        $pm->finish(0, \%undead);
    }

    $pm->wait_all_children;

    $logger->info("Removing dead source code");

    for my $dead (keys %dead) {
        unlink "$target/$dead";
    }
}

sub _expire_data {
    my ($base_directory, %args) = @_;
    my $logger = $args{logger} // die "Logger is mandatory";

    $logger->info("Deleting processed files from '%s'", $base_directory);

    my @processed = bsd_glob $base_directory . '/processed/*';
    for my $processed (@processed) {
        unlink $processed;
    }

    $logger->info("Deleted processed files from '%s'", $base_directory);
}

sub expire_stale_local_data {
    my (%args) = @_;
    my $parts_directory = $args{parts_directory} // die "Parts directory is mandatory";;

    _expire_data($parts_directory, %args);
}

sub expire_stale_data {
    my (%args) = @_;
    my $root_directory = $args{root_directory} // die "Root directory is mandatory";

    _expire_data($root_directory, %args);
}

sub expire_timeboxed_data {
    my (%args) = @_;
    my $logger = $args{logger} // die "Logger is mandatory";
    my $root_directory = $args{root_directory} // die "Root directory is mandatory";
    my $shard = $args{shard} // die "Shard is mandatory";
    my $aggregator_class = $args{aggregator_class} // 'Devel::StatProfiler::Aggregator';
    my $timebox = $args{timebox} // die "Timebox is mandatory";
    my $timebox_periods = $args{timebox_periods} // die "Number of periods is mandatory";

    my @aggregation_dirs = bsd_glob $root_directory . '/reports/*';

    for my $aggregation_dir (@aggregation_dirs) {
        my $aggregator = $aggregator_class->new(
            root_directory => $aggregation_dir,
            shard          => $shard,
            timebox        => $timebox,
        );

        for my $report_name (@{$aggregator->report_names}) {
            my @timeboxes = sort { $b->[0] <=> $a->[0] } map {
                my (undef, $timestamp) = split /\./, File::Basename::basename($_, $shard);

                [$timestamp, $_];
            } bsd_glob $aggregation_dir . '/' . $report_name . "/report.*.${shard}";

            if (@timeboxes > $timebox_periods) {
                my @to_delete = @timeboxes[$timebox_periods .. $#timeboxes];

                $logger->info(
                    "Removing %d timeboxed periods for %s/%s",
                    scalar @to_delete,
                    File::Basename::basename($aggregation_dir),
                    $report_name,
                );

                unlink $_->[1] for @to_delete;
            }
        }
    }
}

1;
