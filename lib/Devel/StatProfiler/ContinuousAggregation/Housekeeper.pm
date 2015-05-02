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

        # this way the filesystem can use "small" symlinks
        symlink '../../../sources', "$source_dir/sources"
            unless -s "$source_dir/sources";
        my %undead;
        for my $file (bsd_glob "$source_dir/??/??/*") {
            next if index($file, '.') != -1;
            my ($dir, $hash) = $file =~ m{[/\\](..[/\\]..[/\\])($hex+)$}
                or die "Unable to parse '$file'";

            if (-f $file && !-l $file) {
                my $final_link = "../../sources/$dir/$hash";
                my $final_file = "$target/$dir/$hash";
                my $temp_file = "$final_file.$$." . int(rand(2 ** 30));
                my $temp_link = "$file.$$." . int(rand(2 ** 30));

                File::Path::mkpath(["$target/$dir"]);
                unless ($existing{"$dir/$hash"}) {
                    File::Copy::copy($file, $temp_file) or die "Unable to copy to temporary file '$temp_file': $!";
                    rename $temp_file, $final_file or die "Unable to rename '$temp_file' to '$final_file': $!";
                }
                # this dance is to have atomic symlink replacement
                symlink $final_link, $temp_link or die "Unable to symlink '$temp_link': $!";
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

sub expire_data {
    my (%args) = @_;
    my $processes = $args{processes} // 1;
    my $logger = $args{logger} // die "Logger is mandatory";
    my $root_directory = $args{root_directory} // die "Root directory is mandato
ry";

    $logger->info("Deleting processed files");

    my @processed = bsd_glob $root_directory . '/processed/*';
    for my $processed (@processed) {
        unlink $processed;
    }

    $logger->info("Deleted processed files");
}

1;
