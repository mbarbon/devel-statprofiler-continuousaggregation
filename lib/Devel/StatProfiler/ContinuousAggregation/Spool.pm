package Devel::StatProfiler::ContinuousAggregation::Spool;

use strict;
use warnings;

use File::Basename qw();
use File::Copy qw();
use File::Glob qw(bsd_glob);
use File::Path qw();

use Devel::StatProfiler::Reader;

sub to_spool {
    my (%args) = @_;
    my $file_target = $args{file} // die "File is mandatory";
    my $file = ref($file_target) ? $file_target->[0] : $file_target;
    my $target = ref($file_target) ? $file_target->[1] : File::Basename::basename($file);
    my $root_directory = $args{root_directory} // die "Root directory is mandatory";
    my $parts_directory = $args{parts_directory};
    my $kind = $args{kind} // 'default';
    my $reader = Devel::StatProfiler::Reader->new($file);
    my $metadata = $reader->get_custom_metadata;
    my $aggregation_id = $metadata->{aggregation_id} // $args{aggregation_id} // 'default';

    my $dest_dir = ($parts_directory // $root_directory) . '/spool/' . $kind . '/' . $aggregation_id;
    my $dest = $dest_dir . '/' . $target;
    my $tmp = $dest . '.tmp';

    File::Path::mkpath([$dest_dir]);
    if (!File::Copy::copy($file, $tmp)) {
        warn "Error copying file '$file' to spool: $!";
        return 0;
    }
    if (!rename($tmp, $dest)) {
        warn "Error renaming file '$file' to final name: $!";
        unlink $tmp;
        return 0;
    }

    return 1;
}

sub read_spool {
    my (%args) = @_;
    my $root_directory = $args{root_directory} // die "Root directory is mandatory";
    my $parts_directory = $args{parts_directory};
    my $local_spool = $args{local_spool};

    my @files = bsd_glob(($local_spool ? $parts_directory : $root_directory ) . '/spool/*/*/*');
    my @result;

    for my $file (@files) {
        next unless $file =~ /\.([0-9a-f]{48})\.[0-9a-f]{8}$/;
        my $aggregation_id = File::Basename::basename(File::Basename::dirname($file));

        push @result, [
            $aggregation_id, $1, $file,
        ];
    }

    return \@result;
}

sub move_to_global_spool {
    my (%args) = @_;
    my $logger = $args{logger} // die "Logger is mandatory";
    my $root_directory = $args{root_directory} // die "Root directory is mandatory";
    my $parts_directory = $args{parts_directory} // die "Parts directory is mandatory";
    my $local_spool_life = $args{local_spool_life} // die "Local spool life is mandatory";
    my @files = bsd_glob $parts_directory . '/spool/*/*/*';
    my $now = time;

    for my $file (@files) {
        my ($mtime) = (stat($file))[9];

        next unless $mtime;
        next if $mtime > $now - $local_spool_life;

        my $base = substr $file, length($parts_directory);
        my $target = $root_directory . $base;
        my $intermediate = $parts_directory . '/processing/' . File::Basename::basename($file);

        File::Path::mkpath([File::Basename::dirname($target)]);

        next unless rename($file, $intermediate);
        $logger->info("Moving %s to global spool", $file);

        unless (File::Copy::copy($intermediate, $target)) {
            $logger->warn("Failed to move %s to global spool: $!", $file);
            next;
        }

        unlink $intermediate;
    }
}

1;
