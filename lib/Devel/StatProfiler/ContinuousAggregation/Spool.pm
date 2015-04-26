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
    my $file = $args{file} // die "File is mandatory";
    my $root_directory = $args{root_directory} // die "Root directory is mandatory";
    my $kind = $args{kind} // 'default';
    my $reader = Devel::StatProfiler::Reader->new($file);
    my $metadata = $reader->get_custom_metadata;
    my $aggregation_id = $metadata->{aggregation_id} // 'default';

    my $dest_dir = $root_directory . '/spool/' . $kind . '/' . $aggregation_id;
    my $dest = $dest_dir . '/' . File::Basename::basename($file);
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
    my @files = bsd_glob $root_directory . '/spool/*/*/*';
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

1;
