#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;
use File::Path;
use File::Basename;
use List::Util qw(all);
use Devel::StatProfiler::ContinuousAggregation;

{
    package DummyLogger;

    sub info { }
    sub warn { }
}

sub _is_empty { return -d $_[0] && (() = glob($_[0] . '/*')) == 0 }

File::Path::rmtree(['t/out', 't/aggregate']);
File::Path::mkpath([
    't/out',
    't/aggregate/html',
    't/aggregate/processed',
    't/aggregate/processing',
    't/aggregate/reports',
    't/aggregate/sources',
    't/aggregate/spool',
]);

my $aggregator = Devel::StatProfiler::ContinuousAggregation->new(
    simple_logger  => 'DummyLogger',
    root_directory => 't/aggregate',
);

unlink $_ for glob 't/out/statprof.out.*';

system $^X, 't/bin/test1.pl', 0;
my ($profile1) = map File::Basename::basename($_), glob 't/out/statprof.out.*';
system $^X, 't/bin/test2.pl', 0;
my ($profile2) = grep $_ ne $profile1,
                 map  File::Basename::basename($_),
                      glob 't/out/statprof.out.*';

$aggregator->move_to_spool(files => [glob 't/out/statprof.out.*']);
ok(all { !-f "t/out/$_" } $profile1, $profile2);
ok(all { -f "t/aggregate/spool/default/test1/$_" } $profile1);
ok(all { -f "t/aggregate/spool/default/test2/$_" } $profile2);

$aggregator->process_profiles;
ok(all { -f "t/aggregate/processed/$_" } $profile1, $profile2);
ok(_is_empty("t/aggregate/spool/default/test1"));
ok(_is_empty("t/aggregate/spool/default/test2"));
ok(-d 't/aggregate/reports/test1/__main__');
ok(-d 't/aggregate/reports/test2/__main__');

$aggregator->generate_reports;
$aggregator->cleanup_old_reports;
ok(-l 't/aggregate/html/test1');
ok(-l 't/aggregate/html/test2');
my ($rep1b, $rep2b) = map readlink "t/aggregate/html/$_", qw(test1 test2);
ok(-f 't/aggregate/html/test1/__main__/all_stacks_by_time.calls');
ok(-f 't/aggregate/html/test2/__main__/all_stacks_by_time.calls');

system $^X, 't/bin/test1.pl', 1;
my ($profile3) = map File::Basename::basename($_), glob 't/out/statprof.out.*';

$aggregator->move_to_spool(files => [glob 't/out/statprof.out.*']);
ok(all { !-f "t/out/$_" } $profile3);
ok(all { -f "t/aggregate/spool/default/test1/$_" } $profile3);

$aggregator->process_profiles;
ok(all { -f "t/aggregate/processed/$_" } $profile3);
ok(_is_empty("t/aggregate/spool/default/test1"));

$aggregator->generate_reports;
$aggregator->cleanup_old_reports;
ok(-l 't/aggregate/html/test1');
ok(-l 't/aggregate/html/test2');
my ($rep1a, $rep2a) = map readlink "t/aggregate/html/$_", qw(test1 test2);
isnt($rep1a, $rep1b);
is($rep2a, $rep2b);
ok(!-d "t/aggregate/html/$rep1b");
ok(-d "t/aggregate/html/$rep1a");
ok(-d "t/aggregate/html/$rep2a");
ok(-f 't/aggregate/html/test1/__main__/all_stacks_by_time.calls');
ok(-f 't/aggregate/html/test2/__main__/all_stacks_by_time.calls');

done_testing;
