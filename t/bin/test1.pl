#!/usr/bin/env perl

use strict;
use warnings;

use Devel::StatProfiler
    -template => 't/out/statprof.out',
    -metadata => {
        aggregation_id  => 'test1',
    };
use Time::HiRes qw(sleep);

if ($ARGV[0]) {
    sleep 0.5;
} else {
    sleep 0.6;
}
