#!/usr/bin/env perl

use strict;
use warnings;

use Devel::StatProfiler
    -template => 't/out/statprof.out',
    -metadata => {
        aggregation_id  => 'test2',
    };
use Time::HiRes qw(sleep);

if ($ARGV[0]) {
    sleep 0.2;
} else {
    sleep 0.3;
}
