#!/usr/bin/env perl

use strict;
use warnings;
use Test::More tests => 7;

use_ok('Devel::StatProfiler::ContinuousAggregation::Fetcher::Git');
use_ok('Devel::StatProfiler::ContinuousAggregation::Logger');
use_ok('Devel::StatProfiler::ContinuousAggregation::Spool');
use_ok('Devel::StatProfiler::ContinuousAggregation::Collector');
use_ok('Devel::StatProfiler::ContinuousAggregation::Generator');
use_ok('Devel::StatProfiler::ContinuousAggregation::Housekeeper');
use_ok('Devel::StatProfiler::ContinuousAggregation');
