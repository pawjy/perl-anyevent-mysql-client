use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->child ('lib');
use lib glob path (__FILE__)->parent->parent->child ('t_deps/modules/*/lib');
use Test::MySQL::CreateDatabase qw(test_dsn);
use AnyEvent::MySQL::Client;
use Test::More;
use Test::X1;

my $dsn = test_dsn 'hoge';
$dsn =~ s/^DBI:mysql://i;
my %dsn = map { split /=/, $_, 2 } split /;/, $dsn;

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->send_quit;
  })->then (sub {
    test {
      ok 1;
    } $c;
  }, sub {
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->send_ping;
  })->then (sub {
    my $result = $_[0];
    test {
      ok not $result;
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 2, name => 'after connect';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->send_quit->then (sub {
    test {
      ok 1;
    } $c;
  }, sub {
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->send_ping;
  })->then (sub {
    my $result = $_[0];
    test {
      ok not $result;
      done $c;
      undef $c;
    } $c;
  });
} n => 2, name => 'before connect';

run_tests;

=head1 LICENSE

Copyright 2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
