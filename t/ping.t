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
    return $client->send_ping;
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
    } $c;
  })->then (sub {
    my $result = $_[0];
    return $client->disconnect->then (sub { return $result });
  })->catch (sub {
    test {
      ok 0;
    } $c;
    return undef;
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 1, name => 'after connect';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->send_ping->then (sub {
    my $result = $_[0];
    test {
      ok not $result;
    } $c;
  }, sub {
    test {
      ok 0;
    } $c;
  })->then (sub {
    test {
      done $c;
      undef $c;
    } $c;
  });
} n => 1, name => 'before connect';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    my $result = $_[0];
    return $client->disconnect->then (sub { return $result });
  })->then (sub {
    return $client->send_ping;
  })->then (sub {
    my $result = $_[0];
    test {
      ok not $result;
    } $c;
  })->catch (sub {
    test {
      ok 0;
    } $c;
    return undef;
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 1, name => 'after disconnect';

run_tests;

=head1 LICENSE

Copyright 2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
