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
  isa_ok $client, 'AnyEvent::MySQL::Client';
  done $c;
} n => 1, name => 'new';

test {
  my $c = shift;

  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->send_query (q{create table foo (id int, unique key (id))}, sub {});
  })->then (sub {
    return $client->send_query (q{insert into foo (id) values (12)}, sub {});
  })->then (sub {
    my @row;
    return $client->send_query (q{select id from foo}, sub {
      push @row, $_[0];
    })->then (sub { return $row[0]->packet->{data}->[0] });
  })->then (sub {
    my $result = $_[0];
    return $client->send_quit->then (sub { return $result });
  })->catch (sub {
    my $error = $_[0];
    test {
      ok 0;
      is $error, undef;
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
    my $result = $_[0];
    test {
      is $result, 12, 'result value';
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 1;

run_tests;

=head1 LICENSE

Copyright 2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
