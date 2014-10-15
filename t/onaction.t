use strict;
use warnings;
no warnings 'once';
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->child ('lib');
use lib glob path (__FILE__)->parent->parent->child ('t_deps/modules/*/lib');
use Test::MySQL::CreateDatabase qw(test_dsn);
use Test::More;
use Test::X1;

my $dsn = test_dsn 'hoge';
$dsn =~ s/^DBI:mysql://i;
my %dsn = map { split /=/, $_, 2 } split /;/, $dsn;

my @ActionLog;
my $i = 0;
$AnyEvent::MySQL::Client::OnActionInit = sub {
  my %args = @_;
  push @ActionLog, [$i, \%args];
  return $i++;
};
$AnyEvent::MySQL::Client::OnActionStart = sub {
  my %args = @_;
  push @ActionLog, [\%args];
};
$AnyEvent::MySQL::Client::OnActionEnd = sub {
  my %args = @_;
  push @ActionLog, \%args;
};

require AnyEvent::MySQL::Client;

test {
  my $c = shift;

  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->query (q{create table foo (id int, unique key (id))}, sub {});
  })->then (sub {
    return $client->statement_prepare (q{insert into foo (id) values (?)}, sub {});
  })->then (sub {
    return $client->statement_execute ($_[0]->packet->{statement_id}, [{type => 'LONG', value => 42}]);
  })->then (sub {
    return $client->query (q{insert into hoge (id) values (42)});
  })->then (sub {
    my $result = $_[0];
    return $client->quit->then (sub { return $result });
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
      is scalar @ActionLog, 5*3;

      is $ActionLog[0]->[1]->{action_type}, 'connect';
      is $ActionLog[0]->[1]->{object}, $client;
      is $ActionLog[0]->[1]->{hostname}, 'unix/';
      is $ActionLog[0]->[1]->{port}, $dsn{mysql_socket};
      is $ActionLog[0]->[1]->{database}, $dsn{dbname};
      is $ActionLog[1]->[0]->{state}, $ActionLog[0]->[0];
      is $ActionLog[2]->{state}, $ActionLog[0]->[0];
      ok $ActionLog[2]->{result}->is_success;

      is $ActionLog[3]->[1]->{action_type}, 'query';
      is $ActionLog[3]->[1]->{object}, $client;
      is $ActionLog[3]->[1]->{query}, 'create table foo (id int, unique key (id))';
      is $ActionLog[4]->[0]->{state}, $ActionLog[3]->[0];
      is $ActionLog[5]->{state}, $ActionLog[3]->[0];
      ok $ActionLog[5]->{result}->is_success;

      is $ActionLog[6]->[1]->{action_type}, 'statement_prepare';
      is $ActionLog[6]->[1]->{object}, $client;
      is $ActionLog[6]->[1]->{query}, 'insert into foo (id) values (?)';
      is $ActionLog[7]->[0]->{state}, $ActionLog[6]->[0];
      is $ActionLog[8]->{state}, $ActionLog[6]->[0];
      ok $ActionLog[8]->{result}->is_success;

      is $ActionLog[9]->[1]->{action_type}, 'statement_execute';
      is $ActionLog[9]->[1]->{object}, $client;
      ok defined $ActionLog[9]->[1]->{statement_id};
      is_deeply $ActionLog[9]->[1]->{params}, [{type => 'LONG', value => 42}];
      is $ActionLog[10]->[0]->{state}, $ActionLog[9]->[0];
      is $ActionLog[11]->{state}, $ActionLog[9]->[0];
      ok $ActionLog[11]->{result}->is_success;

      is $ActionLog[12]->[1]->{action_type}, 'query';
      is $ActionLog[12]->[1]->{object}, $client;
      is $ActionLog[12]->[1]->{query}, 'insert into hoge (id) values (42)';
      is $ActionLog[13]->[0]->{state}, $ActionLog[12]->[0];
      is $ActionLog[14]->{state}, $ActionLog[12]->[0];
      ok $ActionLog[14]->{result}->is_failure;

      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 34;

run_tests;

@ActionLog = ();

=head1 LICENSE

Copyright 2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
