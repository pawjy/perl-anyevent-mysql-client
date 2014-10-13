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
    return $client->send_query (q{create table foo (id int, unique key (id))}, sub {});
  })->then (sub {
    return $client->send_query (q{insert into foo (id) values (12)}, sub {});
  })->then (sub {
    return $client->disconnect;
  })->then (sub {
    return $client->connect
        (hostname => 'unix/', port => $dsn{mysql_socket},
         username => $dsn{user}, password => $dsn{password},
         database => $dsn{dbname});
  })->then (sub {
    my @row;
    return $client->send_query (q{select id from foo}, sub {
      push @row, $_[0];
    })->then (sub { return $row[0]->[1]->{data}->[0] });
  })->then (sub {
    my $result = $_[0];
    test {
      is $result, 12;
    } $c;
  })->catch (sub {
    test {
      ok 0;
    } $c;
    return undef;
  })->then (sub {
    return $client->disconnect;
  })->then (sub {
    my $result = $_[0];
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 1, name => 'reconnect';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->connect
        (hostname => 'unix/', port => $dsn{mysql_socket},
         username => $dsn{user}, password => $dsn{password},
         database => $dsn{dbname})->catch (sub {
      my $result = $_[0];
      test {
        ok $result;
      } $c;
    });
  })->then (sub {
    return $client->send_ping;
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
    } $c;
  })->catch (sub {
    test {
      ok 0;
    } $c;
    return undef;
  })->then (sub {
    return $client->disconnect;
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 2, name => 'connect while connecting';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password})->then (sub {
    my @row;
    return $client->send_query (q{show databases}, sub {
      push @row, $_[0];
    })->then (sub { return scalar @row });
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
    } $c;
  })->catch (sub {
    test {
      ok 0;
    } $c;
    return undef;
  })->then (sub {
    return $client->disconnect;
  })->then (sub {
    my $result = $_[0];
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 1, name => 'with no database name';

run_tests;

=head1 LICENSE

Copyright 2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
