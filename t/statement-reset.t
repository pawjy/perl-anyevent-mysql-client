use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->child ('lib');
use lib glob path (__FILE__)->parent->parent->child ('t_deps/modules/*/lib');
use Encode;
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
  $client->send_statement_reset (124)->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      ok $x;
      isa_ok $x, 'AnyEvent::MySQL::Client::Result';
      ok $x->is_exception;
    } $c;
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 3, name => 'not connected';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->send_statement_prepare ('create table foo0 (id int)');
  })->then (sub {
    return $client->send_statement_reset ($_[0]->packet->{statement_id})->die;
  })->catch (sub {
    test {
      ok 1;
    } $c;
    return $client->disconnect;
  })->then (sub {
    test {
      ok 1;
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 2, name => 'reset die';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->send_statement_reset;
  })->catch (sub {
    my $x = $_[0];
    test {
      ok $x;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 1, name => 'no statement id';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  my $statement_id;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->send_query ('create table foo1 (id int)');
  })->then (sub {
    return $client->send_statement_prepare ('insert into foo1 (id) values (12)');
  })->then (sub {
    my $result = $_[0];
    $statement_id = $result->packet->{statement_id};
    return $client->send_statement_reset ($statement_id);
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_success;
      ok $result->packet;
    } $c;
  })->then (sub {
    return $client->send_statement_execute ($statement_id);
  })->then (sub {
    my $x = $_[0];
    test {
      ok $x->is_success;
      ok $x->packet;
    } $c;
    my $data;
    return $client->send_query ('select * from foo1', sub {
      $data = $_[0]->packet->{data};
    })->then (sub { return $data });
  })->then (sub {
    my $result = $_[0];
    test {
      is_deeply $result, [12];
    } $c;
  })->catch (sub {
    warn $_[0];
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 7, name => 'reset ok';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  my $statement_id;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->send_query ('create table foo2 (id int)');
  })->then (sub {
    return $client->send_statement_prepare ('insert into foo2 (id) values (12)');
  })->then (sub {
    my $result = $_[0];
    $statement_id = $result->packet->{statement_id};
    return $client->send_statement_reset ($statement_id + 30);
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_failure;
      is $result->packet->{error_code}, 1243;
    } $c;
  })->then (sub {
    return $client->send_statement_execute ($statement_id);
  })->then (sub {
    my $x = $_[0];
    test {
      ok 1;
    } $c;
    my $data;
    return $client->send_query ('select * from foo2', sub {
      $data = $_[0]->packet->{data};
    })->then (sub { return $data });
  })->then (sub {
    my $result = $_[0];
    test {
      is_deeply $result, [12];
    } $c;
  })->catch (sub {
    warn $_[0];
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 6, name => 'reset not exist';

run_tests;

=head1 LICENSE

Copyright 2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
