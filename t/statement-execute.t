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
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->send_query ('create table foo1 (id int)');
  })->then (sub {
    return $client->send_statement_prepare ('insert into foo1 (id) values (12)');
  })->then (sub {
    my $result = $_[0];
    my $statement_id = $result->packet->{statement_id};
    return $client->send_statement_execute ($statement_id);
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_success;
      ok $result->packet;
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
} n => 5, name => 'execute ok';

for my $value (
  {type => 'LONG', value => 12, i => 2},
  {type => 'TINY', value => 12, i => 3},
  {type => 'STRING', value => 12, i => 4},
  {type => 'BLOB', value => 12, i => 5},
  {type => 'LONGLONG', value => 12, i => 101},
  {type => 'SHORT', value => 12, i => 102},
  {type => 'DOUBLE', value => 12, i => 103},
  {type => 'FLOAT', value => 12, i => 104},
  {type => 'DECIMAL', value => 12, i => 105},
  {type => 'VAR_STRING', value => 12, i => 106},
  {type => 'ENUM', value => 12, i => 107},
  {type => 'BIT', value => 12, i => 108},
  {type => 'LONG', unsigned => 1, value => 12, i => 109},
  {type => 'TINY', unsigned => 1, value => 12, i => 110},
  {type => 'STRING', unsigned => 1, value => 12, i => 111},
  {type => 'BLOB', unsigned => 1, value => 12, i => 112},
  {type => 'LONGLONG', unsigned => 1, value => 12, i => 113},
  {type => 'SHORT', unsigned => 1, value => 12, i => 114},
  {type => 'DOUBLE', unsigned => 1, value => 12, i => 115},
  {type => 'FLOAT', unsigned => 1, value => 12, i => 124},
  {type => 'DECIMAL', unsigned => 1, value => 12, i => 125},
  {type => 'VAR_STRING', unsigned => 1, value => 12, i => 126},
  {type => 'ENUM', unsigned => 1, value => 12, i => 127},
  {type => 'BIT', unsigned => 1, value => 12, i => 128},
) {
  test {
    my $c = shift;
    my $client = AnyEvent::MySQL::Client->new;
    my $x = $value->{i};
    $client->connect
        (hostname => 'unix/', port => $dsn{mysql_socket},
         username => $dsn{user}, password => $dsn{password},
         database => $dsn{dbname})->then (sub {
      return $client->send_query ("create table foo$x (id int)");
    })->then (sub {
      return $client->send_statement_prepare ("insert into foo$x (id) values (?)");
    })->then (sub {
      my $result = $_[0];
      my $statement_id = $result->packet->{statement_id};
      return $client->send_statement_execute ($statement_id, [$value]);
    })->then (sub {
      my $result = $_[0];
      test {
        ok $result;
        isa_ok $result, 'AnyEvent::MySQL::Client::Result';
        ok $result->is_success;
        ok $result->packet;
      } $c;
      my @data;
      return $client->send_query ("select * from foo$x", sub {
        push @data, $_[0]->packet->{data};
      })->then (sub { return \@data });
    })->then (sub {
      my $result = $_[0];
      test {
        is_deeply $result, [[12]];
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
  } n => 5, name => ['execute ok, placeholder', $value->{type}];
}

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->send_query ('create table foo6 (id int)');
  })->then (sub {
    return $client->send_statement_prepare ('insert into foo6 (id) values (12)');
  })->then (sub {
    my $result = $_[0];
    my $statement_id = $result->packet->{statement_id};
    return $client->send_statement_execute ($statement_id, [{type => 'LONG', value => 10}, {type => 'STRING', value => ''}]);
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_success;
      ok $result->packet;
    } $c;
    my $data;
    return $client->send_query ('select * from foo6', sub {
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
} n => 5, name => 'execute ok, redundant placeholder params';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->send_query ('create table foo7 (id int)');
  })->then (sub {
    return $client->send_statement_prepare ('insert into foo7 (id) values (?), (?)');
  })->then (sub {
    my $result = $_[0];
    my $statement_id = $result->packet->{statement_id};
    return $client->send_statement_execute ($statement_id, []);
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_failure;
      ok $result->packet;
      is $result->packet->{error_code}, 1210;
    } $c;
    my @data;
    return $client->send_query ('select * from foo7 order by id asc', sub {
      push @data, $_[0]->packet->{data};
    })->then (sub { return \@data });
  })->then (sub {
    my $result = $_[0];
    test {
      is_deeply $result, [];
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
} n => 6, name => 'execute ok, missing placeholder params';

for my $value (
  {type => 'LONG', value => -12, i => 202, signed_only => 1},
  {type => 'TINY', value => -12, i => 203, signed_only => 1},
  {type => 'STRING', value => -12, i => 204},
  {type => 'BLOB', value => -12, i => 205},
  {type => 'LONGLONG', value => -12, i => 211, signed_only => 1},
  {type => 'SHORT', value => -12, i => 212, signed_only => 1},
  {type => 'DOUBLE', value => -12, i => 213},
  {type => 'FLOAT', value => -12, i => 214},
  {type => 'DECIMAL', value => -12, i => 215},
  {type => 'VAR_STRING', value => -12, i => 216},
  {type => 'ENUM', value => -12, i => 217},
  {type => 'BIT', value => -12, i => 218},
) {
  test {
    my $c = shift;
    my $client = AnyEvent::MySQL::Client->new;
    my $x = $value->{i};
    $client->connect
        (hostname => 'unix/', port => $dsn{mysql_socket},
         username => $dsn{user}, password => $dsn{password},
         database => $dsn{dbname})->then (sub {
      return $client->send_query ("create table foo$x (id int)");
    })->then (sub {
      return $client->send_statement_prepare ("insert into foo$x (id) values (?)");
    })->then (sub {
      my $result = $_[0];
      my $statement_id = $result->packet->{statement_id};
      return $client->send_statement_execute ($statement_id, [$value]);
    })->then (sub {
      my $result = $_[0];
      test {
        ok $result;
        isa_ok $result, 'AnyEvent::MySQL::Client::Result';
        ok $result->is_success;
        ok $result->packet;
      } $c;
      my @data;
      return $client->send_query ("select * from foo$x", sub {
        push @data, $_[0]->packet->{data};
      })->then (sub { return \@data });
    })->then (sub {
      my $result = $_[0];
      test {
        is_deeply $result, [[-12]];
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
  } n => 5, name => ['execute unsigned', $value->{type}];

  test {
    my $c = shift;
    my $client = AnyEvent::MySQL::Client->new;
    my $x = $value->{i};
    $client->connect
        (hostname => 'unix/', port => $dsn{mysql_socket},
         username => $dsn{user}, password => $dsn{password},
         database => $dsn{dbname})->then (sub {
      return $client->send_query ("create table bar$x (id int)");
    })->then (sub {
      return $client->send_statement_prepare ("insert into bar$x (id) values (?)");
    })->then (sub {
      my $result = $_[0];
      my $statement_id = $result->packet->{statement_id};
      return $client->send_statement_execute ($statement_id, []);
    })->then (sub {
      my $result = $_[0];
      test {
        ok $result;
        isa_ok $result, 'AnyEvent::MySQL::Client::Result';
        ok $result->is_failure;
        is $result->packet, undef;
        like $result->message, qr{XXX};
      } $c;
      my @data;
      return $client->send_query ("select * from bar$x order by id asc", sub {
        push @data, $_[0]->packet->{data};
      })->then (sub { return \@data });
    })->then (sub {
      my $result = $_[0];
      test {
        is_deeply $result, [];
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
  } n => 5, name => ['execute unsigned, out of range', $value->{type}]
      if $value->{short_only};
}

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->send_query ('create table foo21 (id int, name blob, data tinyint(10))');
  })->then (sub {
    return $client->send_statement_prepare ('insert into foo21 (name, id, data) values (?, ?, ?)');
  })->then (sub {
    my $result = $_[0];
    my $statement_id = $result->packet->{statement_id};
    return $client->send_statement_execute ($statement_id, [{type => 'STRING', value => undef}, {type => 'LONG', value => 10}, {type => 'BLOB', value => undef}]);
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_success;
      ok $result->packet;
    } $c;
    my @data;
    return $client->send_query ('select * from foo21', sub {
      push @data, $_[0]->packet->{data};
    })->then (sub { return \@data });
  })->then (sub {
    my $result = $_[0];
    test {
      is_deeply $result, [[10, undef, undef]];
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
} n => 5, name => 'execute ok, redundant placeholder params';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->send_query ('create table foo22 (id int)');
  })->then (sub {
    return $client->send_statement_prepare ('insert into foo22 (id) values (?)');
  })->then (sub {
    my $result = $_[0];
    my $statement_id = $result->packet->{statement_id};
    return $client->send_statement_execute ($statement_id, [{type => 'STRING', value => "\x{400}"}]);
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_failure;
      is $result->packet, undef;
      like $result->message, qr{utf8};
    } $c;
    my @data;
    return $client->send_query ('select * from foo22 order by id asc', sub {
      push @data, $_[0]->packet->{data};
    })->then (sub { return \@data });
  })->then (sub {
    my $result = $_[0];
    test {
      is_deeply $result, [];
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
} n => 6, name => 'utf8-flagged parameter value';

run_tests;

=head1 LICENSE

Copyright 2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
