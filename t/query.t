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
  $client->query ('show tables')->then (sub {
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
    return $client->query ('create table foo (id int)')->die;
  })->catch (sub {
    return $client->disconnect;
  })->then (sub {
    test {
      ok 1;
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 1, name => 'query die';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->query ('create table foo (id int)')->then (sub {
      return $client->query ('insert into foo (id) values (15)');
    })->then (sub {
      return $client->query ('select * from foo');
    });
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_success;
      is ref $result->column_packets, 'ARRAY';
      is_deeply [map { $_->{name} } @{$result->column_packets}], ['id'];
      ok $result->packet;
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
} n => 6, name => 'query with no args, success';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->query ('create table');
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok not $result->is_success;
      ok $result->is_failure;
      is $result->column_packets, undef;
      ok $result->packet;
      is $result->packet->{error_code}, '1064', 'syntax error';
      #warn $result->packet->{error_message};
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
} n => 7, name => 'query with no args, success';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  my @row;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->query ('create table foox (id int)')->then (sub {
      return $client->query ('insert into foox (id) values (15), (31)');
    })->then (sub {
      return $client->query ('select * from foox order by id asc', sub {
        push @row, $_[0];
      });
    });
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_success;
      is ref $result->column_packets, 'ARRAY';
      is_deeply [map { $_->{name} } @{$result->column_packets}], ['id'];
      ok $result->packet;

      is scalar @row, 2;
      my @data;
      for (@row) {
        isa_ok $_, 'AnyEvent::MySQL::Client::Result';
        ok $_->is_success;
        isa_ok $_->packet, 'AnyEvent::MySQL::Client::ReceivedPacket';
        is_deeply [map { $_->{name} } @{$_->column_packets}], ['id'];
        push @data, $_->packet->{data};
      }
      is_deeply \@data, [[15], [31]];
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
} n => 16, name => 'query with with callback, success';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  my @result;
  my $i = 10;
  my $j = 100;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->query ('create table foo2 (id int)')->then (sub {
      return $client->query ('insert into foo2 (id) values (15), (31)');
    })->then (sub {
      return $client->query ('select * from foo2 order by id asc', sub {
        push @result, $i++;
        return AnyEvent::MySQL::Client::Promise->resolve->then (sub {
          push @result, $j++;
        });
      });
    });
  })->then (sub {
    test {
      is_deeply \@result, [10, 100, 11, 101];
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
} n => 1, name => 'query with with callback, return promise';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->query ("\x{500}");
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_failure;
      is $result->packet, undef;
      like $result->message, qr{utf8};
    } $c;
    return $client->query ('show tables');
  })->then (sub {
    my $x = $_[0];
    test {
      ok $x->is_success;
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
} n => 6, name => 'query utf8-flagged';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->query ('create table foo12 (name VARCHAR(3))');
  })->then (sub {
    return $client->query (qq{insert into foo12 (name) values ("\xE4\xBD\x8D")});
  })->then (sub {
    my $data;
    return $client->query ('select * from foo12', sub {
      $data = $_[0]->packet->{data}->[0];
    })->then (sub { return $data });
  })->then (sub {
    my $result = $_[0];
    test {
      is $result, "\xE4\xBD\x8D";
    } $c;
  })->catch (sub {
    warn $_[0];
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->then (sub {
    return $client->connect
        (hostname => 'unix/', port => $dsn{mysql_socket},
         character_set => 'utf8',
         username => $dsn{user}, password => $dsn{password},
         database => $dsn{dbname});
  })->then (sub {
    my $data;
    return $client->query ('select * from foo12', sub {
      $data = $_[0]->packet->{data}->[0];
    })->then (sub { return $data });
  })->then (sub {
    my $result = $_[0];
    test {
      is $result, encode 'utf-8', "\xE4\xBD\x8D";
    } $c;
  })->catch (sub {
    warn $_[0];
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->then (sub {
    return $client->connect
        (hostname => 'unix/', port => $dsn{mysql_socket},
         character_set => 'latin1',
         username => $dsn{user}, password => $dsn{password},
         database => $dsn{dbname});
  })->then (sub {
    my $data;
    return $client->query ('select * from foo12', sub {
      $data = $_[0]->packet->{data}->[0];
    })->then (sub { return $data });
  })->then (sub {
    my $result = $_[0];
    test {
      is $result, "\xE4\xBD\x8D";
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
} n => 3, name => 'query non-ascii';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       character_set => 'utf8',
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->query ('create table foo13 (name VARCHAR(3)) default charset utf8');
  })->then (sub {
    return $client->query (qq{insert into foo13 (name) values ("\xE4\xBD\x8D")});
  })->then (sub {
    my $data;
    return $client->query ('select * from foo13', sub {
      $data = $_[0]->packet->{data}->[0];
    })->then (sub { return $data });
  })->then (sub {
    my $result = $_[0];
    test {
      is $result, "\xE4\xBD\x8D";
    } $c;
  })->catch (sub {
    warn $_[0];
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->then (sub {
    return $client->connect
        (hostname => 'unix/', port => $dsn{mysql_socket},
         character_set => 'binary',
         username => $dsn{user}, password => $dsn{password},
         database => $dsn{dbname});
  })->then (sub {
    my $data;
    return $client->query ('select * from foo13', sub {
      $data = $_[0]->packet->{data}->[0];
    })->then (sub { return $data });
  })->then (sub {
    my $result = $_[0];
    test {
      is $result, "\xE4\xBD\x8D";
    } $c;
  })->catch (sub {
    warn $_[0];
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->then (sub {
    return $client->connect
        (hostname => 'unix/', port => $dsn{mysql_socket},
         character_set => 'latin1',
         username => $dsn{user}, password => $dsn{password},
         database => $dsn{dbname});
  })->then (sub {
    my $data;
    return $client->query ('select * from foo13', sub {
      $data = $_[0]->packet->{data}->[0];
    })->then (sub { return $data });
  })->then (sub {
    my $result = $_[0];
    test {
      is $result, "?";
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
} n => 3, name => 'query non-ascii';

run_tests;

=head1 LICENSE

Copyright 2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
