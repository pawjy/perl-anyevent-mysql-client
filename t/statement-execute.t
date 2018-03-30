use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->child ('t_deps/lib');
use Tests;

my %dsn;

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->statement_execute (124)->then (sub {
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
    return $client->statement_prepare ('create table foo0 (id int)');
  })->then (sub {
    return $client->statement_execute ($_[0]->packet->{statement_id})->die;
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
} n => 2, name => 'execute die';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->statement_execute;
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
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->query ('create table foo1 (id int)');
  })->then (sub {
    return $client->statement_prepare ('insert into foo1 (id) values (12)');
  })->then (sub {
    my $result = $_[0];
    my $statement_id = $result->packet->{statement_id};
    return $client->statement_execute ($statement_id);
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_success;
      ok $result->packet;
    } $c;
    my $data;
    return $client->query ('select * from foo1', sub {
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
      return $client->query ("create table foo$x (id int)");
    })->then (sub {
      return $client->statement_prepare ("insert into foo$x (id) values (?)");
    })->then (sub {
      my $result = $_[0];
      my $statement_id = $result->packet->{statement_id};
      return $client->statement_execute ($statement_id, [$value]);
    })->then (sub {
      my $result = $_[0];
      test {
        ok $result;
        isa_ok $result, 'AnyEvent::MySQL::Client::Result';
        ok $result->is_success;
        ok $result->packet;
      } $c;
      my @data;
      return $client->query ("select * from foo$x", sub {
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
    return $client->query ('create table foo6 (id int)');
  })->then (sub {
    return $client->statement_prepare ('insert into foo6 (id) values (12)');
  })->then (sub {
    my $result = $_[0];
    my $statement_id = $result->packet->{statement_id};
    return $client->statement_execute ($statement_id, [{type => 'LONG', value => 10}, {type => 'STRING', value => ''}]);
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_success;
      ok $result->packet;
    } $c;
    my $data;
    return $client->query ('select * from foo6', sub {
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
    return $client->query ('create table foo7 (id int)');
  })->then (sub {
    return $client->statement_prepare ('insert into foo7 (id) values (?), (?)');
  })->then (sub {
    my $result = $_[0];
    my $statement_id = $result->packet->{statement_id};
    return $client->statement_execute ($statement_id, []);
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
    return $client->query ('select * from foo7 order by id asc', sub {
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
      return $client->query ("create table foo$x (id int)");
    })->then (sub {
      return $client->statement_prepare ("insert into foo$x (id) values (?)");
    })->then (sub {
      my $result = $_[0];
      my $statement_id = $result->packet->{statement_id};
      return $client->statement_execute ($statement_id, [$value]);
    })->then (sub {
      my $result = $_[0];
      test {
        ok $result;
        isa_ok $result, 'AnyEvent::MySQL::Client::Result';
        ok $result->is_success;
        ok $result->packet;
      } $c;
      my @data;
      return $client->query ("select * from foo$x", sub {
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
      return $client->query ("create table bar$x (id int)");
    })->then (sub {
      return $client->statement_prepare ("insert into bar$x (id) values (?)");
    })->then (sub {
      my $result = $_[0];
      my $statement_id = $result->packet->{statement_id};
      return $client->statement_execute ($statement_id, []);
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
      return $client->query ("select * from bar$x order by id asc", sub {
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
    return $client->query ('create table foo21 (id int, name blob, data tinyint(10))');
  })->then (sub {
    return $client->statement_prepare ('insert into foo21 (name, id, data) values (?, ?, ?)');
  })->then (sub {
    my $result = $_[0];
    my $statement_id = $result->packet->{statement_id};
    return $client->statement_execute ($statement_id, [{type => 'STRING', value => undef}, {type => 'LONG', value => 10}, {type => 'BLOB', value => undef}]);
  })->then (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_success;
      ok $result->packet;
    } $c;
    my @data;
    return $client->query ('select * from foo21', sub {
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

for my $test (
  {id => 1, w => {type => 'STRING', value => "\x{400}"}, e => qr/utf8/},
  {id => 2, w => {type => 'unknown', value => 'hoge'}, e => qr/unknown/},
  {id => 3, w => {type => 'DATETIME', value => 'hoge'}, e => qr/syntax/},
  {id => 4, w => {type => 'TIME', value => 'hoge'}, e => qr/syntax/},
) {
  test {
    my $c = shift;
    my $client = AnyEvent::MySQL::Client->new;
    my $x = $test->{id} + 500;
    $client->connect
        (hostname => 'unix/', port => $dsn{mysql_socket},
         username => $dsn{user}, password => $dsn{password},
         database => $dsn{dbname})->then (sub {
      return $client->query ("create table foo$x (id blob)");
    })->then (sub {
      return $client->statement_prepare ("insert into foo$x (id) values (?)");
    })->then (sub {
      my $result = $_[0];
      my $statement_id = $result->packet->{statement_id};
      return $client->statement_execute ($statement_id, [$test->{w}]);
    })->then (sub {
      my $result = $_[0];
      test {
        ok $result;
        isa_ok $result, 'AnyEvent::MySQL::Client::Result';
        ok $result->is_failure;
        is $result->packet, undef;
        like $result->message, $test->{e};
      } $c;
      my @data;
      return $client->query ("select * from foo$x order by id asc", sub {
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
  } n => 6, name => ['bad parameter value', $test->{e}];
}

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->query ('create table foo25 (id int, name blob, data tinyint(10))');
  })->then (sub {
    return $client->query ('insert into foo25 (id,name) values (51, "ab"), (60, "cx")');
  })->then (sub {
    return $client->statement_prepare ('select * from foo25 order by id asc');
  })->then (sub {
    my $result = $_[0];
    my $statement_id = $result->packet->{statement_id};
    my @row;
    return $client->statement_execute ($statement_id, [], sub {
      push @row, $_[0];
    })->then (sub { [$_[0], \@row] });
  })->then (sub {
    my ($result, $rows) = @{$_[0]};
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_success;
      ok $result->packet;
      is ref $result->column_packets, 'ARRAY';
      is_deeply [map { $_->{name} } @{$result->column_packets}], ['id', 'name', 'data'];
      isa_ok $rows->[0]->packet, 'AnyEvent::MySQL::Client::ReceivedPacket';
      isa_ok $rows->[1]->packet, 'AnyEvent::MySQL::Client::ReceivedPacket';
      is_deeply [map { $_->packet->{data} } @$rows],
          [[{type => 'LONG', value => 51}, {type => 'BLOB', value => 'ab'}, {type => 'TINY', value => undef}],
           [{type => 'LONG', value => 60}, {type => 'BLOB', value => 'cx'}, {type => 'TINY', value => undef}]];
    } $c;
  })->then (sub {
    return $client->query ('show tables');
  })->catch (sub {
    warn $_[0];
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->catch (sub {
    warn $_[0];
    test {
      ok 0;
    } $c;
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 9, name => 'execute with columns, with on_row';

for my $test (
  {id => 1, type => 'tinyint', w => '-4', r => {type => 'TINY', value => -4}},
  {id => 2, type => 'decimal(4)', w => '-4', r => {type => 'NEWDECIMAL', value => '-4'}},
  {id => 3, type => 'decimal(4,2)', w => '-4.2', r => {type => 'NEWDECIMAL', value => '-4.20'}},
  {id => 4, type => 'smallint', w => '-4', r => {type => 'SHORT', value => -4}},
  {id => 5, type => 'int', w => '-4', r => {type => 'LONG', value => -4}},
  {id => 6, type => 'bigint', w => '-4', r => {type => 'LONGLONG', value => -4}},
  {id => 7, type => 'float', w => '-4.25', r => {type => 'FLOAT', value => -4.25}},
  {id => 8, type => 'double', w => '-4.5', r => {type => 'DOUBLE', value => -4.5}},
  {id => 9, type => 'varchar(12)', w => qq{"ab vca\xFE"}, r => {type => 'VAR_STRING', value => "ab vca\xFE"}},
  {id => 10, type => 'varbinary(12)', w => qq{"ab vca\xFE"}, r => {type => 'VAR_STRING', value => "ab vca\xFE"}},
  {id => 11, type => 'bit(8)', w => '0b10011010', r => {type => 'BIT', unsigned => 1, value => pack 'C', 0b10011010}},
  {id => 12, type => 'enum("abc","def")', w => '"abc"', r => {type => 'STRING', value => 'abc'}},
  {id => 13, type => 'set("abc","def")', w => '"abc"', r => {type => 'STRING', value => 'abc'}},
  {id => 14, type => 'tinyblob', w => '"abcde"', r => {type => 'BLOB', value => 'abcde'}},
  {id => 15, type => 'mediumblob', w => '"abcde"', r => {type => 'BLOB', value => 'abcde'}},
  {id => 16, type => 'blob', w => '"abcde'.("\x00" x 256).'x"', r => {type => 'BLOB', value => 'abcde'.("\x00" x 256).'x'}},
  {id => 17, type => 'longblob', w => '"abcde'.("\x00" x 70000).'x"', r => {type => 'BLOB', value => 'abcde'.("\x00" x 70000).'x'}},
  {id => 18, type => 'point', w => 'Point(1, 1)', r => {type => 'GEOMETRY', value => "\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF0\x3F\x00\x00\x00\x00\x00\x00\xF0\x3F"}},
  {id => 19, type => 'tinyint unsigned', w => '250', r => {type => 'TINY', value => 250, unsigned => 1}},
  {id => 20, type => 'int unsigned', w => 2**31, r => {type => 'LONG', value => 2**31, unsigned => 1}},
  {id => 21, type => 'datetime', w => '"0021-04-01 12:22:31"', r => {type => 'DATETIME', value => '0021-04-01 12:22:31'}},
  {id => 22, type => 'timestamp', w => '"2010-04-01 12:22:31"', r => {type => 'TIMESTAMP', value => '2010-04-01 12:22:31', unsigned => 1}},
  {id => 23, type => 'date', w => '"2010-04-01"', r => {type => 'DATE', value => '2010-04-01 00:00:00'}},
  {id => 24, type => 'datetime', w => '"0000-00-00 00:00:00"', r => {type => 'DATETIME', value => '0000-00-00 00:00:00'}},
  {id => 25, type => 'time', w => '"00:00:00"', r => {type => 'TIME', value => '00:00:00'}},
  {id => 26, type => 'time', w => '"812:02:44"', r => {type => 'TIME', value => '812:02:44'}},
  {id => 27, type => 'time', w => '"-812:02:01"', r => {type => 'TIME', value => '-812:02:01'}},
  {id => 28, type => 'time', w => '"-12:02:01"', r => {type => 'TIME', value => '-12:02:01'}},
) {
  test {
    my $c = shift;
    my $client = AnyEvent::MySQL::Client->new;
    my $x = $test->{id} + 300;
    $client->connect
        (hostname => 'unix/', port => $dsn{mysql_socket},
         username => $dsn{user}, password => $dsn{password},
         database => $dsn{dbname})->then (sub {
      return $client->query ("create table foo$x (value $test->{type})");
    })->then (sub {
      return $client->query ("insert into foo$x (value) values ($test->{w})");
    })->then (sub {
      return $client->statement_prepare ("select * from foo$x");
    })->then (sub {
      my $result = $_[0];
      my $statement_id = $result->packet->{statement_id};
      my @row;
      return $client->statement_execute ($statement_id, [], sub {
        push @row, $_[0];
      })->then (sub { [$_[0], \@row] });
    })->then (sub {
      my ($result, $rows) = @{$_[0]};
      test {
        is_deeply [map { $_->packet->{data} } @$rows], [[$test->{r}]];
      } $c;
    })->then (sub {
      return $client->query ('show tables');
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
  } n => 1, name => ['receive value', $test->{type}];
}

for my $test (
  {id => 1, type => 'datetime', w => {type => 'DATETIME', value => "0021-04-01 12:22:31"}, r => {type => 'DATETIME', value => '0021-04-01 12:22:31'}},
  {id => 2, type => 'timestamp', w => {type => 'TIMESTAMP', value => "2010-04-01 12:22:31"}, r => {type => 'TIMESTAMP', value => '2010-04-01 12:22:31', unsigned => 1}},
  {id => 3, type => 'date', w => {type => 'DATE', value => "2010-04-01"}, r => {type => 'DATE', value => '2010-04-01 00:00:00'}},
  {id => 4, type => 'datetime', w => {type => 'DATETIME', value => "0000-00-00 00:00:00"}, r => {type => 'DATETIME', value => '0000-00-00 00:00:00'}},
  {id => 5, type => 'datetime', w => {type => 'DATETIME', value => "8000-99-99 99:99:99"}, r => {type => 'DATETIME', value => '0000-00-00 00:00:00'}},
  {id => 25, type => 'time', w => {type => 'TIME', value => "00:00:00"}, r => {type => 'TIME', value => '00:00:00'}},
  {id => 26, type => 'time', w => {type => 'TIME', value => "812:02:44"}, r => {type => 'TIME', value => '812:02:44'}},
  {id => 27, type => 'time', w => {type => 'TIME', value => "-812:02:01"}, r => {type => 'TIME', value => '-812:02:01'}},
  {id => 28, type => 'time', w => {type => 'TIME', value => "-12:02:01"}, r => {type => 'TIME', value => '-12:02:01'}},
  {id => 29, type => 'time', w => {type => 'TIME', value => "-99:99:99"}, r => {type => 'TIME', value => '00:00:00'}},
) {
  test {
    my $c = shift;
    my $client = AnyEvent::MySQL::Client->new;
    my $x = $test->{id} + 400;
    $client->connect
        (hostname => 'unix/', port => $dsn{mysql_socket},
         username => $dsn{user}, password => $dsn{password},
         database => $dsn{dbname})->then (sub {
      return $client->query ("create table foo$x (value $test->{type})");
    })->then (sub {
      return $client->statement_prepare ("insert into foo$x (value) values (?)");
    })->then (sub {
      return $client->statement_execute
          ($_[0]->packet->{statement_id}, [$test->{w}]);
    })->then (sub {
      my $y = $_[0];
      test {
        ok $y->is_success;
      } $c;
      return $client->statement_prepare ("select * from foo$x");
    })->then (sub {
      my $result = $_[0];
      my $statement_id = $result->packet->{statement_id};
      my @row;
      return $client->statement_execute ($statement_id, [], sub {
        push @row, $_[0];
      })->then (sub { [$_[0], \@row] });
    })->then (sub {
      my ($result, $rows) = @{$_[0]};
      test {
        is_deeply [map { $_->packet->{data} } @$rows], [[$test->{r}]];
      } $c;
    })->then (sub {
      return $client->query ('show tables');
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
  } n => 2, name => ['send/receive value', $test->{type}];
}

RUN sub {
  my $dsn = test_dsn 'hoge';
  $dsn =~ s/^DBI:mysql://i;
  %dsn = map { split /=/, $_, 2 } split /;/, $dsn;
};

=head1 LICENSE

Copyright 2014-2018 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
