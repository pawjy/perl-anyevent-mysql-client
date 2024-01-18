use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->child ('t_deps/lib');
use Tests;

my %dsn;
my $USER1 = 'foo';
my $PASS1 = 'bar';
my $USER2 = "\xFE\x80\x03a";
my $PASS2 = "\x66\x90\xAC\xFF";

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_success;
      isa_ok $result->packet, 'AnyEvent::MySQL::Client::ReceivedPacket';
      isa_ok $result->handshake_packet, 'AnyEvent::MySQL::Client::ReceivedPacket';
    } $c;
    return $client->query (q{create table foo (id int, unique key (id))}, sub {});
  })->then (sub {
    return $client->query (q{insert into foo (id) values (12)}, sub {});
  })->then (sub {
    return $client->disconnect;
  })->then (sub {
    return $client->connect
        (hostname => 'unix/', port => $dsn{mysql_socket},
         username => $dsn{user}, password => $dsn{password},
         database => $dsn{dbname});
  })->then (sub {
    my @row;
    return $client->query (q{select id from foo}, sub {
      push @row, $_[0];
    })->then (sub { return $row[0]->packet->{data}->[0] });
  })->then (sub {
    my $result = $_[0];
    test {
      is $result, 12;
    } $c;
  })->catch (sub {
    my $result = $_[0];
    test {
      ok 0;
      is $result, undef;
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
} n => 6, name => 'reconnect';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->disconnect->then (sub {
      my $y = $_[0];
      test {
        ok $y;
        isa_ok $y, 'AnyEvent::MySQL::Client::Result';
        ok $y->is_success;
      } $c;
    });
  })->catch (sub {
    test {
      ok 0;
    } $c;
    return undef;
  })->then (sub {
    my $result = $_[0];
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 3, name => 'disconnect';

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
        isa_ok $result, 'AnyEvent::MySQL::Client::Result';
        ok $result->is_exception;
        ok $result->message;
      } $c;
    });
  })->then (sub {
    return $client->ping;
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
} n => 4, name => 'connect while connecting';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password})->then (sub {
    my @row;
    return $client->query (q{show databases}, sub {
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

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      isa_ok $x, 'AnyEvent::MySQL::Client::Result';
      ok $x->is_exception;
      ok $x->message;
    } $c;
    return $client->disconnect->then (sub {
      my $y = $_[0];
      test {
        ok $y;
        isa_ok $y, 'AnyEvent::MySQL::Client::Result';
        ok $y->is_success;
      } $c;
    });
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 6, name => 'no host';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'dummy.localdomain', port => 1122224)->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      isa_ok $x, 'AnyEvent::MySQL::Client::Result';
      ok $x->is_exception;
      ok $x->message;
    } $c;
    return $client->disconnect->then (sub {
      my $y = $_[0];
      test {
        ok $y;
        isa_ok $y, 'AnyEvent::MySQL::Client::Result';
        ok $y->is_success;
      } $c;
    });
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 6, name => 'bad host';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => rand,
       database => $dsn{dbname})->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      isa_ok $x, 'AnyEvent::MySQL::Client::Result';
      ok $x->is_exception;
      isa_ok $x->packet, 'AnyEvent::MySQL::Client::ReceivedPacket';
      ok $x->message;
      is ''.$x, $x->message;
    } $c;
    return $client->disconnect->then (sub {
      my $y = $_[0];
      test {
        ok $y;
        isa_ok $y, 'AnyEvent::MySQL::Client::Result';
        ok $y->is_success;
      } $c;
    });
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 8, name => 'bad password';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->disconnect->then (sub {
    my $y = $_[0];
    test {
      ok $y;
      isa_ok $y, 'AnyEvent::MySQL::Client::Result';
      ok $y->is_success;
    } $c;
  })->catch (sub {
    test {
      ok 0;
    } $c;
    return undef;
  })->then (sub {
    my $result = $_[0];
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 3, name => 'disconnect not connected';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->ping->DIE;
  })->catch (sub {
    return $client->disconnect;
  })->then (sub {
    test {
      test {
        ok 1;
      } $c;
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 1, name => 'die while sending and then disconnect';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname},
       tls => {})->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      ok $x->is_exception;
      like $x->message, qr{TLS};
      ok $x->packet;
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
} n => 3, name => 'server does not support TLS';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => 'gagewagewaea', password => 'hogefugaaaaafegwat3g',
       database => $dsn{dbname})->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      ok $x->is_exception;
      is $x->packet->{error_code}, 1045;
      ok $x->message;
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
} n => 3, name => 'bad user';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $USER1, password => 'hogefugaaaaafegwat3g',
       database => $dsn{dbname})->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      ok $x->is_exception;
      is $x->packet->{error_code}, 1045;
      ok $x->message;
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
} n => 3, name => 'bad password';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password})->then (sub {
    my @row;
    return $client->query (q{select current_user()}, sub {
      push @row, $_[0];
    })->then (sub { return $row[0]->packet->{data} });
  })->then (sub {
    my $result = $_[0];
    test {
      is_deeply $result, [$dsn{user} . '@localhost'];
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
} n => 1, name => 'login user';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $USER1, password => $PASS1)->then (sub {
    my @row;
    return $client->query (q{select current_user()}, sub {
      push @row, $_[0];
    })->then (sub { return $row[0]->packet->{data} });
  })->then (sub {
    my $result = $_[0];
    test {
      is_deeply $result, [$USER1 . '@localhost'];
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
} n => 1, name => 'user with explicit password';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => "\x{100}", password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      ok $x->is_exception;
      like $x->message, qr{utf8};
      is $x->packet, undef;
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
} n => 3, name => 'user is utf8-flagged';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => "\x{5000}",
       database => $dsn{dbname})->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      ok $x->is_exception;
      like $x->message, qr{utf8};
      is $x->packet, undef;
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
} n => 3, name => 'password is utf8-flagged';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => "\x{5000}")->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      ok $x->is_exception;
      like $x->message, qr{utf8};
      is $x->packet, undef;
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
} n => 3, name => 'dbname is utf8-flagged';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       character_set => 'default',
       username => $USER2, password => $PASS2)->then (sub {
    my @row;
    return $client->query (q{select current_user()}, sub {
      push @row, $_[0];
    })->then (sub { return $row[0]->packet->{data} });
  })->then (sub {
    my $result = $_[0];
    test {
      is_deeply $result, [$USER2 . '@localhost'];
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
} n => 1, name => 'non-ascii username and password';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       character_set => 235,
       username => $USER2, password => $PASS2,
       database => $dsn{dbname})->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      ok $x->is_exception;
      is $x->packet->{error_code}, 1045;
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
} n => 2, name => 'username/password charset mismatch';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       character_set => 'utf-12345',
       username => $USER1, password => $PASS1,
       database => $dsn{dbname})->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      ok $x->is_exception;
      is $x->packet, undef;
      like $x->message, qr{char};
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
} n => 3, name => 'unknown charset';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => "ho\x00ha", password => $PASS1,
       database => $dsn{dbname})->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      ok $x->is_exception;
      is $x->packet, undef;
      like $x->message, qr{NULL};
    } $c;
  })->catch (sub {
    warn $_[0];
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
} n => 3, name => 'null user';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  my $connect_done;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    $connect_done++;
  }, sub {
    $connect_done++;
  });
  $client->disconnect->then (sub {
    my $x = $_[0];
    test {
      ok $x->is_success;
      is $connect_done, 1;
    } $c;
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
} n => 2, name => 'connect then disconnect soon';

RUN sub {
  my $dsn = test_dsn 'root';
  $dsn =~ s/^DBI:mysql://i;
  %dsn = map { split /=/, $_, 2 } split /;/, $dsn;

  my $client = AnyEvent::MySQL::Client->new;
  my %connect;
  if (defined $dsn{port}) {
    $connect{hostname} = $dsn{host};
    $connect{port} = $dsn{port};
  } else {
    $connect{hostname} = 'unix/';
    $connect{port} = $dsn{mysql_socket};
  }
  $client->connect (
    %connect,
    username => $dsn{user},
    password => $dsn{password},
    database => 'mysql',
    character_set => 'default',
  )->then (sub {
    return $client->query ('grant all privileges on *.* to "'.$USER1.'"@"localhost" identified by "'.$PASS1.'"');
  })->then (sub {
    return $client->query ("grant all privileges on *.* to '".$USER2."'\@'localhost' identified by '".$PASS2."'");
  })->then (sub {
    return $client->disconnect;
  })->to_cv->recv;
};

=head1 LICENSE

Copyright 2014-2018 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
