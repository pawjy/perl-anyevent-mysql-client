use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->child ('t_deps/lib');
use Tests;
use Test::Certificates;

Promise->resolve->then (sub {
  return Test::Certificates->wait_create_cert_p ({host => 'server', intermediate => ($ENV{TEST_MYSQL_VERSION} // '') eq 'mysql5.6'});
})->then (sub {
  return Test::Certificates->wait_create_cert_p ({host => 'client1'});
})->then (sub {
  return Test::Certificates->wait_create_cert_p ({host => 'client2'});
})->to_cv->recv;

my %dsn;
my $SSL_USER = 'foo';
my $SSL_PASS = 'bar';
my $USER2 = (substr rand, 0, 15);;
my $PASS2 = '';
my $USER4 = (substr rand, 0, 15);;
my $PASS4 = (substr rand, 0, 15);;
my $USER5 = (substr rand, 0, 15);;
my $PASS5 = '';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    my $x = $_[0];
    test {
      ok $x->is_success;
      ok $x->packet;
    } $c;
  }, sub {
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->catch (sub {
    warn $_[0];
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} n => 2, name => 'non-ssl user';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $SSL_USER, password => $SSL_PASS,
       database => $dsn{dbname})->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      ok $x->is_exception;
      ok $x->packet;
      is $x->packet->{error_code}, 1045; # access denied
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->catch (sub {
    warn $_[0];
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} timeout => 120, n => 3, name => 'ssl-only user, no ssl';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname},
       tls => {verify => 0})->then (sub {
    my $x = $_[0];
    test {
      ok $x->is_success;
      ok $x->packet;
    } $c;
    my $data;
    return $client->query ('SHOW STATUS LIKE "Ssl_cipher"', sub {
      $data = $_[0]->packet->{data};
    })->then (sub {
      my $r = $_[0];
      test {
        ok $r->is_success, $r;
        my @col = map { $_->{name} } @{$r->column_packets};
        my %row;
        for (0..$#col) {
          $row{$col[$_]} = $data->[$_];
        }
        ok $row{Value};
      } $c;
    });
  }, sub {
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->catch (sub {
    warn $_[0];
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} timeout => 120, n => 4, name => 'with optional ssl';

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
      ok $x->message;
      is $x->packet, undef;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->catch (sub {
    warn $_[0];
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} timeout => 120, n => 3, name => 'with optional ssl, verification error';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $SSL_USER, password => $SSL_PASS,
       database => $dsn{dbname},
       tls => {ca_file => Test::Certificates->ca_path ('cert.pem')->stringify,
               key_file => Test::Certificates->cert_path ('key.pem', {host => 'client1'})->stringify,
               cert_file => Test::Certificates->cert_path ('cert-chained.pem', {host => 'client1'})->stringify,
             })->then (sub {
    my $x = $_[0];
    test {
      ok $x->is_success;
      ok $x->packet;
    } $c;
    my $data;
    return $client->query ('SHOW STATUS LIKE "Ssl_cipher"', sub {
      $data = $_[0]->packet->{data};
    })->then (sub {
      my @col = map { $_->{name} } @{$_[0]->column_packets};
      my %row;
      for (0..$#col) {
        $row{$col[$_]} = $data->[$_];
      }
      test {
        ok $row{Value};
      } $c;
    });
  }, sub {
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->catch (sub {
    warn $_[0];
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} timeout => 120, n => 3, name => 'with ssl client auth';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $USER2, password => $PASS2,
       database => $dsn{dbname},
       tls => {ca_file => Test::Certificates->ca_path ('cert.pem')->stringify,
               key_file => Test::Certificates->cert_path ('key.pem', {host => 'client1'})->stringify,
               cert_file => Test::Certificates->cert_path ('cert-chained.pem', {host => 'client1'})->stringify,
             })->then (sub {
    my $x = $_[0];
    test {
      ok $x->is_success;
      ok $x->packet;
    } $c;
    my $data;
    return $client->query ('SHOW STATUS LIKE "Ssl_cipher"', sub {
      $data = $_[0]->packet->{data};
    })->then (sub {
      my @col = map { $_->{name} } @{$_[0]->column_packets};
      my %row;
      for (0..$#col) {
        $row{$col[$_]} = $data->[$_];
      }
      test {
        ok $row{Value};
      } $c;
    });
  }, sub {
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->catch (sub {
    warn $_[0];
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} timeout => 120, n => 3, name => 'empty password';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $USER5, password => $PASS5,
       database => $dsn{dbname},
       tls => {ca_file => Test::Certificates->ca_path ('cert.pem')->stringify,
               key_file => Test::Certificates->cert_path ('key.pem', {host => 'client1'})->stringify,
               cert_file => Test::Certificates->cert_path ('cert-chained.pem', {host => 'client1'})->stringify,
             })->then (sub {
    my $x = $_[0];
    test {
      ok $x->is_success;
      ok $x->packet;
    } $c;
    my $data;
    return $client->query ('SHOW STATUS LIKE "Ssl_cipher"', sub {
      $data = $_[0]->packet->{data};
    })->then (sub {
      my @col = map { $_->{name} } @{$_[0]->column_packets};
      my %row;
      for (0..$#col) {
        $row{$col[$_]} = $data->[$_];
      }
      test {
        ok $row{Value};
      } $c;
    });
  }, sub {
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->catch (sub {
    warn $_[0];
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} timeout => 120, n => 3, name => 'empty native password';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $USER4, password => $PASS4,
       database => $dsn{dbname},
       tls => {ca_file => Test::Certificates->ca_path ('cert.pem')->stringify,
               key_file => Test::Certificates->cert_path ('key.pem', {host => 'client1'})->stringify,
               cert_file => Test::Certificates->cert_path ('cert-chained.pem', {host => 'client1'})->stringify,
             })->then (sub {
    my $x = $_[0];
    test {
      ok $x->is_success;
      ok $x->packet;
    } $c;
    my $data;
    return $client->query ('SHOW STATUS LIKE "Ssl_cipher"', sub {
      $data = $_[0]->packet->{data};
    })->then (sub {
      my @col = map { $_->{name} } @{$_[0]->column_packets};
      my %row;
      for (0..$#col) {
        $row{$col[$_]} = $data->[$_];
      }
      test {
        ok $row{Value};
      } $c;
    });
  }, sub {
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->catch (sub {
    warn $_[0];
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} timeout => 120, n => 3, name => 'native password';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $SSL_USER, password => $SSL_PASS,
       database => $dsn{dbname},
       tls => {verify => 0,
               #ca_file => Test::Certificates->ca_path ('cert.pem')->stringify,
               key_file => Test::Certificates->cert_path ('key.pem', {host => 'client1'})->stringify,
               cert_file => Test::Certificates->cert_path ('cert-chained.pem', {host => 'client1'})->stringify,
             })->then (sub {
    my $x = $_[0];
    test {
      ok $x->is_success;
      ok $x->packet;
    } $c;
    my $data;
    return $client->query ('SHOW STATUS LIKE "Ssl_cipher"', sub {
      $data = $_[0]->packet->{data};
    })->then (sub {
      my @col = map { $_->{name} } @{$_[0]->column_packets};
      my %row;
      for (0..$#col) {
        $row{$col[$_]} = $data->[$_];
      }
      test {
        ok $row{Value};
      } $c;
    });
  }, sub {
    test {
      ok 0;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->catch (sub {
    warn $_[0];
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} timeout => 120, n => 3, name => 'with ssl client auth, no verification';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $SSL_USER, password => $SSL_PASS,
       database => $dsn{dbname},
       tls => {#ca_file => Test::Certificates->ca_path ('cert.pem')->stringify,
               key_file => Test::Certificates->cert_path ('key.pem', {host => 'client1'})->stringify,
               cert_file => Test::Certificates->cert_path ('cert-chained.pem', {host => 'client1'})->stringify,
             })->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      ok $x->is_exception;
      ok $x->message;
      is $x->packet, undef;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->catch (sub {
    warn $_[0];
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} timeout => 120, n => 3, name => 'with ssl client auth, verification error';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $SSL_USER, password => $SSL_PASS,
       database => $dsn{dbname},
       tls => {ca_file => Test::Certificates->ca_path ('cert.pem')->stringify,
               key_file => Test::Certificates->cert_path ('key.pem', {host => 'client2'})->stringify,
               cert_file => Test::Certificates->cert_path ('cert-chained.pem', {host => 'client2'})->stringify,
             })->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my $x = $_[0];
    test {
      ok $x->is_exception;
      ok $x->packet;
      is $x->packet->{error_code}, 1045;
    } $c;
  })->then (sub {
    return $client->disconnect;
  })->catch (sub {
    warn $_[0];
  })->then (sub {
    test {
      done $c;
      undef $c;
      undef $client;
    } $c;
  });
} timeout => 120, n => 3, name => 'with ssl client auth, wrong cert';

RUN sub {
  my $dsn = test_dsn 'hoge';
  $dsn =~ s/^DBI:mysql://i;
  %dsn = map { split /=/, $_, 2 } split /;/, $dsn;

  {
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
      return create_user $client, $SSL_USER, $SSL_PASS, tls_subject => "/CN=client1";
    })->then (sub {
      return create_user $client, $USER2, $PASS2, tls_subject => "/CN=client1";
    })->then (sub {
      return create_user $client, $USER4, $PASS4, tls_subject => "/CN=client1", native_password => 1;
    })->then (sub {
      return create_user $client, $USER5, $PASS5, tls_subject => "/CN=client1", native_password => 1;
    })->then (sub {
      return $client->disconnect;
    })->to_cv->recv;
  }
}, {
  mycnf => {
    'ssl-ca' => Test::Certificates->ca_path ('cert.pem')->stringify,
    'ssl-cert' => Test::Certificates->cert_path ('cert-chained.pem', {host => 'server'})->stringify,
    'ssl-key' => Test::Certificates->cert_path ('key.pem', {host => 'server'})->stringify,
  },
  path => Test::Certificates->cert_path ('')->parent,
};

=head1 LICENSE

Copyright 2014-2024 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
