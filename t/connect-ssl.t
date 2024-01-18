use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->child ('t_deps/lib');
use Tests;

my $ReuseCerts = $ENV{REUSE_CERTS};
my $generate_certs_path = path (__FILE__)->parent->parent
    ->child ('t_deps/modules/rdb-utils/bin/generate-certs-for-tests.pl');
my $temp_dir = File::Temp->newdir;
my $certs_path = path ($temp_dir->dirname);
my $wait_until_time = time;
if ($ReuseCerts) {
  $certs_path = path (__FILE__)->parent->parent->child ('local/test-certs');
  unless (-f $certs_path->child ('client2-cert.pem') and
          $certs_path->child ('client2-cert.pem')->stat->[9] < time + 24*3600) {
    system ('perl', $generate_certs_path, $certs_path, 'server', 'client1', 'client2') == 0 or die $?;
    my $cert_time = time;
    $wait_until_time = $cert_time + 60 - [gmtime $cert_time]->[0];
  }
} else {
  system ('perl', $generate_certs_path, $certs_path, 'server', 'client1', 'client2') == 0 or die $?;
  my $cert_time = time;
  $wait_until_time = $cert_time + 60 - [gmtime $cert_time]->[0];
  chmod 0777, $certs_path;
}
$certs_path = $certs_path->absolute;

my %MyCnfArgs = (
  'ssl-ca' => $certs_path->child ('ca-cert.pem')->stringify,
  'ssl-cert' => $certs_path->child ('server-cert.pem')->stringify,
  'ssl-key' => $certs_path->child ('server-key-pkcs1.pem')->stringify,
);

my %dsn;
my $SSL_USER = 'foo';
my $SSL_PASS = 'bar';

my $cert_cv = AE::cv;
warn sprintf "Wait %s seconds...\n", $wait_until_time - time;
my $timer; $timer = AE::timer $wait_until_time - time, 0, sub {
  #warn "mysql -S $dsn{mysql_socket} -ufoo -pbar --ssl-ca=@{[$certs_path->child ('ca-cert.pem')]} --ssl-cert=@{[$certs_path->child ('client1-cert.pem')]} --ssl-key=@{[$certs_path->child ('client1-key-pkcs1.pem')]}";
  #warn $dsn;
  undef $timer;
  $cert_cv->send;
};

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
} wait => $cert_cv, timeout => 120, n => 3, name => 'ssl-only user, no ssl';

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
} wait => $cert_cv, timeout => 120, n => 3, name => 'with optional ssl';

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
} wait => $cert_cv, timeout => 120, n => 3, name => 'with optional ssl, verification error';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $SSL_USER, password => $SSL_PASS,
       database => $dsn{dbname},
       tls => {ca_file => $certs_path->child ('ca-cert.pem')->stringify,
               key_file => $certs_path->child ('client1-key.pem')->stringify,
               cert_file => $certs_path->child ('client1-cert.pem')->stringify})->then (sub {
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
} wait => $cert_cv, timeout => 120, n => 3, name => 'with ssl client auth';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $SSL_USER, password => $SSL_PASS,
       database => $dsn{dbname},
       tls => {verify => 0,
               #ca_file => $certs_path->child ('ca-cert.pem')->stringify,
               key_file => $certs_path->child ('client1-key.pem')->stringify,
               cert_file => $certs_path->child ('client1-cert.pem')->stringify})->then (sub {
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
} wait => $cert_cv, timeout => 120, n => 3, name => 'with ssl client auth, no verification';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $SSL_USER, password => $SSL_PASS,
       database => $dsn{dbname},
       tls => {#ca_file => $certs_path->child ('ca-cert.pem')->stringify,
               key_file => $certs_path->child ('client1-key.pem')->stringify,
               cert_file => $certs_path->child ('client1-cert.pem')->stringify})->then (sub {
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
} wait => $cert_cv, timeout => 120, n => 3, name => 'with ssl client auth, verification error';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $SSL_USER, password => $SSL_PASS,
       database => $dsn{dbname},
       tls => {ca_file => $certs_path->child ('ca-cert.pem')->stringify,
               key_file => $certs_path->child ('client2-key.pem')->stringify,
               cert_file => $certs_path->child ('client2-cert.pem')->stringify})->then (sub {
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
} wait => $cert_cv, timeout => 120, n => 3, name => 'with ssl client auth, wrong cert';

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
    return $client->query ('grant all privileges on *.* to "'.$SSL_USER.'"@"localhost" identified by "'.$SSL_PASS.'" require subject "/CN=client1.test"');
  })->then (sub {
    return $client->disconnect;
  })->to_cv->recv;
  }
}, {
  path => $certs_path,
  mycnf => \%MyCnfArgs,
};

=head1 LICENSE

Copyright 2014-2024 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
