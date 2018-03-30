use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->child ('t_deps/lib');
use Tests;

my %dsn;

test {
  my $c = shift;

  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->query (q{create table foo (id int, unique key (id))}, sub {});
  })->then (sub {
    return $client->query (q{begin}, sub {});
  })->then (sub {
    return $client->query (q{insert into foo (id) values (12)}, sub {});
  })->then (sub {
    return $client->query (q{commit}, sub {});
  })->then (sub {
    my @row;
    return $client->query (q{select id from foo}, sub {
      push @row, $_[0];
    })->then (sub {
      test {
        is 0+@row, 1;
        is $row[0]->packet->{data}->[0], 12;
      } $c;
    });
  })->catch (sub {
    my $e = $_[0];
    test {
      ok 0, $e;
    } $c;
  })->then (sub {
    return $client->quit;
  })->then (sub {
    return $client->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 2, name => 'begin ... commit';

test {
  my $c = shift;

  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->query (q{create table foo2 (id int, unique key (id))}, sub {});
  })->then (sub {
    return $client->query (q{begin}, sub {});
  })->then (sub {
    return $client->query (q{insert into foo2 (id) values (12)}, sub {});
  })->then (sub {
    return $client->query (q{commit}, sub {});
  })->then (sub {
    return $client->query (q{begin}, sub {});
  })->then (sub {
    return $client->query (q{insert into foo2 (id) values (32)}, sub {});
  })->then (sub {
    return $client->query (q{rollback}, sub {});
  })->then (sub {
    my @row;
    return $client->query (q{select id from foo2}, sub {
      push @row, $_[0];
    })->then (sub {
      test {
        is 0+@row, 1;
        is $row[0]->packet->{data}->[0], 12;
      } $c;
    });
  })->catch (sub {
    my $e = $_[0];
    test {
      ok 0, $e;
    } $c;
  })->then (sub {
    return $client->quit;
  })->then (sub {
    return $client->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 2, name => 'begin ... rollback';

test {
  my $c = shift;

  my $client = AnyEvent::MySQL::Client->new;
  $client->connect
      (hostname => 'unix/', port => $dsn{mysql_socket},
       username => $dsn{user}, password => $dsn{password},
       database => $dsn{dbname})->then (sub {
    return $client->query (q{create table foo3 (id int, unique key (id))}, sub {});
  })->then (sub {
    return $client->query (q{begin}, sub {});
  })->then (sub {
    return $client->statement_prepare ("insert into foo3 (id) values (12)");
  })->then (sub {
    return $client->statement_execute
        ($_[0]->packet->{statement_id}, []);
  })->then (sub {
    return $client->query (q{commit}, sub {});
  })->then (sub {
    return $client->query (q{begin}, sub {});
  })->then (sub {
    return $client->statement_prepare ("insert into foo3 (id) values (32)");
  })->then (sub {
    return $client->statement_execute
        ($_[0]->packet->{statement_id}, []);
  })->then (sub {
    return $client->query (q{rollback}, sub {});
  })->then (sub {
    my @row;
    return $client->query (q{select id from foo3}, sub {
      push @row, $_[0];
    })->then (sub {
      test {
        is 0+@row, 1;
        is $row[0]->packet->{data}->[0], 12;
      } $c;
    });
  })->catch (sub {
    my $e = $_[0];
    test {
      ok 0, $e;
    } $c;
  })->then (sub {
    return $client->quit;
  })->then (sub {
    return $client->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 2, name => 'begin ... rollback (prepared)';

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
