package ConnectedTests;
use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t_deps/modules/*/lib');
use Carp;
use File::Temp qw(tempdir);
use Promise;
use Promised::Flow;
use Promised::Command::Docker;
use Promised::File;
use Web::Transport::FindPort;
use Test::More;
use Test::X1;
use AnyEvent::MySQL::Client;
use Web::Encoding;

our @EXPORT = (grep { not /^\$/ } @Test::More::EXPORT, @Test::X1::EXPORT,
               @Web::Encoding::EXPORT);

sub import ($;@) {
  my $from_class = shift;
  my ($to_class, $file, $line) = caller;
  no strict 'refs';
  for (@_ ? @_ : @{$from_class . '::EXPORT'}) {
    my $code = $from_class->can ($_)
        or croak qq{"$_" is not exported by the $from_class module at $file line $line};
    *{$to_class . '::' . $_} = $code;
  }
} # import

if ($ENV{TEST_SQL_DEBUG}) {
  eval q{ use AnyEvent::MySQL::Client::ShowLog };
  die $@ if $@;
}

my $DBNumber = 1;
my $ServerInfo;

my $GetTestDB = {};
sub test_db ($) {
  my $name = shift;
  $name .= '_' . $DBNumber . '_test';
  my $client = AnyEvent::MySQL::Client->new;
  return $GetTestDB->{$name} ||= Promise->resolve->then (sub {
    return promised_wait_until {
      return $client->connect (
        hostname => $ServerInfo->{host},
        port => $ServerInfo->{port},
        username => 'root',
        password => $ServerInfo->{root_password},
        database => 'mysql',
      )->then (sub {
        return 1;
      })->catch (sub {
        return $client->disconnect->catch (sub { })->then (sub { 0 });
      });
    } timeout => 30;
  })->then (sub {
    return $client->query (
      sprintf q{create user '%s'@'%s' identified by '%s'},
          $ServerInfo->{user}, '%', $ServerInfo->{password},
    );
  })->then (sub {
    return $client->query ('create database if not exists ' . $name);
  })->then (sub {
    return $client->query (
      encode_web_utf8 sprintf q{grant all on %s.* to '%s'@'%s'},
          $name, $ServerInfo->{user}, '%',
    );
  })->then (sub {
    my $dsn = {
      #user => $ServerInfo->{user},
      #password => $ServerInfo->{password},
      user => 'root',
      password => $ServerInfo->{root_password},
      host => $ServerInfo->{host},
      port => $ServerInfo->{port},
      #host => 'unix/',
      #port => $ServerInfo->{socket},
      dbname => $name,
    };
    #return 'dbi:mysql:' . join ';', map { $_ . '=' . $dsn->{$_} } keys %$dsn;
    return $name;
  })->finally (sub {
    return $client->disconnect;
  });
} # test_db

#my $MySQLImage = 'mysql/mysql-server';
my $MySQLImage = 'mariadb';
sub start_server () {
  my $tempdir = tempdir (CLEANUP => 1);
  my $temp_path = path ($tempdir);

  my $port = find_listenable_port;
  my $envs = {
    MYSQL_USER => 'username',
    MYSQL_PASSWORD => 'password',
    MYSQL_ROOT_PASSWORD => 'rootpassword',
    #MYSQL_ROOT_HOST => $handler->dockerhost->to_ascii,
    MYSQL_ROOT_HOST => '%',
    #MYSQL_DATABASE => $dbname[0] . $data->{_dbname_suffix},
    MYSQL_LOG_CONSOLE => 1,
  };
  $ServerInfo = {
    user => 'username',
    password => 'password',
    root_password => 'rootpassword',
    host => '0',
    port => $port,
    socket => $temp_path->child ('mysql.sock')->absolute,
  };
  my $my_cnf = join "\n", '[mysqld]',
      'user=mysql',
      #'user=' . $<,
            'default_authentication_plugin=mysql_native_password', # XXX
            #'skip-networking',
            'bind-address=0.0.0.0',
            'port=3306',
            'innodb_lock_wait_timeout=2',
            'max_connections=1000',
            #'sql_mode=', # old default
            #'sql_mode=NO_ENGINE_SUBSTITUTION,STRICT_TRANS_TABLES', # 5.6 default
            'socket=/tmp/mysql.sock',
            ;
  my $docker = Promised::Command::Docker->new (
    image => $MySQLImage,
    docker_run_options => [
      (map { ('-v', $_) }
        $temp_path->child ('my.cnf')->absolute . ':/etc/my.cnf',
        #$temp_path->child ('data')->absolute . ':/var/lib/mysql',
        $temp_path->child ('mysql.sock')->absolute . ':/tmp/mysql.sock',
      ()),
      '-p', $port . ':3306',
      (map { ('-e', $_ . '=' . $envs->{$_}) } keys %$envs),
      #'--name' => $container_name,
    ],
  );
  $docker->propagate_signal (1);
  $docker->signal_before_destruction ('TERM');
  #$docker->logs ($logs);
  return Promised::File->new_from_path ($temp_path->child ('data'))->mkpath->then (sub {
    return Promised::File->new_from_path ($temp_path->child ('my.cnf'))->write_byte_string ($my_cnf);
  })->then (sub {
    return $docker->start;
  })->then (sub {
    return {
      port => $port,
      stop => sub { return $docker->stop->finally (sub { undef $tempdir }) },
    };
  });
} # start_server

push @EXPORT, qw(RUN);
sub RUN () {
  note "Start server...";
  my $server = start_server->to_cv->recv;
  note "Server started";

  run_tests;

  note "Stop server...";
  $server->{stop}->()->to_cv->recv;
  note "Server stopped";

  undef $GetTestDB;
} # RUN

push @EXPORT, qw(CTest);
sub CTest (&%) {
  my ($code, %args) = @_;
  test {
    my $c = shift;
    my $client = AnyEvent::MySQL::Client->new;
    return test_db ($args{db} // '')->then (sub {
      my $name = shift;
      return $client->connect
        (hostname => $ServerInfo->{host},
         port => $ServerInfo->{port},
         username => $ServerInfo->{user},
         password => $ServerInfo->{password},
         database => $name);
    })->then (sub {
      return Promise->resolve ([$c, $client]);
    })->then ($code)->then (sub {
      done $c;
      undef $c;
    }, sub {
      my $e = $_[0];
      test {
        ok 0, "No exception: $e";
      } $c;
      done $c;
      undef $c;
    })->finally (sub {
      return $client->disconnect;
    });
  } %args;
} # CTest

1;

=head1 LICENSE

Copyright 2018-2020 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
