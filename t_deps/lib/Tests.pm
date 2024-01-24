package Tests;
use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t_deps/modules/*/lib');
use Carp;
use AbortController;
use Test::More;
use Test::X1;
use AnyEvent::MySQL::Client;
use Web::Encoding;
use ServerSet;

use AMCSS;

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

our $ServerData;

push @EXPORT, qw(test_dsn);
sub test_dsn ($;%) {
  my $name = shift;
  my %args = @_;

  my $dsn = {%{$ServerData->{local_dsn_options}->{root}}};
  my $test_dsn = $ServerData->{local_dsn_options}->{test};
  if ($name eq 'root') {
    $test_dsn = $dsn;
  }
  
  my $client = AnyEvent::MySQL::Client->new;
  my %connect;
  if (defined $dsn->{port}) {
    $connect{hostname} = $dsn->{host}->to_ascii;
    $connect{port} = $dsn->{port};
  } else {
    $connect{hostname} = 'unix/';
    $connect{port} = $dsn->{mysql_socket};
  }
  $client->connect (
    %connect,
    username => $dsn->{user},
    password => $dsn->{password},
    database => $dsn->{dbname},
  )->then (sub {
    my $escaped = $dsn->{dbname} = $name . '_test';
    $escaped =~ s/`/``/g;
    return $client->query ("CREATE DATABASE IF NOT EXISTS `$escaped`")->then (sub {
      die $_[0] unless $_[0]->is_success;
      return $client->query (
        encode_web_utf8 sprintf q{grant all on `%s`.* to '%s'@'%s'},
        $escaped, $test_dsn->{user}, '%',
      );
    })->then (sub {
      die $_[0] unless $_[0]->is_success;
    });
  })->finally (sub {
    return $client->disconnect;
  })->to_cv->recv;

  if ($args{unix} or not $args{tcp}) {
    my $dsn = {%$test_dsn,
               dbname => $dsn->{dbname}};
    delete $dsn->{port};
    delete $dsn->{host};
    my $dsns = ServerSet->dsn ('mysql', $dsn);
    return $dsns;
  } else {
    my $dsn = {%$test_dsn,
               dbname => $dsn->{dbname}};
    delete $dsn->{mysql_socket};
    my $dsns = ServerSet->dsn ('mysql', $dsn);
    return $dsns;
  }
} # test_dsn

push @EXPORT, qw(create_user);
sub create_user ($$$;%) {
  my ($client, $user, $password, %args) = @_;
  if ($ServerData->{mysql_version} =~ /^mysql8?$/) {
    my $sql = 'create user "'.$user.'"@"%" identified';
    $sql .= ' with "mysql_native_password"' if $args{native_password};
    $sql .= ' by "'.$password.'"';
    $sql .= ' require subject "'.$args{tls_subject}.'"' if defined $args{tls_subject};
    return $client->query ($sql)->then (sub {
      die $_[0] unless $_[0]->is_success;
      return $client->query ('grant all privileges on *.* to "'.$user.'"@"%"');
    })->then (sub {
      die $_[0] unless $_[0]->is_success;
    });
  } else {
    my $sql = 'create user "'.$user.'"@"%" identified';
    $sql .= ' by "'.$password.'"';
    $sql .= ' require subject "'.$args{tls_subject}.'"' if defined $args{tls_subject};
    return $client->query ($sql)->then (sub {
      die $_[0] unless $_[0]->is_success;
      my $sql = 'grant all privileges on *.* to "'.$user.'"@"%" identified';
      $sql .= ' by "'.$password.'"';
      $sql .= ' require subject "'.$args{tls_subject}.'"' if defined $args{tls_subject};
      return $client->query ($sql);
    })->then (sub {
      die $_[0] unless $_[0]->is_success;
    });
  }
} # create_user

push @EXPORT, qw(RUN);
sub RUN (;$$) {
  my $init = shift || sub { };
  my $args = shift || {};
  $args->{mysql_version} //= $ENV{TEST_MYSQL_VERSION};

  note "Servers...";
  my $ac = AbortController->new;
  my $v = AMCSS->run (
    %$args,
    signal => $ac->signal,
  )->to_cv->recv;
  local $ServerData = $v->{data};
  
  eval {
    $init->();

    note "Tests...";
    run_tests;
  };
  my $error;
  if ($@) {
    note "Failed";
    warn $@;
    $error = 1;
  }
  
  note "Done";
  $ac->abort;
  $v->{done}->to_cv->recv;
  exit 1 if $error;
} # RUN

1;

=head1 LICENSE

Copyright 2018-2024 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
