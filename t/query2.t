use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->child ('t_deps/lib');
use ConnectedTests;

CTest {
  my ($c, $client) = @{$_[0]};
  return $client->query ('create table foo (id int)')->then (sub {
    return $client->query ('insert into foo (id) values (15)');
  })->then (sub {
    return $client->query ('select * from foo');
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
  });
} n => 6, name => 'query with no args, success';

CTest {
  my ($c, $client) = @{$_[0]};
  return $client->query ('create table')->then (sub {
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
  });
} n => 7, name => 'query with no args, success';

CTest {
  my ($c, $client) = @{$_[0]};
  my @row;
  return $client->query ('create table foox (id int)')->then (sub {
    return $client->query ('insert into foox (id) values (15), (31)');
  })->then (sub {
    return $client->query ('select * from foox order by id asc', sub {
      push @row, $_[0];
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
  });
} n => 16, name => 'query with with callback, success';

CTest {
  my ($c, $client) = @{$_[0]};
  my @result;
  my $i = 10;
  my $j = 100;
  return $client->query ('create table foo2 (id int)')->then (sub {
    return $client->query ('insert into foo2 (id) values (15), (31)');
  })->then (sub {
    return $client->query ('select * from foo2 order by id asc', sub {
      push @result, $i++;
      return AnyEvent::MySQL::Client::Promise->resolve->then (sub {
        push @result, $j++;
      });
    });
  })->then (sub {
    test {
      is_deeply \@result, [10, 100, 11, 101];
    } $c;
  });
} n => 1, name => 'query with with callback, return promise';

CTest {
  my ($c, $client) = @{$_[0]};
  return $client->query ("\x{500}")->catch (sub {
    my $result = $_[0];
    test {
      ok $result;
      isa_ok $result, 'AnyEvent::MySQL::Client::Result';
      ok $result->is_exception;
      is $result->packet, undef;
      like $result->message, qr{utf8};
    } $c;
    return $client->query ('show tables');
  })->then (sub {
    my $x = $_[0];
    test {
      ok $x->is_success;
    } $c;
  });
} n => 6, name => 'query utf8-flagged';

RUN;

=head1 LICENSE

Copyright 2014-2020 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
