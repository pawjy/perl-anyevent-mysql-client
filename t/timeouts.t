use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->child ('lib');
use lib glob path (__FILE__)->parent->parent->child ('t_deps/modules/*/lib');
use AnyEvent::MySQL::Client;
use Test::More;
use Test::X1;

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  ok $client->connection_packet_timeout;
  $client->connection_packet_timeout (13.4);
  is $client->connection_packet_timeout, 13.4;
  $client->connection_packet_timeout (0);
  is $client->connection_packet_timeout, 0;
  $client->connection_packet_timeout (undef);
  ok $client->connection_packet_timeout;
  done $c;
} n => 4, name => 'connection_packet_timeout';

test {
  my $c = shift;
  my $client = AnyEvent::MySQL::Client->new;
  ok $client->query_packet_timeout;
  $client->query_packet_timeout (13.4);
  is $client->query_packet_timeout, 13.4;
  $client->query_packet_timeout (0);
  is $client->query_packet_timeout, 0;
  $client->query_packet_timeout (undef);
  ok $client->query_packet_timeout;
  done $c;
} n => 4, name => 'query_packet_timeout';

run_tests;

=head1 LICENSE

Copyright 2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
