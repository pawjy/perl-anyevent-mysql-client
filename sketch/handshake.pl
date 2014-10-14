use strict;
use warnings;
use Data::Dumper;
use AnyEvent;
use AnyEvent::MySQL::Client;

my $cv = AE::cv;

my $hostname = shift or die;
my $port = 3306;

my $client = AnyEvent::MySQL::Client->new;

$client->connect (hostname => $hostname, port => $port)->then (sub {
  return $_[0]->handshake_packet || $_[0]->packet;
}, sub {
  return $_[0]->packet;
})->then (sub {
  warn Dumper $_[0];
})->then (sub {
  return $client->disconnect;
})->catch (sub {
  warn $_[0];
})->then (sub {
  $cv->send;
});

$cv->recv;
