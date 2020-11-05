package Web::Transport::FindPort;
use strict;
use warnings;
our $VERSION = '1.0';
use Carp;
use Socket;
use Web::Transport::_Defs;

our @EXPORT = qw(find_listenable_port);

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

my $EphemeralStart = 1024;
my $EphemeralEnd = 5000;

my $UsedPorts = [@{$Web::Transport::_Defs::BadPorts}];
## Bad ports are excluded
## <https://fetch.spec.whatwg.org/#port-blocking>.

sub is_listenable_port ($) {
  my $port = shift;
    return 0 unless $port;
    return 0 if $UsedPorts->[$port];
    
    my $proto = getprotobyname('tcp');
    socket(my $server, PF_INET, SOCK_STREAM, $proto) || die "socket: $!";
    setsockopt($server, SOL_SOCKET, SO_REUSEADDR, pack("l", 1)) || die "setsockopt: $!";
    bind($server, sockaddr_in($port, INADDR_ANY)) || return 0;
    listen($server, SOMAXCONN) || return 0;
    close($server);
    return 1;
}

sub find_listenable_port () {
    
    for (1..10000) {
      my $port = $EphemeralStart + int rand($EphemeralEnd - $EphemeralStart);
        next if $UsedPorts->[$port];
        if (is_listenable_port($port)) {
            $UsedPorts->[$port] = 1;
            return $port;
        }
    }

    die "Listenable port not found";
}

1;

=head1 LICENSE

Copyright 2010 Hatena <http://www.hatena.ne.jp/>

Copyright 2020 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
