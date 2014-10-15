package AnyEvent::MySQL::Client;
use strict;
use warnings;
use warnings FATAL => 'substr';
use warnings FATAL => 'uninitialized';
our $VERSION = '1.0';
require utf8;
use Scalar::Util qw(weaken);
use Digest::SHA qw(sha1);
use AnyEvent::Handle;
use AnyEvent::MySQL::Client::Promise;

## Character sets
## <http://dev.mysql.com/doc/internals/en/character-set.html>.
sub CHARSET_LATIN1 () { 8 } # latin1_swedish_ci
sub CHARSET_UTF8 () { 33 } # utf8_general_ci
sub CHARSET_BINARY () { 63 } # binary

## Capability flags
## <http://dev.mysql.com/doc/internals/en/capability-flags.html>.
sub CLIENT_LONG_PASSWORD                  () { 1 }
sub CLIENT_FOUND_ROWS                     () { 2 }
sub CLIENT_LONG_FLAG                      () { 4 }
sub CLIENT_CONNECT_WITH_DB                () { 8 }
sub CLIENT_PROTOCOL_41                    () { 512 }
sub CLIENT_SSL                            () { 2048 }
sub CLIENT_TRANSACTIONS                   () { 8192 }
sub CLIENT_SECURE_CONNECTION              () { 32768 }
sub CLIENT_MULTI_STATEMENTS               () { 65536 }
sub CLIENT_MULTI_RESULTS                  () { 131072 }
sub CLIENT_PLUGIN_AUTH                    () { 0x00080000 }
sub CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA () { 0x00200000 }
sub CLIENT_SESSION_TRACK                  () { 0x00800000 }
sub CLIENT_DEPRECATE_EOF                  () { 0x01000000 }

## Status flags
## <http://dev.mysql.com/doc/internals/en/status-flags.html>.
sub SERVER_STATUS_CURSOR_EXISTS () { 0x0040 }
sub SERVER_SESSION_STATE_CHANGED () { 0x4000 }

## Column definition's |flags| value
## <http://dev.mysql.com/doc/refman/5.7/en/c-api-data-structures.html>.
sub UNSIGNED_FLAG () { 32 }

sub COM_QUIT         () { 0x01 }
sub COM_QUERY        () { 0x03 }
sub COM_PING         () { 0x0E }
sub COM_STMT_PREPARE () { 0x16 }
sub COM_STMT_EXECUTE () { 0x17 }
sub COM_STMT_CLOSE   () { 0x19 }
sub COM_STMT_RESET   () { 0x1A }

sub OK_Packet  () { 0x00 }
sub ERR_Packet () { 0xFF }
sub EOF_Packet () { 0xFE }

our $OnActionInit ||= sub { };
our $OnActionStart ||= sub { };
our $OnActionEnd ||= sub { };

sub new ($) {
  my $class = shift;
  return bless {}, $class;
} # new

sub connection_packet_timeout ($;$) {
  if (@_ > 1) {
    $_[0]->{connection_packet_timeout} = $_[1];
  }
  return $_[0]->{connection_packet_timeout} if defined $_[0]->{connection_packet_timeout};
  return 10;
} # connection_packet_timeout

sub query_packet_timeout ($;$) {
  if (@_ > 1) {
    $_[0]->{query_packet_timeout} = $_[1];
  }
  return $_[0]->{query_packet_timeout} if defined $_[0]->{query_packet_timeout};
  return 60;
} # query_packet_timeout

sub connect ($%) {
  my ($self, %args) = @_;
  return AnyEvent::MySQL::Client::Promise->reject
      (bless {is_exception => 1,
              message => 'There is a connection'}, __PACKAGE__ . '::Result')
          if defined $self->{connect_promise};

  for (qw(username password database)) {
    if (defined $args{$_} and utf8::is_utf8 ($args{$_})) {
      return AnyEvent::MySQL::Client::Promise->reject
          (bless {is_exception => 1,
                  message => "|$_| is utf8-flagged"}, __PACKAGE__ . '::Result');
    }
  }

  ## Character set
  ## <http://dev.mysql.com/doc/refman/5.7/en/charset-connection.html>,
  ## <http://dev.mysql.com/doc/internals/en/character-set.html>.
  my $charset = CHARSET_BINARY;
  if (defined $args{character_set}) {
    if ($args{character_set} eq 'binary') {
      #
    } elsif ($args{character_set} eq 'latin1') {
      $charset = CHARSET_LATIN1;
    } elsif ($args{character_set} eq 'utf8') {
      $charset = CHARSET_UTF8;
    } elsif ($args{character_set} =~ /\A[0-9]+\z/) {
      $charset = 0+$args{character_set};
    } elsif ($args{character_set} eq 'default') {
      $charset = undef;
    } else {
      return AnyEvent::MySQL::Client::Promise->reject
          (bless {is_exception => 1,
                  message => "Unknown character set |$args{character_set}|"},
               __PACKAGE__ . '::Result');
    }
  }

  my $action_state = $OnActionInit->(%args, character_set => $charset,
                                     object => $self,
                                     action_type => 'connect');
  $OnActionStart->(state => $action_state);

  my ($ok_close, $ng_close);
  my $promise_close = AnyEvent::MySQL::Client::Promise->new
      (sub { ($ok_close, $ng_close) = @_ });
  $self->{close_promise} = $promise_close;

  $self->{handle} = AnyEvent::Handle->new
      (connect => [$args{hostname}, $args{port}],
       no_delay => 1,
       wtimeout => $self->query_packet_timeout,
       on_connect => sub {
         #
       },
       on_connect_error => sub {
         my ($hdl, $msg) = @_;
         $hdl->destroy;
         if (defined $self->{on_eof}) {
           $self->{on_eof}->(bless {is_exception => 1,
                                    code => 0+$!,
                                    message => $msg}, __PACKAGE__ . '::Result');
           $ok_close->(bless {is_success => 1}, __PACKAGE__ . '::Result');
         } else {
           $ng_close->(bless {is_exception => 1,
                              code => 0+$!,
                              message => $msg}, __PACKAGE__ . '::Result');
         }
         delete $self->{handle};
         delete $self->{connect_promise};
         delete $self->{command_promise};
         delete $self->{close_promise};
       },
       on_error => sub {
         my ($hdl, $fatal, $msg) = @_;
         $hdl->destroy;
         if (defined $self->{on_eof}) {
           $self->{on_eof}->(bless {is_exception => 1,
                                    code => 0+$!,
                                    message => $msg}, __PACKAGE__ . '::Result');
           $ok_close->(bless {is_success => 1}, __PACKAGE__ . '::Result');
         } else {
           $ng_close->(bless {is_exception => 1,
                              code => 0+$!,
                              message => $msg}, __PACKAGE__ . '::Result');
         }
         delete $self->{handle};
         delete $self->{connect_promise};
         delete $self->{command_promise};
         delete $self->{close_promise};
       },
       on_eof => sub {
         my ($hdl) = @_;
         $hdl->destroy;
         $self->{on_eof}->() if defined $self->{on_eof};
         $ok_close->(bless {is_success => 1}, __PACKAGE__ . '::Result');
         delete $self->{handle};
         delete $self->{connect_promise};
         delete $self->{command_promise};
         delete $self->{close_promise};
       });

  my ($ok_command, $ng_command);
  my $handshake_packet;
  $self->{command_promise} = AnyEvent::MySQL::Client::Promise->new
      (sub { ($ok_command, $ng_command) = @_ });
  return $self->{connect_promise} = $self->_push_read_packet
      (label => 'initial handshake',
       timeout => $self->connection_packet_timeout)->then (sub {
    my $packet = $handshake_packet = $_[0];

    $packet->_int (1 => 'version');
    unless ($packet->{version} == 0x0A) {
      $self->_terminate_connection;
      die bless {is_exception => 1,
                 packet => $packet,
                 message => sprintf 'Protocol version %02X not supported',
                     $packet->{version}}, __PACKAGE__ . '::Result';
    }

    $packet->_string_null ('server_version');
    $packet->_int (4 => 'connection_id');
    $packet->_string (8 => 'auth_plugin_data');
    $packet->_skip (1);
    $packet->_int (2 => 'capability_flags');
    if ($packet->_has_more_bytes) {
      $packet->_int (1 => 'character_set');
      $packet->_int (2 => 'status_flags');
      $packet->_int (2 => 'capability_flags_2');
      $packet->{capability_flags} |= (delete $packet->{capability_flags_2}) << 16;
      if ($packet->{capability_flags} & CLIENT_PLUGIN_AUTH) {
        $packet->_int (1 => 'auth_plugin_data_len');
      } else {
        $packet->_skip (1);
      }
      $packet->_skip (10);
      if ($packet->{capability_flags} & CLIENT_SECURE_CONNECTION) {
        my $length = $packet->{auth_plugin_data_len} - 8;
        $length = 13 if $length < 13;
        $packet->_string ($length => 'auth_plugin_data_2');
        $packet->{auth_plugin_data} .= delete $packet->{auth_plugin_data_2};
        $packet->{auth_plugin_data} =~ s/\x00\z//;
      }
      if ($packet->{capability_flags} & CLIENT_PLUGIN_AUTH) {
        $packet->_string_null ('auth_plugin_name');
      }
    }
    $packet->_end;

    $self->{capabilities} = CLIENT_LONG_PASSWORD | CLIENT_FOUND_ROWS |
        CLIENT_LONG_FLAG | CLIENT_CONNECT_WITH_DB | CLIENT_PROTOCOL_41 |
        CLIENT_TRANSACTIONS | CLIENT_SECURE_CONNECTION |
        CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS;
    $charset = $packet->{character_set} if not defined $charset;
    $self->{character_set} = $charset;

    if (defined $args{tls}) {
      unless ($packet->{capability_flags} & CLIENT_SSL) {
        die bless {is_exception => 1,
                   packet => $packet,
                   message => 'Server does not support TLS'},
                       __PACKAGE__ . '::Result';
      }
      $self->{capabilities} |= CLIENT_SSL;
    }

    unless (($packet->{capability_flags} | $self->{capabilities}) == $packet->{capability_flags}) {
      die bless {is_exception => 1,
                 packet => $packet,
                 message => "Server does not have some capability: Server $packet->{capability_flags} / Client $self->{capabilities}"}, __PACKAGE__ . '::Result';
    }

    if (defined $args{tls}) {
      ## <http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::SSLRequest>
      my $req = AnyEvent::MySQL::Client::SentPacket->new (1);
      $req->_int4 ($self->{capabilities});
      $req->_int4 (0x1_000000);
      $req->_int1 ($charset);
      $req->_null (23);
      $req->_end;
      $self->_push_send_packet ($req);

      my ($tls_ok, $tls_ng) = @_;
      my $promise = AnyEvent::MySQL::Client::Promise->new
          (sub { ($tls_ok, $tls_ng) = @_ });
      $self->{handle}->on_drain (sub {
        $_[0]->on_starttls (sub {
          if ($_[1]) {
            $tls_ok->([$packet, 2]);
          } else {
            $tls_ng->(bless {is_exception => 1,
                             handshake_packet => $packet,
                             message => "Can't start TLS: $_[2]"},
                          __PACKAGE__ . '::Result');
          }
          $_[0]->on_starttls (undef);
        });
        $_[0]->on_drain (undef);
        $_[0]->starttls ('connect', {verify => 1, %{$args{tls}}});
      });
      $self->{handle}->start_read;
      return $promise;
    } else {
      return [$packet, 1];
    }
  })->then (sub {
    my ($packet, $next_id) = @{$_[0]};
    my $response = AnyEvent::MySQL::Client::SentPacket->new ($next_id);
    $response->_int4 ($self->{capabilities});
    $response->_int4 (0x1_000000);
    $response->_int1 ($charset);
    $response->_null (23);
    $response->_string_null (defined $args{username} ? $args{username} : '');

    ## Secure password authentication (|mysql_native_password|)
    ## <http://dev.mysql.com/doc/internals/en/secure-password-authentication.html>.
    my $password = defined $args{password} ? $args{password} : '';
    if (length $password) {
      my $sp = sha1 ($password);
      $password = $sp ^ sha1 ($packet->{auth_plugin_data} . sha1 ($sp));
    }
    if ($self->{capabilities} & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
      $response->_string_lenenc ($password);
    } elsif ($self->{capabilities} & CLIENT_SECURE_CONNECTION) {
      $response->_int1 (length $password);
      $response->_string_var ($password);
    } else {
      $response->_string_null ($password);
    }

    if ($self->{capabilities} & CLIENT_CONNECT_WITH_DB) {
      $response->_string_null (defined $args{database} ? $args{database} : '');
    }
    #if ($self->{capabilities} & CLIENT_PLUGIN_AUTH) {
    #  $response->_string_null ('(auth plugin name)');
    #}
    #if ($self->{capabilities} & CLIENT_CONNECT_ATTRS) {
    #  # (not supported)
    #}
    $response->_end;

    $self->_push_send_packet ($response);
    return $self->_push_read_packet
      (label => 'response to handshake response',
       timeout => $self->connection_packet_timeout);
  })->then (sub {
    my $packet = $_[0];
    $self->_parse_packet ($packet);
    if (not defined $packet->{header} or not $packet->{header} == OK_Packet) {
      die bless {is_exception => 1,
                 packet => $packet,
                 message => "Failed to connect to server"}, __PACKAGE__ . '::Result';
    }
    $ok_command->();
    my $result = bless {is_success => 1,
                        handshake_packet => $handshake_packet,
                        packet => $packet}, __PACKAGE__ . '::Result';
    $OnActionEnd->(state => $action_state, result => $result);
    return $result;
  })->catch (sub {
    my $error = $_[0];
    $self->_terminate_connection;
    $ng_command->();
    $OnActionEnd->(state => $action_state, result => $error);
    if (defined $self->{close_promise}) {
      return $self->{close_promise}->then (sub { die $error });
    } else {
      return AnyEvent::MySQL::Client::Promise->reject ($error);
    }
  });
} # connect

sub _terminate_connection ($) {
  my ($self) = @_;
  if (defined $self->{handle}) {
    $self->{handle}->wtimeout (0.1);
    $self->{handle}->push_shutdown;
    $self->{handle}->start_read;
  }
} # _terminate_connection

sub disconnect ($) {
  my $self = $_[0];
  if (defined $self->{connect_promise}) {
    $self->{connect_promise}->then (sub {
      $self->{handle}->push_shutdown;
      $self->{handle}->push_read (sub { return 0 }); # discard
    }, sub {
      $self->{handle}->push_shutdown;
      $self->{handle}->push_read (sub { return 0 }); # discard
    });
    return $self->{close_promise}->catch (sub {
      my $result = $_[0];
      if (ref $result and
          $result->is_exception and
          defined $result->{code} and
          $result->{code} == 32) { # EPIPE
        delete $result->{is_exception};
        $result->{is_success} = 1;
        return $result;
      } else {
        die $result;
      }
    });
  } else {
    return AnyEvent::MySQL::Client::Promise->new
        (sub { $_[0]->(bless {is_success => 1}, __PACKAGE__ . '::Result') });
  }
} # disconnect

sub quit ($) {
  my ($self) = @_;
  return AnyEvent::MySQL::Client::Promise->new
      (sub { $_[0]->(bless {is_success => 1,
                            message => 'Not connected'}, __PACKAGE__ . '::Result') })
      unless defined $self->{connect_promise};
  return $self->{command_promise} = $self->{command_promise}->then (sub {
    my $packet = AnyEvent::MySQL::Client::SentPacket->new (0);
    $packet->_int1 (COM_QUIT);
    $packet->_end;
    $self->_push_send_packet ($packet);
    return $self->_push_read_packet
        (label => 'quit command response',
         timeout => $self->connection_packet_timeout,
         allow_eof => 1);
  })->then (sub {
    my $packet = $_[0] or return undef;
    die bless {is_exception => 1,
               message => "Unexpected packet sequence ($packet->{sequence_id} where 1 expected)",
               packet => $packet}, __PACKAGE__ . '::Result'
                   unless $packet->{sequence_id} == 1;
    $self->_parse_packet ($packet);
    if (not defined $packet->{header} or not $packet->{header} == OK_Packet) {
      die bless {is_exception => 1,
                 message => 'Unexpected packet for COM_QUIT',
                 packet => $packet}, __PACKAGE__ . '::Result';
    }
    return undef;
  })->then (sub {
    return $self->disconnect;
  }, sub {
    my $result = $_[0];
    if (ref $result and
        $result->is_exception and
        defined $result->{code} and
        $result->{code} == 32) { # EPIPE
      delete $result->{is_exception};
      $result->{is_success} = 1;
      return $result;
    } else {
      return $self->disconnect->then (sub { die $result });
    }
  });
} # quit

sub ping ($) {
  my ($self) = @_;
  return AnyEvent::MySQL::Client::Promise->new (sub { $_[0]->(0) })
      unless defined $self->{connect_promise};
  $self->{command_promise} = $self->{command_promise}->then (sub {
    my $packet = AnyEvent::MySQL::Client::SentPacket->new (0);
    $packet->_int1 (COM_PING);
    $packet->_end;
    $self->_push_send_packet ($packet);
    return $self->_push_read_packet
        (label => 'ping command response',
         timeout => $self->connection_packet_timeout);
  })->then (sub {
    my $packet = $_[0];
    die bless {is_exception => 1,
               message => "Unexpected packet sequence ($packet->{sequence_id} where 1 expected)",
               packet => $packet}, __PACKAGE__ . '::Result'
                   unless $packet->{sequence_id} == 1;
    $self->_parse_packet ($packet);
    if (not defined $packet->{header} or not $packet->{header} == OK_Packet) {
      die bless {is_exception => 1,
                 message => 'Unexpected packet for COM_PING',
                 packet => $packet}, __PACKAGE__ . '::Result';
    }
    return undef;
  })->then (sub {
    $self->{handle}->start_read;
    return 1;
  }, sub {
    $self->_terminate_connection;
    return 0;
  });
} # ping

my $ReadEOFPacket = sub ($) {
  my $self = $_[0];
  return $self->_push_read_packet
      (label => 'EOF after column definitions',
       timeout => $self->query_packet_timeout)->then (sub {
    my $packet = $_[0];
    $self->_parse_packet ($packet);
    if (not defined $packet->{header} or
        not $packet->{header} == EOF_Packet) {
      die bless {is_exception => 1,
                 message => 'Unexpected packet',
                 packet => $packet}, __PACKAGE__ . '::Result';
    }
    return $packet;
  });
}; # $ReadEOFPacket

my $ReadColumnDefinition = sub ($$) {
  my ($self, $columns) = @_;
  return $self->_push_read_packet
      (label => 'column definition',
       timeout => $self->query_packet_timeout)->then (sub {
    ## <http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition>.
    my $packet = $_[0];
    $self->_parse_packet ($packet);
    $packet->_string_lenenc ('catalog');
    $packet->_string_lenenc ('schema');
    $packet->_string_lenenc ('table');
    $packet->_string_lenenc ('org_table');
    $packet->_string_lenenc ('name');
    $packet->_string_lenenc ('org_name');
    $packet->_int_lenenc ('next_length');
    $packet->_int (2 => 'character_set');
    $packet->_int (4 => 'column_length');
    $packet->_int (1 => 'column_type');
    $packet->_int (2 => 'flags');
    $packet->_int (1 => 'decimals');
    $packet->_skip (2);
    #if command == COM_FIELD_LIST:
    #$packet->_int_lenenc ('default_values_length');
    #$packet->_string ($packet->{default_values_length} => 'default_values');
    $packet->_end;
    push @$columns, $packet;
  });
}; # $ReadColumnDefinition

sub query ($$;$) {
  my ($self, $query, $on_row) = @_;
  return AnyEvent::MySQL::Client::Promise->reject
      (bless {is_exception => 1,
              message => 'Not connected'}, __PACKAGE__ . '::Result')
          unless defined $self->{connect_promise};

  if (utf8::is_utf8 ($query)) {
    return $self->{command_promise} = $self->{command_promise}->then (sub {
      return bless {is_failure => 1,
                    message => "Query |$query| is utf8-flagged"},
                        __PACKAGE__ . '::Result';
    });
  }

  my $action_state = $OnActionInit->(query => defined $query ? $query : '',
                                     object => $self,
                                     action_type => 'query');
  return $self->{command_promise} = $self->{command_promise}->then (sub {
    $OnActionStart->(state => $action_state);

    my $packet = AnyEvent::MySQL::Client::SentPacket->new (0);
    $packet->_int1 (COM_QUERY);
    $packet->_string_eof (defined $query ? $query : '');
    $packet->_end;
    $self->_push_send_packet ($packet);
    return $self->_push_read_packet
        (label => 'query command response',
         timeout => $self->query_packet_timeout);
  })->then (sub {
    my $packet = $_[0];
    die bless {is_exception => 1,
               message => "Unexpected packet sequence ($packet->{sequence_id} where 1 expected)",
               packet => $packet}, __PACKAGE__ . '::Result'
                   unless $packet->{sequence_id} == 1;

    ## <http://dev.mysql.com/doc/internals/en/com-query-response.html>.
    $self->_parse_packet ($packet);
    if (not defined $packet->{header}) {
      $packet->_int_lenenc ('column_count');
      $packet->_end;
      my $column_count = $packet->{column_count};
      my @column;

      my $promise = AnyEvent::MySQL::Client::Promise->all ([map {
        $ReadColumnDefinition->($self, \@column);
      } 1..$column_count]);
      unless ($self->{capabilities} & CLIENT_DEPRECATE_EOF) {
        $promise = $promise->then (sub { $ReadEOFPacket->($self) });
      }
      weaken ($self = $self);
      my $read_row_code; $read_row_code = sub {
        return $self->_push_read_packet
            (label => 'resultset row',
             timeout => $self->query_packet_timeout)->then (sub {
          ## <http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow>.
          my $packet = $_[0];
          $self->_parse_packet ($packet);
          if (not defined $packet->{header} or
              $packet->{header} == 0xFB) {
            pos (${$packet->{payload_ref}})--
                if defined $packet->{header} and $packet->{header} == 0xFB;
            $packet->{data} = [];
            for (1..$column_count) {
              $packet->_string_lenenc_or_null ('_data');
              push @{$packet->{data}}, delete $packet->{_data};
            }
            $packet->_end;
            if (defined $on_row) {
              return AnyEvent::MySQL::Client::Promise->resolve->then (sub {
                $on_row->(bless {is_success => 1,
                                 column_packets => \@column,
                                 packet => $packet}, __PACKAGE__ . '::Result');
              })->then ($read_row_code);
            } else {
              return $read_row_code->();
            }
          } elsif ($packet->{header} == OK_Packet or
                   $packet->{header} == EOF_Packet) {
            return bless {is_success => 1,
                          column_packets => \@column,
                          packet => $packet}, __PACKAGE__ . '::Result';
          } elsif ($packet->{header} == ERR_Packet) {
            return bless {is_failure => 1,
                          packet => $packet}, __PACKAGE__ . '::Result';
          } else { # Unknown
            die bless {is_exception => 1,
                       message => 'Unexpected packet',
                       packet => $packet}, __PACKAGE__ . '::Result';
          }
        });
      };
      return $promise->then (sub { $read_row_code->() });
    } elsif ($packet->{header} == OK_Packet) {
      return bless {is_success => 1, packet => $packet}, __PACKAGE__ . '::Result';
    } elsif ($packet->{header} == ERR_Packet) {
      return bless {is_failure => 1, packet => $packet}, __PACKAGE__ . '::Result';
    } else { # not supported
      die bless {is_exception => 1,
                 message => 'Unexpected packet',
                 packet => $packet}, __PACKAGE__ . '::Result';
    }
  })->then (sub {
    $self->{handle}->start_read;
    $OnActionEnd->(state => $action_state, result => $_[0]);
    return $_[0];
  }, sub {
    $self->_terminate_connection;
    $OnActionEnd->(state => $action_state, result => $_[0]);
    die $_[0];
  });
} # query

sub statement_prepare ($$) {
  my ($self, $query) = @_;
  return AnyEvent::MySQL::Client::Promise->reject
      (bless {is_exception => 1,
              message => 'Not connected'}, __PACKAGE__ . '::Result')
          unless defined $self->{connect_promise};

  if (utf8::is_utf8 ($query)) {
    return $self->{command_promise} = $self->{command_promise}->then (sub {
      return bless {is_failure => 1,
                    message => "Query |$query| is utf8-flagged"},
                        __PACKAGE__ . '::Result';
    });
  }

  my $action_state = $OnActionInit->(query => defined $query ? $query : '',
                                     object => $self,
                                     action_type => 'statement_prepare');
  return $self->{command_promise} = $self->{command_promise}->then (sub {
    $OnActionStart->(state => $action_state);

    my $packet = AnyEvent::MySQL::Client::SentPacket->new (0);
    $packet->_int1 (COM_STMT_PREPARE);
    $packet->_string_eof (defined $query ? $query : '');
    $packet->_end;
    $self->_push_send_packet ($packet);
    return $self->_push_read_packet
        (label => 'statement prepare command response',
         timeout => $self->query_packet_timeout);
  })->then (sub {
    my $packet = $_[0];
    die bless {is_exception => 1,
               message => "Unexpected packet sequence ($packet->{sequence_id} where 1 expected)",
               packet => $packet}, __PACKAGE__ . '::Result'
                   unless $packet->{sequence_id} == 1;
    if (0x00 == unpack 'C', substr ${$packet->{payload_ref}}, 0, 1) {
      ## <http://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html>.
      $packet->_int (1 => 'status');
      $packet->_int (4 => 'statement_id');
      $packet->_int (2 => 'num_columns');
      $packet->_int (2 => 'num_params');
      $packet->_skip (1);
      $packet->_int (2 => 'warning_count');
      $packet->_end;
      my $promise;
      my @param;
      my @column;
      my @read;
      if ($packet->{num_params}) {
        push @read,
          (map {
            $ReadColumnDefinition->($self, \@param);
          } 1..$packet->{num_params}),
          $ReadEOFPacket->($self);
      }
      if ($packet->{num_columns}) {
        push @read,
          (map {
            $ReadColumnDefinition->($self, \@column);
          } 1..$packet->{num_columns}),
          $ReadEOFPacket->($self);
      }
      return AnyEvent::MySQL::Client::Promise->all (\@read)->then (sub {
        return bless {is_success => 1,
                      column_packets => \@column,
                      param_packets => \@param,
                      packet => $packet}, __PACKAGE__ . '::Result';
      });
    } else {
      $self->_parse_packet ($packet);
      if ($packet->{header} == ERR_Packet) {
        return bless {is_failure => 1,
                      packet => $packet}, __PACKAGE__ . '::Result';
      } else {
        die bless {is_exception => 1,
                   message => 'Unexpected packet',
                   packet => $packet}, __PACKAGE__ . '::Result';
      }
    }
  })->then (sub {
    $self->{handle}->start_read;
    $OnActionEnd->(state => $action_state, result => $_[0]);
    return $_[0];
  }, sub {
    $self->_terminate_connection;
    $OnActionEnd->(state => $action_state, result => $_[0]);
    die $_[0];
  });
} # statement_prepare

sub statement_execute ($$;$$) {
  my ($self, $statement_id, $params_orig, $on_row) = @_;
  return AnyEvent::MySQL::Client::Promise->reject
      (bless {is_exception => 1,
              message => 'Not connected'}, __PACKAGE__ . '::Result')
          unless defined $self->{connect_promise};

  my $params = AnyEvent::MySQL::Client::Values->pack ($params_orig);
  if (defined $params->{error}) {
    return $self->{command_promise} = $self->{command_promise}->then (sub {
      return $params->{error};
    });
  }

  my $action_state = $OnActionInit->(statement_id => $statement_id,
                                     params => $params_orig,
                                     object => $self,
                                     action_type => 'statement_execute');
  return $self->{command_promise} = $self->{command_promise}->then (sub {
    $OnActionStart->(state => $action_state);

    my $packet = AnyEvent::MySQL::Client::SentPacket->new (0);
    $packet->_int1 (COM_STMT_EXECUTE);
    $packet->_int4 (0+$statement_id);
    $packet->_int1 (0); # flags
    $packet->_int4 (1); # iteration_count
    $packet->_string_var ($params->{null_bitmap}); # NULL bitmap
    $packet->_int1 (1);
    $packet->_string_eof (${$params->{types_ref}});
    $packet->_string_eof (${$params->{values_ref}});
    $packet->_end;
    $self->_push_send_packet ($packet);
    return $self->_push_read_packet
        (label => 'statement execute command response',
         timeout => $self->query_packet_timeout);
  })->then (sub {
    my $packet = $_[0];
    die bless {is_exception => 1,
               message => "Unexpected packet sequence ($packet->{sequence_id} where 1 expected)",
               packet => $packet}, __PACKAGE__ . '::Result'
                   unless $packet->{sequence_id} == 1;
    $self->_parse_packet ($packet);
    if (not defined $packet->{header}) {
      ## <http://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html>.
      $packet->_int_lenenc ('column_count');
      my $column_count = $packet->{column_count};
      $packet->_end;
      
      my @column;
      return AnyEvent::MySQL::Client::Promise->all ([
        (map {
          $ReadColumnDefinition->($self, \@column);
        } 1..$column_count),
        $ReadEOFPacket->($self),
      ])->then (sub {
        unless ($_[0]->[-1]->{status_flags} & SERVER_STATUS_CURSOR_EXISTS) {
          weaken ($self = $self);
          my $read_row_code; $read_row_code = sub {
            return $self->_push_read_packet
                (label => 'binary resultset row',
                 timeout => $self->query_packet_timeout)->then (sub {
              my $packet = $_[0];
              if (0x00 == unpack 'C', substr ${$packet->{payload_ref}}, 0, 1) {
                ## <http://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html#packet-ProtocolBinary::ResultsetRow>.
                $packet->{data} = AnyEvent::MySQL::Client::Values->unpack
                    (\@column, $packet);
                if (defined $on_row) {
                  return AnyEvent::MySQL::Client::Promise->resolve->then (sub {
                    $on_row->(bless {is_success => 1,
                                     column_packets => \@column,
                                     packet => $packet},
                              __PACKAGE__ . '::Result');
                  })->then ($read_row_code);
                } else {
                  return $read_row_code->();
                }
              } else {
                $self->_parse_packet ($packet);
                if ($packet->{header} == EOF_Packet) {
                  return bless {is_success => 1,
                                column_packets => \@column,
                                packet => $packet}, __PACKAGE__ . '::Result';
                } else { # Unknown
                  die bless {is_exception => 1,
                             message => 'Unexpected packet',
                             packet => $packet}, __PACKAGE__ . '::Result';
                }
              }
            });
          };
          return $read_row_code->();
        } else {
          return bless {is_success => 1,
                        column_packets => \@column,
                        packet => $_[0]->[-1]}, __PACKAGE__ . '::Result';
        }
      });
    } elsif ($packet->{header} == OK_Packet) {
      return bless {is_success => 1,
                    packet => $packet}, __PACKAGE__ . '::Result';
    } elsif ($packet->{header} == ERR_Packet) {
      return bless {is_failure => 1,
                    packet => $packet}, __PACKAGE__ . '::Result';
    } else {
      die bless {is_exception => 1,
                 message => 'Unexpected packet',
                 packet => $packet}, __PACKAGE__ . '::Result';
    }
  })->then (sub {
    $self->{handle}->start_read;
    $OnActionEnd->(state => $action_state, result => $_[0]);
    return $_[0];
  }, sub {
    $self->_terminate_connection;
    $OnActionEnd->(state => $action_state, result => $_[0]);
    die $_[0];
  });
} # statement_execute

sub statement_close ($$) {
  my ($self, $statement_id) = @_;
  return AnyEvent::MySQL::Client::Promise->reject
      (bless {is_exception => 1,
              message => 'Not connected'}, __PACKAGE__ . '::Result')
          unless defined $self->{connect_promise};

  return $self->{command_promise} = $self->{command_promise}->then (sub {
    my $packet = AnyEvent::MySQL::Client::SentPacket->new (0);
    $packet->_int1 (COM_STMT_CLOSE);
    $packet->_int4 (0+$statement_id);
    $packet->_end;
    $self->_push_send_packet ($packet);
    $self->{handle}->start_read;
    return bless {is_success => 1}, __PACKAGE__ . '::Result';
  })->catch (sub {
    $self->_terminate_connection;
    die $_[0];
  });
} # statement_close

sub statement_reset ($$) {
  my ($self, $statement_id) = @_;
  return AnyEvent::MySQL::Client::Promise->reject
      (bless {is_exception => 1,
              message => 'Not connected'}, __PACKAGE__ . '::Result')
          unless defined $self->{connect_promise};

  return $self->{command_promise} = $self->{command_promise}->then (sub {
    my $packet = AnyEvent::MySQL::Client::SentPacket->new (0);
    $packet->_int1 (COM_STMT_RESET);
    $packet->_int4 (0+$statement_id);
    $packet->_end;
    $self->_push_send_packet ($packet);
    return $self->_push_read_packet
        (label => 'statement reset command response',
         timeout => $self->query_packet_timeout);
  })->then (sub {
    my $packet = $_[0];
    die bless {is_exception => 1,
               message => "Unexpected packet sequence ($packet->{sequence_id} where 1 expected)",
               packet => $packet}, __PACKAGE__ . '::Result'
                   unless $packet->{sequence_id} == 1;
    $self->_parse_packet ($packet);
    if ($packet->{header} == OK_Packet) {
      return bless {is_success => 1,
                    packet => $packet}, __PACKAGE__ . '::Result';
    } elsif ($packet->{header} == ERR_Packet) {
      return bless {is_failure => 1,
                    packet => $packet}, __PACKAGE__ . '::Result';
    } else {
      die bless {is_exception => 1,
                 message => 'Unexpected packet',
                 packet => $packet}, __PACKAGE__ . '::Result';
    }
  })->then (sub {
    $self->{handle}->start_read;
    return $_[0];
  }, sub {
    $self->_terminate_connection;
    die $_[0];
  });
} # statement_reset

sub _push_read_packet ($) {
  my ($self, %args) = @_;
  my ($ok, $ng) = @_;
  my $promise = AnyEvent::MySQL::Client::Promise->new
      (sub { ($ok, $ng) = @_ });

  my $code = sub {
    my $handle = $self->{handle};
    $self->{on_eof} = sub {
      if ($args{allow_eof} and not defined $_[0]) {
        $ok->(undef);
      } else {
        $ng->(defined $_[0] ? $_[0]
                            : (bless {is_exception => 1,
                                      message => "$args{label}: Connection closed"}, __PACKAGE__ . '::Result'));
      }
      undef $self;
    };
    $handle->rtimeout (0);
    $handle->rtimeout_reset;
    $handle->on_rtimeout (sub {
      $ng->(bless {is_exception => 1,
                   message => "$args{label}: Timeout"}, __PACKAGE__ . '::Result');
      $_[0]->push_shutdown;
    });
    $handle->rtimeout ($args{timeout} || 10);

    ## <http://dev.mysql.com/doc/internals/en/mysql-packet.html#packet-Protocol::Packet>
    $handle->push_read (chunk => 4, sub {
      my $payload_length = unpack 'V', substr ($_[1], 0, 3) . "\x00";
      my $sequence_id = unpack 'C', substr $_[1], 3, 1;
      $_[0]->unshift_read (chunk => $payload_length, sub {
        $_[0]->rtimeout (0);
        $_[0]->on_rtimeout (undef);
        delete $self->{on_eof};
        my $packet = bless {payload_length => $payload_length,
                            sequence_id => $sequence_id,
                            payload_ref => \($_[1])},
                                __PACKAGE__ . '::ReceivedPacket';
        pos (${$packet->{payload_ref}}) = 0;
        $ok->($packet);

        if (@{$self->{read_packet_codes} ||= []}) {
          (shift @{$self->{read_packet_codes}})->();
        }
      });
    });
  }; # $code;
  if (@{$self->{read_packet_codes} ||= []}) {
    push @{$self->{read_packet_codes}}, $code;
  } else {
    $code->();
  }

  return $promise;
} # _push_read_packet

sub _push_send_packet ($$) {
  my ($self, $packet) = @_;
  $self->{handle}->push_write
      (substr (pack ('V', $packet->{payload_length}), 0, 3) .
       pack ('C', $packet->{sequence_id}) .
       ${$packet->{payload_ref}});
} # _push_send_packet

sub _parse_packet ($$) {
  my ($self, $packet) = @_;
  $packet->_int (1 => 'header');
  if ($packet->{header} == OK_Packet) {
    ## <http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html>
    $packet->_int_lenenc ('affected_rows');
    $packet->_int_lenenc ('last_insert_id');
    if ($self->{capabilities} & CLIENT_PROTOCOL_41) {
      $packet->_int (2 => 'status_flags');
      $packet->_int (2 => 'warnings');
    } elsif ($self->{capabilities} & CLIENT_TRANSACTIONS) {
      $packet->_int (2 => 'status_flags');
    }
    if ($self->{capabilities} & CLIENT_SESSION_TRACK) {
      $packet->_string_lenenc ('info');
      if ($packet->{status_flags} & SERVER_SESSION_STATE_CHANGED) {
        $packet->_string_lenenc ('session_state_changes');
      }
    } else {
      $packet->_string_eof ('info');
    }
    $packet->_end;
  } elsif ($packet->{header} == ERR_Packet) {
    ## <http://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html>.
    $packet->_int (2 => 'error_code');
    if ($self->{capabilities} & CLIENT_PROTOCOL_41) {
      $packet->_string (1 => 'sql_state_marker');
      $packet->_string (5 => 'sql_state');
    }
    $packet->_string_eof ('error_message');
    $packet->_end;
  } elsif ($packet->{header} == EOF_Packet and
           $packet->{payload_length} <= 9-4) {
    ## <http://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html>.
    if ($self->{capabilities} & CLIENT_PROTOCOL_41) {
      $packet->_int (2 => 'warnings');
      $packet->_int (2 => 'status_flags');
    }
    $packet->_end;
  } elsif ($packet->{header} == 0xFB) {
    #
  } else {
    pos (${$packet->{payload_ref}})--;
    delete $packet->{header};
  }
} # _parse_packet

sub DESTROY {
  $_[0]->disconnect;
}

package AnyEvent::MySQL::Client::ReceivedPacket;

sub _skip ($$) {
  die bless {is_exception => 1,
             packet => $_[0],
             message => "Incomplete packet"}, 'AnyEvent::MySQL::Client::Result'
                 unless $_[0]->_has_more_bytes ($_[1]);
  pos (${$_[0]->{payload_ref}}) += $_[1];
} # _skip

sub _int ($$$) {
  die bless {is_exception => 1,
             packet => $_[0],
             message => "Incomplete packet"}, 'AnyEvent::MySQL::Client::Result'
                 unless $_[0]->_has_more_bytes ($_[1]);
  my $pos = pos ${$_[0]->{payload_ref}};
  pos (${$_[0]->{payload_ref}}) += $_[1];
  $_[0]->{$_[2]} = unpack 'V', substr (${$_[0]->{payload_ref}}, $pos, $_[1]) . "\x00\x00\x00";
} # _int

sub _int_lenenc ($$) {
  my $pos = pos ${$_[0]->{payload_ref}};
  my $fb = substr ${$_[0]->{payload_ref}}, $pos, 1;
  if ($fb eq "\xFB") {
    die bless {is_exception => 1,
               packet => $_[0],
               message => "0xFB is specified as int<lenenc>"},
                   'AnyEvent::MySQL::Client::Result';
  } elsif ($fb eq "\xFF") {
    die bless {is_exception => 1,
               packet => $_[0],
               message => "0xFF is specified as int<lenenc>"},
                   'AnyEvent::MySQL::Client::Result';
  } elsif ($fb eq "\xFC") {
    $_[0]->{$_[1]} = unpack 'v', substr ${$_[0]->{payload_ref}}, $pos + 1, 2;
    pos (${$_[0]->{payload_ref}}) += 3;
  } elsif ($fb eq "\xFD") {
    $_[0]->{$_[1]} = unpack 'V', substr (${$_[0]->{payload_ref}}, $pos + 1, 3) . "\x00";
    pos (${$_[0]->{payload_ref}}) += 4;
  } elsif ($fb eq "\xFE") {
    $_[0]->{$_[1]} = unpack 'Q<', substr ${$_[0]->{payload_ref}}, $pos + 1, 8;
    pos (${$_[0]->{payload_ref}}) += 9;
  } else {
    $_[0]->{$_[1]} = unpack 'C', $fb;
    pos (${$_[0]->{payload_ref}}) += 1;
  }
} # _int_lenenc

sub _string ($$$) {
  die bless {is_exception => 1,
             packet => $_[0],
             message => "Incomplete packet"}, 'AnyEvent::MySQL::Client::Result'
                 unless $_[0]->_has_more_bytes ($_[1]);
  my $pos = pos ${$_[0]->{payload_ref}};
  pos (${$_[0]->{payload_ref}}) += $_[1];
  $_[0]->{$_[2]} = substr ${$_[0]->{payload_ref}}, $pos, $_[1];
} # _string

sub _string_eof ($$) {
  ${$_[0]->{payload_ref}} =~ /\G(.*)/gcs;
  $_[0]->{$_[1]} = $1;
} # _string_eof

sub _string_null ($$) {
  ${$_[0]->{payload_ref}} =~ /\G([^\x00]*)\x00/gc
      or die bless {is_exception => 1,
                    packet => $_[0],
                    message => "Incomplete packet"},
                        'AnyEvent::MySQL::Client::Result';
  $_[0]->{$_[1]} = $1;
} # _string_null

sub _string_lenenc ($$) {
  $_[0]->_int_lenenc ('_length');
  $_[0]->{$_[1]} = substr ${$_[0]->{payload_ref}}, (pos (${$_[0]->{payload_ref}})), $_[0]->{_length};
  pos (${$_[0]->{payload_ref}}) += delete $_[0]->{_length};
} # _string_lenenc

sub _string_lenenc_or_null ($$) {
  if (${$_[0]->{payload_ref}} =~ /\G\xFB/gc) {
    $_[0]->{$_[1]} = undef;
  } else {
    $_[0]->_int_lenenc ('_length');
    $_[0]->{$_[1]} = substr ${$_[0]->{payload_ref}}, (pos (${$_[0]->{payload_ref}})), $_[0]->{_length};
    pos (${$_[0]->{payload_ref}}) += delete $_[0]->{_length};
  }
} # _string_lenenc_or_null

sub _has_more_bytes ($;$) {
  return pos (${$_[0]->{payload_ref}}) + ($_[1] || 0) <= length ${$_[0]->{payload_ref}};
} # _has_more_bytes

sub _end ($) {
  die bless {is_exception => 1,
             packet => $_[0],
             message => (sprintf "Packet has remaining data (%d < %d)",
                             pos ${$_[0]->{payload_ref}},
                             length ${$_[0]->{payload_ref}})},
                                 'AnyEvent::MySQL::Client::Result'
                                     if $_[0]->_has_more_bytes (1);
  delete $_[0]->{payload_ref};
} # _end

## For debugging
sub dump ($) {
  print STDERR join ' ', map { sprintf '%02X', ord $_ } split //, ${$_[0]->{payload_ref}};
  print STDERR "\n";
} # dump

package AnyEvent::MySQL::Client::SentPacket;
use Carp qw(croak);

sub new ($$) {
  my $packet = '';
  croak "Bad sequence ID" if $_[1] > 0xFF or $_[1] < 0;
  return bless {payload_ref => \$packet, sequence_id => $_[1]}, $_[0];
} # new

sub _pack ($$$) {
  my $packed = pack ($_[1], $_[2]);
  die bless {is_exception => 1,
             message => "Value range error: |$_[2]|"},
                 'AnyEvent::MySQL::Client::Result'
                     unless $_[2] == unpack $_[1], $packed;
  ${$_[0]->{payload_ref}} .= $packed;
} # _pack

sub _int1 ($$) { $_[0]->_pack ('C', $_[1]) }
sub _int2 ($$) { $_[0]->_pack ('v', $_[1]) }
sub _int4 ($$) { $_[0]->_pack ('V', $_[1]) }

sub _int_lenenc ($$) {
  if ($_[1] < 0) {
    croak "Bad value $_[1]";
  } elsif ($_[1] < 251) {
    ${$_[0]->{payload_ref}} .= pack 'C', $_[1];
  } elsif ($_[1] < 2**16) {
    ${$_[0]->{payload_ref}} .= "\xFC" . pack 'v', $_[1];
  } elsif ($_[1] < 2**24) {
    ${$_[0]->{payload_ref}} .= "\xFD" . substr pack ('V', $_[1]), 0, 3;
  } elsif ($_[1] < 2**64) {
    ${$_[0]->{payload_ref}} .= "\xFE" . pack 'Q<', $_[1];
  } else {
    croak "Bad value $_[1]";
  }
} # _int_lenenc

sub _string_null ($$) {
  die bless {is_exception => 1,
             message => "Value contains NULL"},
                 'AnyEvent::MySQL::Client::Result'
                     if $_[1] =~ /\x00/;
  die bless {is_exception => 1,
             message => "Value is utf8-flagged: |$_[1]|"},
                 'AnyEvent::MySQL::Client::Result'
                     if utf8::is_utf8 ($_[1]);
  ${$_[0]->{payload_ref}} .= $_[1] . "\x00";
} # _string_null

sub _string_var ($$) {
  die bless {is_exception => 1,
             message => "Value is utf8-flagged: |$_[1]|"},
                 'AnyEvent::MySQL::Client::Result'
                     if utf8::is_utf8 ($_[1]);
  ${$_[0]->{payload_ref}} .= $_[1];
} # _string_var

sub _string_eof ($$) {
  die bless {is_exception => 1,
             message => "Value is utf8-flagged: |$_[1]|"},
                 'AnyEvent::MySQL::Client::Result'
                     if utf8::is_utf8 ($_[1]);
  ${$_[0]->{payload_ref}} .= $_[1];
} # _string_eof

sub _string_lenenc ($$) {
  die bless {is_exception => 1,
             message => "Value is utf8-flagged: |$_[1]|"},
                 'AnyEvent::MySQL::Client::Result'
                     if utf8::is_utf8 ($_[1]);
  $_[0]->_int_lenenc (length $_[1]);
  ${$_[0]->{payload_ref}} .= $_[1];
} # _string_lenenc

sub _null ($$) {
  ${$_[0]->{payload_ref}} .= ("\x00" x $_[1]);
} # _null

sub _end ($) {
  $_[0]->{payload_length} = length ${$_[0]->{payload_ref}};
  croak "Packet payload too long"
      if 0xFFFFFF < length $_[0]->{payload_length};
} # _end

## For debugging
sub dump ($) {
  print STDERR join ' ', map { sprintf '%02X', ord $_ } split //, ${$_[0]->{payload_ref}};
  print STDERR "\n";
} # dump

package AnyEvent::MySQL::Client::Values;

## Column types
## <http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType>
my $TypeNameToTypeID = {
  DECIMAL => 0x00,
  TINY => 0x01,
  SHORT => 0x02,
  LONG => 0x03,
  FLOAT => 0x04,
  DOUBLE => 0x05,
  #NULL => 0x06,
  TIMESTAMP => 0x07,
  LONGLONG => 0x08,
  #INT24 => 0x09,
  DATE => 0x0A,
  TIME => 0x0B,
  DATETIME => 0x0C,
  #YEAR => 0x0D,
  VARCHAR => 0x0F,
  BIT => 0x10,
  NEWDECIMAL => 0xF6,
  ENUM => 0xF7,
  SET => 0xF8,
  TINY_BLOB => 0xF9,
  MEDIUM_BLOB => 0xFA,
  LONG_BLOB => 0xFB,
  BLOB => 0xFC,
  VAR_STRING => 0xFD,
  STRING => 0xFE,
  GEOMETRY => 0xFF,
};
my $TypeIDToTypeName = {reverse %$TypeNameToTypeID};

my $TypeIDToValueSyntax = {
  0x00, '_string_lenenc', # DECIMAL
  0x01, ['_pack', 'c', 'C', 1], # TINY
  0x02, ['_pack', 's<', 'v', 2], # SHORT
  0x03, ['_pack', 'l<', 'V', 4], # LONG
  0x04, ['_pack', 'f<', 'f<', 4], # FLOAT
  0x05, ['_pack', 'd<', 'd<', 8], # DOUBLE
  0x07, '_timestamp', # TIMESTAMP
  0x08, ['_pack', 'q<', 'Q<', 8], # LONGLONG
  #0x09, ['_pack', 'l<', 'V', 4], # INT24
  0x0A, '_timestamp', # DATE
  0x0B, '_time', # TIME
  0x0C, '_timestamp', # DATETIME
  #0x0D, ['_pack', 's<', 'V', 2], # YEAR
  0x0F, '_string_lenenc', # VARCHAR
  0x10, '_string_lenenc', # BIT
  0xF6, '_string_lenenc', # NEWDECIMAL
  0xF7, '_string_lenenc', # ENUM
  0xF8, '_string_lenenc', # SET
  0xF9, '_string_lenenc', # TINY_BLOB
  0xFA, '_string_lenenc', # MEDIUM_BLOB
  0xFB, '_string_lenenc', # LONG_BLOB
  0xFC, '_string_lenenc', # BLOB
  0xFD, '_string_lenenc', # VAR_STRING
  0xFE, '_string_lenenc', # STRING
  0xFF, '_string_lenenc', # GEOMETRY
};

sub pack ($$) {
  my ($class, $in) = @_;
  my $out = bless {}, $class;

  unless (@{$in or []}) {
    $out->{null_bitmap} = '';
    $out->{types_ref} = \'';
    $out->{values_ref} = \'';
    return $out;
  }

  $out->{null_bitmap} = pack 'b*', join '', map { defined $_->{value} ? '0' : '1' } @$in;

  my $types = AnyEvent::MySQL::Client::SentPacket->new (0);
  my $values = AnyEvent::MySQL::Client::SentPacket->new (0);

  eval {
    for (@$in) {
      ## <http://dev.mysql.com/doc/internals/en/com-stmt-execute.html>
      my $type = $TypeNameToTypeID->{$_->{type} || ''};
      die bless {is_failure => 1,
                 message => "Unknown type |@{[$_->{type} || '']}|"},
                     'AnyEvent::MySQL::Client::Result'
                         unless defined $type;
      $types->_int1 ($type);
      $types->_int1 ($_->{unsigned} ? 0x80 : 0x00);
      if (defined $_->{value}) {
        my $syntax = $TypeIDToValueSyntax->{$type};
        if (ref $syntax) {
          $values->_pack ($_->{unsigned} ? $syntax->[2] : $syntax->[1],
                          $_->{value});
        } elsif ($syntax eq '_timestamp') {
          unless ($_->{value} =~ /\A([0-9]{4})-([0-9]{2})-([0-9]{2})(?: ([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]+)|)|)\z/) {
            die bless {is_failure => 1,
                       message => "Timestamp syntax error: |$_->{value}|"},
                           'AnyEvent::MySQL::Client::Result';
          }
          $values->_int1 (11);
          $values->_int2 ($1);
          $values->_int1 ($2);
          $values->_int1 ($3);
          $values->_int1 ($4 || 0);
          $values->_int1 ($5 || 0);
          $values->_int1 ($6 || 0);
          $values->_int4 (substr ((($7 || 0).'000000'), 0, 6));
        } elsif ($syntax eq '_time') {
          unless ($_->{value} =~ /\A(-|)([0-9]{2,}):([0-9]{2}):([0-9]{2})(?:\.([0-9]+)|)\z/) {
            die bless {is_failure => 1,
                       message => "Time syntax error: |$_->{value}|"},
                           'AnyEvent::MySQL::Client::Result';
          }
          $values->_int1 (12);
          $values->_int1 ($1 ? 1 : 0);
          $values->_int4 (int ($2 / 24));
          $values->_int1 ($2 % 24);
          $values->_int1 ($3);
          $values->_int1 ($4);
          $values->_int4 (substr ((($5 || 0).'000000'), 0, 6));
        } else {
          $values->$syntax ($_->{value});
        }
      }
    }
  };
  if ($@) {
    $out->{error} = $@;
    if (ref $@) {
      $out->{error}->{is_failure} = 1;
      delete $out->{error}->{is_exception};
    }
  } else {
    $out->{types_ref} = $types->{payload_ref};
    $out->{values_ref} = $values->{payload_ref};
  }
  return $out;
} # pack

sub unpack ($$$) {
  my ($class, $columns, $packet) = @_;
  my $null_length = int ((@$columns + 7 + 2) / 8);
  my $null = unpack 'b*', substr ${$packet->{payload_ref}}, 1, $null_length;
  pos (${$packet->{payload_ref}}) = 1 + $null_length;
  my @value;
  for (0..$#$columns) {
    my $type = $columns->[$_]->{column_type};
    my $syntax = $TypeIDToValueSyntax->{$type};
    die bless {is_error => 1,
               packet => $packet,
               message => "Unknown type |$type|"},
                   'AnyEvent::MySQL::Client::Result' unless defined $syntax;
    my $value = {type => $TypeIDToTypeName->{$type}, value => undef};
    $value->{unsigned} = 1 if $columns->[$_]->{flags} & AnyEvent::MySQL::Client::UNSIGNED_FLAG;
    unless (substr $null, 2+$_, 1) {
      if (ref $syntax) {
        $packet->_string ($syntax->[3] => '_value');
        $value->{value} = unpack $value->{unsigned} ? $syntax->[2] : $syntax->[1], delete $packet->{_value};
      } elsif ($syntax eq '_timestamp') {
        $packet->_int (1 => '_length');
        my $length = delete $packet->{_length};
        if ($length == 11) {
          $packet->_int (2 => '_year');
          $packet->_int (1 => '_month');
          $packet->_int (1 => '_day');
          $packet->_int (1 => '_hour');
          $packet->_int (1 => '_minute');
          $packet->_int (1 => '_second');
          $packet->_int (4 => '_microsecond');
          $value->{value} = sprintf '%04d-%02d-%02d %02d:%02d:%02d.%06d',
              (delete $value->{_year}), (delete $value->{_month}), (delete $value->{_day}),
              (delete $value->{_hour}), (delete $value->{_minute}), (delete $value->{_second}),
              (delete $value->{_microsecond});
        } elsif ($length == 7) {
          $packet->_int (2 => '_year');
          $packet->_int (1 => '_month');
          $packet->_int (1 => '_day');
          $packet->_int (1 => '_hour');
          $packet->_int (1 => '_minute');
          $packet->_int (1 => '_second');
          $value->{value} = sprintf '%04d-%02d-%02d %02d:%02d:%02d',
              (delete $packet->{_year}), (delete $packet->{_month}), (delete $packet->{_day}),
              (delete $packet->{_hour}), (delete $packet->{_minute}), (delete $packet->{_second});
        } elsif ($length == 4) {
          $packet->_int (2 => '_year');
          $packet->_int (1 => '_month');
          $packet->_int (1 => '_day');
          $value->{value} = sprintf '%04d-%02d-%02d 00:00:00',
              (delete $packet->{_year}), (delete $packet->{_month}), (delete $packet->{_day});
        } elsif ($length == 0) {
          $value->{value} = '0000-00-00 00:00:00';
        } else {
          die bless {is_error => 1,
                     packet => $packet,
                     message => "Unsupported timestamp (length = $length)"},
                         'AnyEvent::MySQL::Client::Result';
        }
      } elsif ($syntax eq '_time') {
        $packet->_int (1 => '_length');
        my $length = delete $packet->{_length};
        if ($length == 12) {
          $packet->_int (1 => '_is_negative');
          $packet->_int (4 => '_days');
          $packet->_int (1 => '_hours');
          $packet->_int (1 => '_minutes');
          $packet->_int (1 => '_seconds');
          $packet->_int (4 => '_microseconds');
          $value->{value} = sprintf '%s%02d:%02d:%02d.%06d',
              (delete $packet->{_is_negative} ? '-' : ''),
              (delete $packet->{_days}) * 24 + (delete $packet->{_hours}),
              (delete $packet->{_minutes}), (delete $packet->{_seconds}),
              (delete $packet->{_microseconds});
        } elsif ($length == 8) {
          $packet->_int (1 => '_is_negative');
          $packet->_int (4 => '_days');
          $packet->_int (1 => '_hours');
          $packet->_int (1 => '_minutes');
          $packet->_int (1 => '_seconds');
          $value->{value} = sprintf '%s%02d:%02d:%02d',
              (delete $packet->{_is_negative} ? '-' : ''),
              (delete $packet->{_days}) * 24 + (delete $packet->{_hours}),
              (delete $packet->{_minutes}), (delete $packet->{_seconds});
        } elsif ($length == 0) {
          $value->{value} = '00:00:00';
        } else {
          die bless {is_error => 1,
                     packet => $packet,
                     message => "Unsupported time (length = $length)"},
                         'AnyEvent::MySQL::Client::Result';
        }
      } else {
        $packet->$syntax ('_value');
        $value->{value} = delete $packet->{_value};
      }
    }
    push @value, $value;
  }
  $packet->_end;
  return \@value;
} # unpack

package AnyEvent::MySQL::Client::Result;
use overload '""' => 'stringify', fallback => 1;

sub is_success ($) { $_[0]->{is_success} }
sub is_failure ($) { $_[0]->{is_failure} }
sub is_exception ($) { $_[0]->{is_exception} }
sub packet ($) { $_[0]->{packet} }
sub column_packets ($) { $_[0]->{column_packets} }
sub param_packets ($) { $_[0]->{param_packets} }
sub handshake_packet ($) { $_[0]->{handshake_packet} }

sub message ($) {
  my $msg = $_[0]->{message};
  if (defined $_[0]->{packet}) {
    if (defined $_[0]->{packet}->{error_message}) {
      $msg = defined $msg
          ? $msg . ': ' . $_[0]->{packet}->{error_message}
          : $_[0]->{packet}->{error_message};
    }
    if (defined $_[0]->{packet}->{error_code}) {
      $msg = defined $msg ? $msg . ' ' : '';
      $msg .= '(Error code '.$_[0]->{packet}->{error_code}.')';
    }
  }
  return $msg;
} # message

## |stringify| MUST return a true value.
sub stringify ($) {
  my $msg = $_[0]->message;
  if ($msg) {
    return $msg;
  } else {
    return $_[0]->{is_success} ? 'Success' :
           $_[0]->{is_failure} ? 'Failure' :
           $_[0]->{is_exception} ? 'Exception' : 'Unknown';
  }
} # stringify

1;

=head1 LICENSE

Copyright 2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
