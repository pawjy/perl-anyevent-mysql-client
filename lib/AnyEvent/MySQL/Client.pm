package AnyEvent::MySQL::Client;
use strict;
use warnings;
use warnings FATAL => 'substr';
use warnings FATAL => 'uninitialized';
our $VERSION = '1.0';
require utf8;
use Scalar::Util qw(weaken);
use Digest::SHA qw(sha1);
use Encode qw(encode);
use AnyEvent::Handle;
use AnyEvent::MySQL::Client::Promise;

# XXX debug hook
# XXX SSL
# XXX prepared
# XXX new_from_dsn
# XXX new_from_url
# XXX transaction
# XXX charset handling

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
sub SERVER_SESSION_STATE_CHANGED () { 0x4000 }

sub COM_QUIT         () { 0x01 }
sub COM_QUERY        () { 0x03 }
sub COM_PING         () { 0x0E }
sub COM_STMT_PREPARE () { 0x16 }
sub COM_STMT_EXECUTE () { 0x17 }

sub OK_Packet  () { 0x00 }
sub ERR_Packet () { 0xFF }
sub EOF_Packet () { 0xFE }

# XXX
sub connection_packet_timeout () { 10 }
sub query_packet_timeout () { 60 }

sub _b ($) {
  return utf8::is_utf8 ($_[0]) ? encode ('utf-8', $_[0]) : $_[0];
} # _b

sub new ($) {
  my $class = shift;
  return bless {}, $class;
} # new

sub connect ($%) {
  my ($self, %args) = @_;
  return AnyEvent::MySQL::Client::Promise->reject
      (bless {is_exception => 1,
              message => 'There is a connection'}, __PACKAGE__ . '::Result')
          if defined $self->{connect_promise};

  my ($ok_close, $ng_close);
  my $promise_close = AnyEvent::MySQL::Client::Promise->new
      (sub { ($ok_close, $ng_close) = @_ });
  $self->{close_promise} = $promise_close;

  $self->{handle} = AnyEvent::Handle->new
      (connect => [$args{hostname}, $args{port}],
       no_delay => 1,
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
  $self->{command_promise} = AnyEvent::MySQL::Client::Promise->new
      (sub { ($ok_command, $ng_command) = @_ });
  return $self->{connect_promise} = $self->_push_read_packet
      (label => 'initial handshake',
       timeout => $self->connection_packet_timeout)->then (sub {
    my $packet = $_[0];

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
      $packet->_int (1 => 'character_set'); # XXX
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
    unless (($packet->{capability_flags} | $self->{capabilities}) == $packet->{capability_flags}) {
      die bless {is_exception => 1,
                 packet => $packet,
                 message => "Server does not have some capability: Server $packet->{capability_flags} / Client $self->{capabilities}"}, __PACKAGE__ . '::Result';
    }

    my $response = AnyEvent::MySQL::Client::SentPacket->new (1);
    $response->_int4 ($self->{capabilities});
    $response->_int4 (0x1_000000);
    $response->_int1 ($packet->{character_set});
    $response->_null (23);
    $response->_string_null (defined $args{username} ? $args{username} : ''); # XXX charset
    my $password = defined $args{password} ? $args{password} : ''; # XXX charset
    if (length $password) {
      my $stage1_hash = sha1 ($password);
      $password = sha1 ($packet->{auth_plugin_data} . sha1 ($stage1_hash)) ^ $stage1_hash;
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
      $response->_string_null (defined $args{database} ? $args{database} : ''); # XXX charset
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
    return bless {is_success => 1,
                  packet => $packet}, __PACKAGE__ . '::Result';
  })->catch (sub {
    my $error = $_[0];
    $self->_terminate_connection;
    $ng_command->();
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
    $self->{handle}->push_shutdown;
    $self->{handle}->start_read;
    return $self->{close_promise};
  } else {
    return AnyEvent::MySQL::Client::Promise->new
        (sub { $_[0]->(bless {is_success => 1}, __PACKAGE__ . '::Result') });
  }
} # disconnect

sub send_quit ($) {
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
    if ($result->is_exception and
        defined $result->{code} and
        $result->{code} == 32) { # EPIPE
      delete $result->{is_exception};
      $result->{is_success} = 1;
      return $result;
    } else {
      return $self->disconnect->then (sub { die $result });
    }
  });
} # send_quit

sub send_ping ($) {
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
} # send_ping

sub send_query ($$;$) {
  my ($self, $query, $on_row) = @_;
  return AnyEvent::MySQL::Client::Promise->reject
      (bless {is_exception => 1,
              message => 'Not connected'}, __PACKAGE__ . '::Result')
          unless defined $self->{connect_promise};
  $self->{command_promise} = $self->{command_promise}->then (sub {
    my $packet = AnyEvent::MySQL::Client::SentPacket->new (0);
    $packet->_int1 (COM_QUERY);
    $packet->_string_eof (defined $query ? _b $query : ''); # XXX charset
    $packet->_end;
    $self->_push_send_packet ($packet);
    return $self->_push_read_packet
        (label => 'query command response',
         timeout => $self->query_packet_timeout);
  })->then (sub {
    my $packet = $_[0];
    ## <http://dev.mysql.com/doc/internals/en/com-query-response.html>.
    $self->_parse_packet ($packet);
    if (not defined $packet->{header}) {
      $packet->_int_lenenc ('column_count');
      $packet->_end;
      my $column_count = $packet->{column_count};
      my @column;

      my $promise = AnyEvent::MySQL::Client::Promise->all ([map {
        $self->_push_read_packet
            (label => 'column definition ' . $_,
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
          push @column, $packet;
        });
      } 1..$column_count]);
      unless ($self->{capabilities} & CLIENT_DEPRECATE_EOF) {
        $promise = $promise->then (sub {
          return $self->_push_read_packet
              (label => 'EOF after column definitions',
               timeout => $self->query_packet_timeout);
        })->then (sub {
          my $packet = $_[0];
          $self->_parse_packet ($packet);
          if (not defined $packet->{header} or
              not $packet->{header} == EOF_Packet) {
            die bless {is_exception => 1,
                       message => 'Unexpected packet',
                       packet => $packet}, __PACKAGE__ . '::Result';
          }
        });
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
            $on_row->(bless {is_success => 1,
                             column_packets => \@column,
                             packet => $packet}, __PACKAGE__ . '::Result')
                if defined $on_row;
            return $read_row_code->();
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
    return $_[0];
  }, sub {
    $self->_terminate_connection;
    die $_[0];
  });
} # send_query

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
      $packet->_string (2 => 'warnings');
      $packet->_string (2 => 'status_flags');
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
    pos (${$_[0]->{payload_ref}}) += 2;
  } elsif ($fb eq "\xFD") {
    $_[0]->{$_[1]} = unpack 'V', substr ${$_[0]->{payload_ref}}, $pos + 1, 3;
    pos (${$_[0]->{payload_ref}}) += 3;
  } elsif ($fb eq "\xFE") {
    $_[0]->{$_[1]} = unpack 'Q<', substr ${$_[0]->{payload_ref}}, $pos + 1, 8;
    pos (${$_[0]->{payload_ref}}) += 8;
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

package AnyEvent::MySQL::Client::SentPacket;

sub new ($$) {
  my $packet = '';
  die "Bad sequence ID" if $_[1] > 0xFF or $_[1] < 0;
  return bless {payload_ref => \$packet, sequence_id => $_[1]}, $_[0];
} # new

sub _int1 ($$) {
  die sprintf 'Bad value %d', $_[1] if $_[1] > 0xFF or $_[1] < 0;
  ${$_[0]->{payload_ref}} .= substr pack ('V', $_[1]), 0, 1;
} # _int1

sub _int4 ($$) {
  die sprintf 'Bad value %d', $_[1] if $_[1] > 0xFFFFFFFF or $_[1] < 0;
  ${$_[0]->{payload_ref}} .= substr pack ('V', $_[1]), 0, 4;
} # _int4

sub _int_lenenc ($$) {
  if ($_[1] < 0) {
    die "Bad value $_[1]";
  } elsif ($_[1] < 251) {
    ${$_[0]->{payload_ref}} .= pack 'C', $_[1];
  } elsif ($_[1] < 2**16) {
    ${$_[0]->{payload_ref}} .= "\xFC" . pack 'v', $_[1];
  } elsif ($_[1] < 2**24) {
    ${$_[0]->{payload_ref}} .= "\xFD" . substr pack ('V', $_[1]), 0, 3;
  } elsif ($_[1] < 2**64) {
    ${$_[0]->{payload_ref}} .= "\xFE" . pack 'Q<', $_[1];
  } else {
    die "Bad value $_[1]";
  }
} # _int_lenenc

sub _string_null ($$) {
  die "Value has NULL" if $_[1] =~ /\x00/;
  ${$_[0]->{payload_ref}} .= $_[1] . "\x00";
} # _string_null

sub _string_var ($$) {
  ${$_[0]->{payload_ref}} .= $_[1];
} # _string_var

sub _string_eof ($$) {
  ${$_[0]->{payload_ref}} .= $_[1];
} # _string_eof

sub _string_lenenc ($$) {
  $_[0]->_int_lenenc (length $_[1]);
  ${$_[0]->{payload_ref}} .= $_[1];
} # _string_lenenc

sub _null ($$) {
  ${$_[0]->{payload_ref}} .= ("\x00" x $_[1]);
} # _null

sub _end ($) {
  $_[0]->{payload_length} = length ${$_[0]->{payload_ref}};
  die "Packet payload too long"
      if 0xFFFFFF < length $_[0]->{payload_length};
} # _end

package AnyEvent::MySQL::Client::Result;
use overload '""' => 'stringify', fallback => 1;

sub is_success ($) { $_[0]->{is_success} }
sub is_failure ($) { $_[0]->{is_failure} }
sub is_exception ($) { $_[0]->{is_exception} }
sub packet ($) { $_[0]->{packet} }
sub column_packets ($) { $_[0]->{column_packets} }

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
