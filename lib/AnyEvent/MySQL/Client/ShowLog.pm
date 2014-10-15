package AnyEvent::MySQL::Client::ShowLog;
use strict;
use warnings;
no warnings 'utf8';
our $VERSION = '1.0';
use Carp;
use Time::HiRes qw(time);
use Scalar::Util qw(refaddr);

$Carp::CarpInternal{+__PACKAGE__} = 1;

our $SQLCount ||= 0;

our $WARN;
our $COUNT;
our $EscapeMethod ||= 'perl';
our $Colored;
$Colored = -t STDERR unless defined $Colored;

if ($ENV{SQL_DEBUG}) {
  $WARN = 1;
  $COUNT = 1;
}

sub import {
  $WARN = 1 unless defined $WARN;
  $COUNT = 1 unless defined $COUNT;
} # import

$AnyEvent::MySQL::Client::OnActionInit = sub {
  my %args = @_;
  return bless {
    %args,
    object => refaddr $args{object},
    class => ref $args{object},
    source_location => Carp::shortmess,
  }, __PACKAGE__ . '::State';
};

$AnyEvent::MySQL::Client::OnActionStart = sub {
  my %args = @_;
  $args{state}->{start_time} = time;
};

$AnyEvent::MySQL::Client::OnActionEnd = sub {
  my %args = @_;
  $args{state}->{end_time} = time;
  $args{state}->show_result ($args{result});
};

package AnyEvent::MySQL::Client::ShowLog::State;
use Term::ANSIColor ();
use AnyEvent::Handle;

my $STDERR = AnyEvent::Handle->new (fh => \*STDERR, on_error => sub { });

sub with_color ($$) {
  if ($Colored) {
    return Term::ANSIColor::colored ([$_[0]], $_[1]);
  } else {
    return $_[1];
  }
} # with_color

sub _ltsv_escape ($) {
  if ($EscapeMethod eq 'perl') {
    my $v = $_[0];
    if ($Colored) {
      $v =~ s/([^\x20-\x5B\x5D-\x7E])/with_color 'bright_magenta', (ord $1 > 0xFF ? sprintf '\x{%04X}', ord $1 : sprintf '\x%02X', ord $1)/ge;
    } else {
      $v =~ s/([^\x20-\x5B\x5D-\x7E])/ord $1 > 0xFF ? sprintf '\x{%04X}', ord $1 : sprintf '\x%02X', ord $1/ge;
    }
    return $v;
  } else { # asis
    return $_[0];
  }
}

sub carp ($@) {
  my $location = shift->{source_location};
  $location =~ s/^\s*at\s*//;
  my $line = 0;
  if ($location =~ s/\s*line\s*(\d+)\.?\s*$//) {
    $line = $1;
  }
  my $v = '';
  if ($Colored) {
    $v .= join '', map { s/((?:\t|^)[^:]+)/with_color 'green', $1/ge; $_ } @_;
    $v .= Term::ANSIColor::color ('white');
  } else {
    $v .= join '', @_;
  }
  $v .= join '',
      "\tcaller_file_name:" . _ltsv_escape $location,
      "\tcaller_line:$line";
  $v .= Term::ANSIColor::color ('reset') if $Colored;
  $v .= "\n";
  $STDERR->push_write ($v);
} # carp

my $StatementIDToSQL = {};

sub show_result ($$) {
  my ($self, $result) = @_;
  return if $self->{result_shown}++;
  if ($self->{action_type} eq 'query') {
    $SQLCount++ if $COUNT;
    $self->carp (sprintf "runtime:%.2f\tsql:%s\tsql_binds:%s\trows:%d",
                 $self->{end_time} - $self->{start_time},
                 _ltsv_escape $self->{query},
                 _ltsv_escape '',
                 (defined $result && defined $result->packet ? $result->packet->{affected_rows} || 0 : 0))
        if $WARN;
  } elsif ($self->{action_type} eq 'statement_prepare') {
    if (defined $result and defined $result->packet) {
      $StatementIDToSQL->{$self->{object}, $result->packet->{statement_id} // ''} = $self->{query};
    }
  } elsif ($self->{action_type} eq 'statement_execute') {
    $SQLCount++ if $COUNT;
    $self->carp (sprintf "runtime:%.2f\tsql:%s\tsql_binds:%s\trows:%d",
                 $self->{end_time} - $self->{start_time},
                 _ltsv_escape ($StatementIDToSQL->{$self->{object}, $self->{statement_id}} // ''),
                 _ltsv_escape '('.(join ', ', map { $_->{value} // '(undef)' } @{$self->{params}}).')',
                 (defined $result && defined $result->packet ? $result->packet->{affected_rows} || 0 : 0))
        if $WARN;
  } elsif ($self->{action_type} eq 'connect') {
    if ($WARN) {
      my $dsn = sprintf 'DBI:mysql:host=%s;port=%s;_use_tls=%d;username=%s;password=%s;dbname=%s',
          $self->{hostname} // '',
          $self->{port} // '',
          $self->{tls} ? 1 : 0,
          $self->{username} // '',
          $self->{password} // '',
          $self->{database};
      $self->carp (with_color 'bright_black',
                   sprintf "runtime:%.2f\tdsn:%s\toperation_class:%s\toperation_method:%s",
                   $self->{end_time} - $self->{start_time},
                   _ltsv_escape $dsn,
                   $self->{class},
                   $self->{action_type});
    }
  }
} # show_result

sub DESTROY {
  $_[0]->show_result;
}

1;

=head1 NAME

AnyEvent::MySQL::Client::ShowLog - Show SQL logs for debugging

=head1 SYNOPSIS

  $ perl -MAnyEvent::MySQL::Client::ShowLog app.pl

=head1 DESCRIPTION

The C<AnyEvent::MySQL::Client::ShowLog> module, when C<use>d, outputs
C<connect> and SQL execution logs of L<AnyEvent::MySQL::Client> to the
standard error output in the LTSV format.

=head1 SEE ALSO

L<AnyEvent::MySQL::Client>.

Labeled Tab-separated Values (LTSV) <http://ltsv.org/>.

=head1 AUTHOR

Wakaba <wakaba@suikawiki.org>.

=head1 ACKNOWLEDGEMENTS

This module derived from L<DBIx::ShowSQL>
<https://github.com/wakaba/perl-rdb-utils/blob/master/lib/DBIx/ShowSQL.pod>,
which is inspired by L<DBIx::MoCo>'s debugging functions and
L<Devel::KYTProf>.

=head1 LICENSE

Public Domain.

=cut
