package Test::Certificates;
use strict;
use warnings;
use Path::Tiny;
use Promise;
use Web::Transport::PKI::Generator;
use Web::Transport::PKI::Parser;

my $root_path = path (__FILE__)->parent->parent->parent->parent->absolute;
my $cert_path = $root_path->child ('local/cert');
my $cn = $ENV{SERVER_HOST_NAME} || 'hoge.test';
$cert_path->mkpath;
my $DUMP = $ENV{DUMP} || $ENV{PROMISED_COMMAND_DEBUG};
my $RSA = 1;

sub ca_path ($$) {
  return $cert_path->child ("ca-" . $_[1]);
} # ca_path

sub escape ($) {
  my $s = $_[0];
  $s =~ s/([^0-9a-z])/sprintf '_%02X', ord $1/ge;
  return $s;
} # escape

sub cert_path ($$;$) {
  my (undef, undef, $cert_args) = @_;
  return $cert_path->child (escape ($cert_args->{host} || $cn) . '-'
      . ($cert_args->{no_san} ? 'nosan-' : '')
      . ($cert_args->{must_staple} ? 'muststaple-' : '')
      . (defined $cert_args->{cn} ? 'cn-' . (escape $cert_args->{cn}) . '-' : '')
      . (defined $cert_args->{cn2} ? 'cn2-' . (escape $cert_args->{cn2}) . '-' : '')
      . $_[1]);
} # cert_path

sub cert_name ($) {
  return $cn;
} # cert_name

sub x ($) {
  warn "\$ $_[0]\n" if $DUMP;
  system ($_[0]) == 0 or die "|$_[0]| failed: $?";
} # x

sub generate_ca_cert_p ($) {
  my $class = $_[0];
  my $ca_key_path = $class->ca_path ('key.pem');
  unless ($ca_key_path->is_file) {
    my $ca_cert_path = $class->ca_path ('cert.pem');
    my $gen = Web::Transport::PKI::Generator->new;
    my $p = ($RSA ? $gen->create_rsa_key->then (sub { [rsa => $_[0]] }) : $gen->create_ec_key->then (sub { [ec => $_[0]] }))->then (sub {
      my ($type, $key) = @{$_[0]};
      my $ca_name = {CN => "ca.test"};
      $ca_key_path->spew ($key->to_pem);
      return $gen->create_certificate (
        subject => $ca_name,
        issuer => $ca_name,
        ca => 1,
        not_before => time - 3600,
        not_after => time + 3600*24*366*100,
        serial_number => 1,
        $type => $key,
        "ca_" . $type => $key,
      );
    })->then (sub {
      my $cert = $_[0];
      $ca_cert_path->spew ($cert->to_pem);
    });
    return $p;
  }
  return Promise->resolve;
} # generate_ca_cert_p

sub generate_certs ($$) {
  my ($class, $cert_args) = @_;

  my $lock_path = $cert_args->{intermediate} ? $class->ca_path ('lock') : $class->cert_path ('lock', {host => 'intermediate'});
  my $lock = $lock_path->openw ({locked => 1});

  warn "\n\n";
  warn "======================\n";
  warn "$$: @{[scalar gmtime]}: Generating certificate (@{[$class->cert_path ('', $cert_args)]})...\n";
  
  return $class->generate_ca_cert_p->then (sub {
    my $ica_key_path = $cert_args->{intermediate} ? $class->ca_path ('key.pem') : $class->cert_path ('key.pem', {host => 'intermediate'});
    my $ca_cert_path = $class->ca_path ('cert.pem');
    my $ica_cert_path = $cert_args->{intermediate} ? $ca_cert_path : $class->cert_path ('cert.pem', {host => 'intermediate'});
    my $chained_ca_cert_path = $cert_args->{intermediate} ? $ca_cert_path : $class->cert_path ('cert-chained.pem', {host => 'intermediate'});

  my $ca_name = {CN => "ca.test"};
  my $ica_subj = $cert_args->{intermediate} ? {CN => 'intermediate'} : $ca_name;
  my $subject_name = $cert_args->{host} || $cn;
  my $server_subj = {CN => (defined $cert_args->{cn} ? $cert_args->{cn} : $subject_name)};
  $server_subj->{"2.5.4.3"} = $cert_args->{cn2} if defined $cert_args->{cn2};

    my $gen = Web::Transport::PKI::Generator->new;
    my $parser = Web::Transport::PKI::Parser->new;
    my $server_cert_path = $class->cert_path ('cert.pem', $cert_args);
    my $chained_cert_path = $class->cert_path ('cert-chained.pem', $cert_args);
    
    my $server_key_path = $class->cert_path ('key.pem', $cert_args);
    my $p = ($RSA ? $gen->create_rsa_key->then (sub { [rsa => $_[0]] }) : $gen->create_ec_key->then (sub { [ec => $_[0]] }))->then (sub {
      my ($type, $key) = @{$_[0]};
      $server_key_path->spew ($key->to_pem);
      return $gen->create_certificate (
      #issuer => $ica_subj,
      subject => $server_subj,
      ($cert_args->{no_san} ? () : (san_hosts => [$subject_name])),
      ca => $cert_args->{intermediate},
      ee => ! $cert_args->{intermediate},
      must_staple => $cert_args->{must_staple},
      not_before => time - 3600,
      not_after => time + 3600*24*100,
      serial_number => int rand 10000000,
      'ca_' . $type => $parser->parse_pem ($ica_key_path->slurp)->[0],
      ca_cert => $parser->parse_pem ($ica_cert_path->slurp)->[0],
      $type => $key,
    );
  })->then (sub {
    my $cert = $_[0];
    $server_cert_path->spew ($cert->to_pem);
    x "cat \Q$server_cert_path\E \Q$ica_cert_path\E > \Q$chained_cert_path\E";
    x "cat \Q$ca_cert_path\E >> \Q$chained_cert_path\E";
    });
    return $p;
  })->then (sub {
    warn "$$: @{[scalar gmtime]}: Certificate generation done\n";

    undef $lock;
  });
} # generate_certs

my $Recreate = $ENV{RECREATE_CERTS};
sub wait_create_cert_p ($$) {
  my ($class, $cert_args) = @_;
  if ($Recreate or
      ($_[0]->ca_path ('cert.pem')->is_file and
       $_[0]->ca_path ('cert.pem')->stat->mtime + 60*60*24 < time)) {
    warn "Recreate certificates...\n";
    x "rm \Q$cert_path\E/*.pem || true";
    $Recreate = 0;
  }
  my $cert_pem_path = $class->cert_path ('cert.pem', $cert_args);
  return Promise->resolve->then (sub {
    unless ($cert_pem_path->is_file) {
      return Promise->resolve->then (sub {
        return $class->generate_certs ({host => 'intermediate', intermediate => 1})
            unless $class->cert_path ('cert.pem', {host => 'intermediate'})->is_file;
      })->then (sub {
        return $class->generate_certs ($cert_args);
      });
    }
  });
} # wait_create_cert_p

1;

=head1 LICENSE

Copyright 2007-2024 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
