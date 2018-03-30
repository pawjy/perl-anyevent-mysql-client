package Tests;
use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t_deps/modules/*/lib');
use Carp;
use Promised::Mysqld;
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

my $Mysqld;
my $DBNumber = 1;

push @EXPORT, qw(test_dsn);
sub test_dsn ($) {
  my $name = shift;
  $name .= '_' . $DBNumber . '_test';
  my $dsn = $Mysqld->get_dsn_string (dbname => $name);
  $Mysqld->create_db_and_execute_sqls ($name, [])->to_cv->recv;
  return $dsn;
} # test_dsn

push @EXPORT, qw(RUN);
sub RUN (;$$) {
  my $init = shift || sub { };
  my $args = shift || {};
  
  $Mysqld = Promised::Mysqld->new;

  for (keys %$args) {
    $Mysqld->my_cnf->{$_} = $args->{$_};
  }
  
  note "Start mysqld...";
  $Mysqld->start->to_cv->recv;
  note "Mysqld started";

  $init->();

  run_tests;

  note "Stopping...";
  $Mysqld->stop->to_cv->recv;
  undef $Mysqld;
} # RUN

1;

=head1 LICENSE

Copyright 2018 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
