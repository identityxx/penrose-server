#!/usr/bin/perl

if ($#ARGV == 0) {
    $name = $ARGV[0];
} else {
    $name = `/usr/bin/whoami`;
    chomp($name);
}

print "Calling getgrnam(\"$name\")...\n\n";

($name, $password, $gid, $members) = getgrnam($name);

print "Name     : $name\n";
print "Password : $password\n";
print "GID      : $gid\n";
print "Members  : $members\n";
