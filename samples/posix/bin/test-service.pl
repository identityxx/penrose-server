#!/usr/bin/perl

if ($#ARGV == 1) {
    $name     = $ARGV[0];
    $protocol = $ARGV[1];
} else {
    $name     = "ftp";
    $protocol = "tcp";
}

print "Calling getservbyname(\"$name\", \"$protocol\")...\n\n";

($name, $aliases, $port, $protocol) = getservbyname($name, $protocol);

print "Name     : $name\n";
print "Aliases  : $aliases\n";
print "Port     : $port\n";
print "Protocol : $protocol\n";
