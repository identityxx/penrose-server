#!/usr/bin/perl

if ($#ARGV == 0) {
    $name = $ARGV[0];
} else {
    $name = "tcp";
}

print "Calling getprotobyname(\"$name\")...\n\n";

($name, $aliases, $protocol) = getprotobyname($name);

print "Name     : $name\n";
print "Aliases  : $aliases\n";
print "Protocol : $protocol\n";
