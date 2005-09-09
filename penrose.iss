; Copyright (c) 2000-2005, Identyx Corporation.
; All rights reserved.

[Setup]

AppName=Penrose
AppVerName=Penrose 0.9.4
DefaultDirName={pf}\Penrose
DefaultGroupName=Penrose
UninstallDisplayName=Penrose
UninstallDisplayIcon={app}\penrose.ico
Compression=zip
SolidCompression=yes
OutputBaseFilename=penrose-0.9.4
OutputDir=dist
LicenseFile=LICENSE.txt

[Files]

Source: "target\dist\*"; DestDir: "{app}"; Components: main
Source: "target\dist\bin\*"; DestDir: "{app}\bin"; Components: main
Source: "target\dist\conf\*"; DestDir: "{app}\conf"; Flags: onlyifdoesntexist uninsneveruninstall; Components: main
Source: "target\dist\conf\default\*"; DestDir: "{app}\conf\default"; Flags: recursesubdirs; Components: main
Source: "target\dist\docs\*"; DestDir: "{app}\docs"; Flags: recursesubdirs; Components: docs
Source: "target\dist\lib\*"; DestDir: "{app}\lib"; Flags: recursesubdirs; Components: main
Source: "target\dist\schema\*"; DestDir: "{app}\schema"; Flags: recursesubdirs; Components: main
Source: "target\dist\samples\*"; DestDir: "{app}\samples"; Flags: recursesubdirs; Components: sample

[Dirs]

Name: "{app}\docs"
Name: "{app}\lib\ext"
Name: "{app}\samples"
Name: "{app}\var"

[Components]

Name: "main"; Description: "Main Files"; Types: full compact custom; Flags: fixed
Name: "docs"; Description: "Documentations"; Types: full
Name: "sample"; Description: "Sample Files"; Types: full

[Icons]

Name: "{group}\Documentation\README.txt"; Filename: "{app}\README.txt"
Name: "{group}\Documentation\LICENSE.txt"; Filename: "{app}\LICENSE.txt"
Name: "{group}\Documentation\COPYING.txt"; Filename: "{app}\COPYING.txt"
Name: "{group}\Documentation\INSTALL-BINARY.txt"; Filename: "{app}\INSTALL-BINARY.txt"
Name: "{group}\Documentation\Penrose API"; Filename: "{app}\docs\javadoc\index.html"; Flags: createonlyiffileexists;
Name: "{group}\Documentation\Online Documentation"; Filename: "{app}\docs\Online Documentation.url";
Name: "{group}\Documentation\Penrose Website"; Filename: "{app}\docs\Penrose Website.url";
Name: "{group}\Documentation\Safehaus Website"; Filename: "{app}\docs\Safehaus Website.url";
Name: "{group}\Penrose Server"; Filename: "{app}\bin\penrose.bat"; IconFilename: "{app}\penrose.ico"; WorkingDir: "{app}"
Name: "{group}\Configuration Files"; Filename: "{app}\conf";
Name: "{group}\Sample Files"; Filename: "{app}\samples";
Name: "{group}\Schema Files"; Filename: "{app}\schema";
Name: "{group}\Install Penrose Service"; Filename: "{app}\bin\install-penrose-service.bat"; IconFilename: "{app}\penrose.ico"; WorkingDir: "{app}"
Name: "{group}\Uninstall Penrose Service"; Filename: "{app}\bin\uninstall-penrose-service.bat"; IconFilename: "{app}\penrose.ico"; WorkingDir: "{app}"
Name: "{group}\Uninstall Penrose"; Filename: "{uninstallexe}"
