; Copyright (c) 2000-2005, Identyx Corporation.
; All rights reserved.

;--------------------------------
;Include Modern UI

  !include "MUI.nsh"

;--------------------------------
;General

  ;Name and file
  Name "Penrose"
  BrandingText " "
  OutFile "dist\penrose-0.9.4-nsis.exe"

  ;Default installation folder
  InstallDir "$PROGRAMFILES\Penrose"

  ;Get installation folder from registry if available
  InstallDirRegKey HKCU "Software\Penrose" ""
  
  ShowInstDetails nevershow
  ShowUnInstDetails nevershow

;--------------------------------
;Variables

  Var STARTMENU_FOLDER

;--------------------------------
;Interface Settings

  !define MUI_ABORTWARNING

;--------------------------------
;Pages

  !insertmacro MUI_PAGE_WELCOME
  !define MUI_LICENSEPAGE_RADIOBUTTONS
  !insertmacro MUI_PAGE_LICENSE "target\dist\LICENSE.txt"
  !insertmacro MUI_PAGE_DIRECTORY
  !insertmacro MUI_PAGE_COMPONENTS

  !define MUI_STARTMENUPAGE_REGISTRY_ROOT "HKCU"
  !define MUI_STARTMENUPAGE_REGISTRY_KEY "Software\Penrose"
  !define MUI_STARTMENUPAGE_REGISTRY_VALUENAME "Start Menu Folder"

  !insertmacro MUI_PAGE_STARTMENU Application $STARTMENU_FOLDER
  !insertmacro MUI_PAGE_INSTFILES

  !insertmacro MUI_UNPAGE_CONFIRM
  !insertmacro MUI_UNPAGE_INSTFILES

;--------------------------------
;Languages

  !insertmacro MUI_LANGUAGE "English"

;--------------------------------
;Installer Sections

Section -Dirs
  CreateDirectory "$INSTDIR\docs"
  CreateDirectory "$INSTDIR\lib\ext"
  CreateDirectory "$INSTDIR\samples"
  CreateDirectory "$INSTDIR\var"
SectionEnd

Section -Files
  SetOutPath "$INSTDIR"
  File /r "target\dist\bin"
  SetOutPath "$INSTDIR\conf"
  SetOverwrite off
  File "target\dist\conf\*"
  SetOutPath "$INSTDIR\conf\default"
  SetOverwrite on
  File /r "target\dist\conf\default"
  SetOutPath "$INSTDIR"
  File /r "target\dist\lib"
  File /r "target\dist\schema"
  File "target\dist\*"
  WriteUninstaller "$INSTDIR\Uninstall.exe"
SectionEnd

Section -Links
  WriteRegStr HKCU "Software\Penrose" "" $INSTDIR

  !insertmacro MUI_STARTMENU_WRITE_BEGIN Application
    CreateDirectory "$SMPROGRAMS\$STARTMENU_FOLDER"
    CreateDirectory "$SMPROGRAMS\$STARTMENU_FOLDER\Documentation"
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\Documentation\README.txt.lnk" "$INSTDIR\README.txt"
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\Documentation\LICENSE.txt.lnk" "$INSTDIR\LICENSE.txt"
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\Documentation\COPYING.txt.lnk" "$INSTDIR\COPYING.txt"
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\Documentation\INSTALL-BINARY.txt.lnk" "$INSTDIR\INSTALL-BINARY.txt"
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\Documentation\Online Documentation.lnk" "$INSTDIR\docs\Online Documentation.url"
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\Documentation\Penrose Website.lnk" "$INSTDIR\docs\Penrose Website.url"
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\Documentation\Safehaus Website.lnk" "$INSTDIR\docs\Safehaus Website.url"
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\Configuration Files.lnk" "$INSTDIR\conf"
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\Penrose Server.lnk" "$INSTDIR\bin\penrose.bat" "" "$INSTDIR\penrose.ico" "" "" "" ""
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\Schema Files.lnk" "$INSTDIR\schema"
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\Uninstall Penrose.lnk" "$INSTDIR\Uninstall.exe"
  !insertmacro MUI_STARTMENU_WRITE_END

SectionEnd

Section "Documentations" DocFiles

  SetOutPath "$INSTDIR"
  File /r "target\dist\docs"

  !insertmacro MUI_STARTMENU_WRITE_BEGIN Application
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\Documentation\Penrose API.lnk" "$INSTDIR\docs\javadoc\index.html"
  !insertmacro MUI_STARTMENU_WRITE_END

SectionEnd

Section "Sample Files" SampleFiles

  SetOutPath "$INSTDIR"
  File /r "target\dist\samples"

  !insertmacro MUI_STARTMENU_WRITE_BEGIN Application
    CreateShortCut "$SMPROGRAMS\$STARTMENU_FOLDER\Sample Files.lnk" "$INSTDIR\samples"
  !insertmacro MUI_STARTMENU_WRITE_END

SectionEnd

;--------------------------------
;Descriptions

  LangString DESC_DocFiles ${LANG_ENGLISH} "Documentations."
  LangString DESC_SampleFiles ${LANG_ENGLISH} "Sample Files."

  !insertmacro MUI_FUNCTION_DESCRIPTION_BEGIN
    !insertmacro MUI_DESCRIPTION_TEXT ${DocFiles} $(DESC_DocFiles)
    !insertmacro MUI_DESCRIPTION_TEXT ${SampleFiles} $(DESC_SampleFiles)
  !insertmacro MUI_FUNCTION_DESCRIPTION_END

;--------------------------------
;Uninstaller Section

Section "Uninstall"

  RMDir /r "$INSTDIR\bin"
  RMDir /r "$INSTDIR\conf\default"
  RMDir /r "$INSTDIR\docs"
  Delete "$INSTDIR\lib\*"
  RMDir /r "$INSTDIR\samples"
  RMDir /r "$INSTDIR\schema"
  Delete "$INSTDIR\*"

  !insertmacro MUI_STARTMENU_GETFOLDER Application $STARTMENU_FOLDER

  RMDir /r "$SMPROGRAMS\$STARTMENU_FOLDER"

  DeleteRegKey /ifempty HKCU "Software\Penrose"

SectionEnd