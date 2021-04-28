#!/bin/bash

function setSimpleDBRoot
{
	local this
	local lls
	local link
	local bin

	this="${BASH_SOURCE-$0}"
	while [ -h "$this" ]; do
	  lls=`ls -ld "$this"`
	  link=`expr "$lls" : '.*-> \(.*\)$'`
	  if expr "$link" : '.*/.*' > /dev/null; then
	    this="$link"
	  else
	    this=`dirname "$this"`/"$link"
	  fi
	done

	# convert relative path to absolute path
	bin=`dirname "$this"`
	script=`basename "$this"`
	bin=`cd "$bin"; pwd`
	#this="$bin/$script"

	echo "$bin/..";
}

function isLinux
{
	test "$(uname)" = "Linux"
}

function isMac
{
	test "$(uname)" = "Darwin"
}

function isCygwin
{
	os="$(uname)"
	test "${os:0:6}" = "CYGWIN"
}

function xEnabled
{
	if isCygwin
	then
		return false
	fi

	(xterm -e "") 2>&1 > /dev/null 
}

#--------------------------------------------init--------------------------------------
SIMPLEDB_ROOT=${SIMPLEDB_ROOT="$(setSimpleDBRoot)"}

osName="$(uname)"
if [ "$osName" = Darwin ] || [ "${osName:0:6}" = CYGWIN ] || [ "$osName" = Linux ]
#if [ "$osName" = Darwin ] ||  [ "$osName" = Linux ]
then
	true
else
	echo "Unsupported OS. Currently only Linux, Mac and Windows with \ 
	Cygwin are supported"
	#echo "Unsupported OS. Currently only Linux and Mac OS \ 
	#are supported"
	exit;
fi

if [ $# -lt 1 ]
then
	echo "Usage: ./startSequential.sh catalogFile [-explain] [-f queryFile]"
	exit 1
fi

catalogFile=$(cd $(dirname $1);pwd)/$(basename $1)
shift
CLASSPATH_SEPARATOR=":"

if isCygwin
then
	catalogFile="$(cygpath --windows $catalogFile)"
	CLASSPATH_SEPARATOR=";"
fi

#outputStyle="X"
#if ! xEnabled
#then
#	outputStyle="T"
#fi

javaOptions=

if isCygwin
then
	javaOptions=-Djline.terminal=jline.UnixTerminal
fi

#--------------------------------------------start server--------------------------------------
cd "$SIMPLEDB_ROOT"
exec java $javaOptions -classpath "bin/src${CLASSPATH_SEPARATOR}lib/*" simpledb.SimpleDb parser $catalogFile $*
