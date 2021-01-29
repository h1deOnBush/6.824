#! /bin/zsh
set -e
if [ $# -ne 2 ]; then
	echo "Usage: $0 [test] [repeat time]"
	exit 1
fi
for ((i=0;i<$2;i++))

do
  echo $i
	#go test
	go test -run $1
done
