if [ -z $KYLIN_HOME ];
then
    export KYLIN_HOME=`cd -P -- "$(dirname -- "$0")" && dirname $(pwd)`
fi
