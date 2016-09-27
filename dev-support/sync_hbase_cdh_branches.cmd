git checkout master
git pull
git reset apache/master --hard

git checkout apache/1.5.x-HBase1.x
git format-patch -1
git checkout master
git am -3 --ignore-whitespace 0001-KYLIN-1528-Create-a-branch-for-v1.5-with-HBase-1.x-A.patch
git push apache master:1.5.x-HBase1.x -f
rm 0001-KYLIN-1528-Create-a-branch-for-v1.5-with-HBase-1.x-A.patch

git checkout apache/1.5.x-CDH5.7
git format-patch -1
git checkout master
git am -3 --ignore-whitespace 0001-KYLIN-1672-support-kylin-on-cdh-5.7.patch
git push apache master:1.5.x-CDH5.7 -f
rm 0001-KYLIN-1672-support-kylin-on-cdh-5.7.patch

git reset apache/master --hard
