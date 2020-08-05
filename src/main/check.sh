cat mr-out-* | sort > ./stdans/my
diff ./stdans/ans ./stdans/my
echo "check done."