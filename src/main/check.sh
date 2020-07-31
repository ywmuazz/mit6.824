cat mr-out-* | sort > ./stdans/my
diff ./stdans/ans ./stdans
echo "check done."