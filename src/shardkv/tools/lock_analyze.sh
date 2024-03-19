logfile=$1
echo "Mutex usage analysis for $logfile" 
sed -n '1,$p' $logfile | grep -e ".*released.* the lock" > lock_release.log
sed -n '1,$p' $logfile | grep -e ".*acquired.* the lock" > lock_acquire.log
python3 merge.py
python3 analyze.py