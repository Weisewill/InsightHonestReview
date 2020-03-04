rm -r /usr/local/spark/work/*
spark-submit cleanData.py --start 2006 --end 2019 --type comments
