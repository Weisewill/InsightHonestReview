rm -r /usr/local/spark/work/*
spark-submit cleanData.py --start 2010 --end 2010 --type comments
rm -r /usr/local/spark/work/*
spark-submit cleanData.py --start 2011 --end 2011 --type comments
rm -r /usr/local/spark/work/*
spark-submit cleanData.py --start 2012 --end 2012 --type comments

