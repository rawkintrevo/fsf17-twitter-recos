

## Step 1. Get credentials

Copy `conf/credentials.json.template` to `conf/credentials.json`

Twitter Insights Creds (required for historical tweets)
1. Sign up for free 30-day trial of IBM BlueMix
2. Add Twitter Insights Service
3. Open service, go to "Service credentials" tab.  Click on "view credentials"
4. Copy values to `conf/credentials.json`

Twitter Creds
1. Go to [Twitter Application Management](https://apps.twitter.com/)
2. Sign up if needed.  Create App or use creds from existing App
3. Copy required creds to `conf/credentials.json` file

## Step 2. Download data

1. Open `./get-data/collect_tweets.py`
2. Change `DATA_DIR` on line 8 to your path (lazy hack, I know- open a pr if you don't like it)
3. Run the script interactively to collect tweets.
4. bonus: change value of `q` on line 68 if you want to work on something other than "pizza"

## Step 3. Build CCO matrices with Spark

1. Assuming you have Spark 1.6 and Mahout 0.13.0 installed...
2. And you have set `JAVA_HOME`, `MAHOUT_HOME`, and `SPARK_HOME` environment variables...
3. Open Mahout's Spark Shell with `MAHOUT_HOME/bin/mahout spark-shell`
4. Run script `./build-cco.scala` interactively starting at line 30
5. ^^ Change `baseDir` on line 32 to point to the directory you specified in Step 2, item 2 (the `DATA_DIR` from the python script)

Matrices will be output to the `baseDir`

## Step 4. Run Flink Job

1. Run `mvn clean package` to build this stuff
2. Open Flink WebUI and submit new jar found in `./flink/target/ffsf17.flink-1.0-SNAPSHOT.jar`
3. Application takes one argument- the path to this build- should be same as `DATA_DIR` but one level higher. e.g. /path/to/ffsf17-twitter-recos

## Step 5.

Run the job, look for output in `tweets.txt` in the directory specified in the Flink argument.

## Step 6.

Profit

