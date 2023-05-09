# Data Ingestion

## Snippet of Dataset

The Twitter dataset is composed of four distinct parts, each providing a unique perspective on the tweets gathered. It can be downloaded from [github](https://github.com/thepanacealab/covid19_twitter). I attached my code [RBDA_Project_Twitter_dataset.ipynb](https://colab.research.google.com/drive/1R4nv_b3UR_ak_k2yWHjW_DtZnpgJO5n_?usp=share_link).

### Twitter raw data

The first part of the dataset is the raw data (119M), which spans from March 22nd, 2020 to December 31st, 2022. However, it should be noted that only tweets from July 20th, 2020 onwards include the language column.

Comprising 5 columns, the Twitter dataset offers a comprehensive view of each tweet. The first column is the unique Twitter ID, which serves as a reference to the corresponding tweet using Twitter Dev Tools. However, the approval for access to this tool has not yet been granted. The second and third columns indicate the date and time of the tweet, respectively. The fourth column denotes the language used in the tweet, while the last column displays the country code. It should be noted, however, that the majority of the entries in this column are NaN values.

| tweet_id            | date       | time     | lang | country_code |
| ------------------- | ---------- | -------- | ---- | ------------ |
| 1395954198999621633 | 2021-05-22 | 04:06:31 | en   | NaN          |
| 1396315437017833472 | 2021-05-23 | 04:01:57 | en   | NaN          |
| 1396315419518980099 | 2021-05-23 | 04:01:52 | en   | NaN          |

### n-gram data

The single, bi, and tri-gram data (1m each) are statistical results derived from the Twitter raw data. These datasets provide a count of how often words appeared on a given day, and are organized by the corresponding n-gram (single, bi, or tri-gram). The process of creating these datasets involved splitting the words used in each tweet and tallying the frequency of each word's appearance.

Each dataset is organized into three columns. The first column represents the word, the second column displays the count, and the third column indicates the date.

| Gram        | Counts | date       |
| ----------- | ------ | ---------- |
| coronavirus | 301676 | 2020-05-22 |
| covid       | 241074 | 2020-05-22 |
| 19          | 229764 | 2020-05-22 |

| Gram                 | Counts | date       |
| -------------------- | ------ | ---------- |
| covid 19             | 224879 | 2020-05-22 |
| coronavirus pandemic | 9787   | 2020-05-22 |
| 19 pandemic          | 8519   | 2020-05-22 |

| Gram                | Counts | date       |
| ------------------- | ------ | ---------- |
| covid 19 pandemic   | 8506   | 2020-05-22 |
| covid 19 cases      | 5627   | 2020-05-22 |
| penyebaran covid 19 | 3681   | 2020-05-22 |

## Data Ingestion

Python notebook to download data and transfer it to NYU HDFS. Detail Python code can be found in [RBDA_Project_Twitter_dataset.ipynb](https://colab.research.google.com/drive/1R4nv_b3UR_ak_k2yWHjW_DtZnpgJO5n_?usp=share_link).

```sh
hdfs dfs -put /home/wf2099_nyu_edu/RBDA RBDA
```

And then move them to different folders.

```sh
hdfs dfs -mkdir RBDAP_RAW
hdfs dfs -mv RBDA/raw_data_en.csv RBDAP_RAW/

wf2099_nyu_edu@nyu-dataproc-m:~$ hdfs dfs -mkdir RBDAP_Single
wf2099_nyu_edu@nyu-dataproc-m:~$ hdfs dfs -mkdir RBDAP_Bi
wf2099_nyu_edu@nyu-dataproc-m:~$ hdfs dfs -mkdir RBDAP_Tri
wf2099_nyu_edu@nyu-dataproc-m:~$ hdfs dfs -mv RBDA/terms_data.csv RBDAP_Single/
wf2099_nyu_edu@nyu-dataproc-m:~$ hdfs dfs -mv RBDA/bigram_data.csv RBDAP_Bi/
wf2099_nyu_edu@nyu-dataproc-m:~$ hdfs dfs -mv RBDA/trigram_data.csv RBDAP_Tri/
```

## Data Cleaning

When downloading the data from internet. I make a simple filter in python code, filtering language = en.

Using Trino to have an overview about the twitter raw data.

### Create hive table

Connect to hive.

```sh
beeline -u jdbc:hive2://localhost:10000

use wf2099_nyu_edu;
```

Create table for twitter raw data.

```sh
create external table twitter_raw
(idx bigint, twitter_id bigint, post_date string, post_time string, lang string, country_code string)
row format delimited fields terminated by ','
location '/user/wf2099_nyu_edu/RBDAP_RAW/';
```

Create table for twitter Singlegram data.

```sh
create external table singlegram
(idx bigint, gram string, counts bigint, post_date string)
row format delimited fields terminated by ','
location '/user/wf2099_nyu_edu/RBDAP_Single/';
```

Create table for twitter Bigram data.

```sh
create external table bigram
(idx bigint, gram string, counts bigint, post_date string)
row format delimited fields terminated by ','
location '/user/wf2099_nyu_edu/RBDAP_Bi/';
```

Create table for twitter Trigram data.

```sh
create external table Trigram
(idx bigint, gram string, counts bigint, post_date string)
row format delimited fields terminated by ','
location '/user/wf2099_nyu_edu/RBDAP_Tri/';
```

### Use Trino to make some queries

```sh
presto

use hive.wf2099_nyu_edu;

select post_date, count(*) cnt
from twitter_raw
group by post_date
order by cnt asc;

select post_date, count(*) cnt
from singlegram
group by post_date
order by cnt asc;
```

## Data Profiling

I construct 2 critical features for this analysis project.

### Twitter count per day

The first one is the number of English tweets per day. By writing MapReduce code. I can get the number of English tweets per day.

Java Mapreduce code

```java
public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString().toLowerCase();
    String[] tokens = line.split(",");
    if (tokens.length < 3) {
      return;
    }
    String token = tokens[2];
    if (token.length() == 10) { // This is to check the date format
      context.write(new Text(token), one);
    }
  }

      public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
```

Complie the java code.

```sh
cd project/code/

javac -classpath `hadoop classpath` *.java

jar cvf TwitterCount.jar *.class

hadoop jar TwitterCount.jar TwitterCount RBDAP_RAW/raw_data_en.csv RBDAP_RAW/output

hadoop fs -cat RBDAP_RAW/output/part-r-00000
```

And the output is like this.

```sh
2020-07-26      269350
2020-07-27      451040
2020-07-28      491664
2020-07-29      511801
2020-07-30      500214
2020-07-31      463180
```

### Top 100 popular words

The second one is the top 100 popular word during these year. I will find it using Trino.

```sh

presto

use hive.wf2099_nyu_edu;

CREATE TABLE top_100_grams as
select gram, sum(counts) as counts
from singlegram
group by gram
order by counts desc
limit 100;

select * from top_100_grams order by counts desc;

CREATE TABLE top_100_bigrams as
select gram, sum(counts) as counts
from bigram
group by gram
order by counts desc
limit 100;

select * from top_100_bigrams order by counts desc;

CREATE TABLE top_100_trigrams as
select gram, sum(counts) as counts
from trigram
group by gram
order by counts desc
limit 100;

select * from top_100_trigrams order by counts desc;

```

And the date after profiling

| gram        | counts    |
| ----------- | --------- |
| covid       | 105042770 |
| 19          | 93810573  |
| coronavirus | 71903348  |
| covid19     | 71279336  |
| people      | 15441598  |
| amp         | 15246189  |
| cases       | 14801319  |
| new         | 14191565  |
| vaccine     | 12424466  |
| pandemic    | 11654535  |
|             |           |
|             |           |
|             |           |

## Attached files

```jupyter

```
