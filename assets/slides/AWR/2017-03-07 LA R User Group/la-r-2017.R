## #############################################################################
## write records
## #############################################################################

library(rJava)
.jcall("java/lang/System", "S", "setProperty", "aws.profile", "personal")

library(AWR.Kinesis); library(jsonlite); library(futile.logger); library(nycflights13)
while (TRUE) {

    flight <- flights[sample(1:nrow(flights), 1), ]

    res <- kinesis_put_record('test-AWR', region = 'us-east-1', data = toJSON(flight),
                              partitionKey = flight$dest)
    flog.info(paste('Pushed a new flight to Kinesis:', res$sequenceNumber))

}

## #############################################################################
## KMS
## #############################################################################

kms_encrypt('alias/AWR', 'foobar')

kms_decrypt('AQECAHiiz4GEPFQLL9AA0N5TY/lDR5euQQScpXQU9iYTn+u3zAAAAGQwYgYJKoZIhvcNAQcG\noFUwUwIBADBOBgkqhkiG9w0BBwEwHgYJYIZIAWUDBAEuMBEEDGdpJK5U8HHQ0P+CUgIBEIAh\ncEH6sbXfAFkPRugwG0zCN2aJxJ1B5sckI0wq+l4rkP09')

kms_generate_data_key('alias/AWR')

## #############################################################################
## get a records
## #############################################################################

library(rJava)
.jcall("java/lang/System", "S", "setProperty", "aws.profile", "personal")

library(AWR.Kinesis)
library(jsonlite)
library(futile.logger)

## get an iterator
sir <- .jnew('com.amazonaws.services.kinesis.model.GetShardIteratorRequest')
sir$setStreamName('test-AWR')
sir$setShardId(.jnew('java/lang/String', '0'))
sir$setShardIteratorType('TRIM_HORIZON')
kc <- .jnew('com.amazonaws.services.kinesis.AmazonKinesisClient')
kc$setEndpoint('kinesis.us-east-1.amazonaws.com')
iterator <- kc$getShardIterator(sir)$getShardIterator()

## get records
gir <- .jnew('com.amazonaws.services.kinesis.model.GetRecordsRequest')
gir$setShardIterator(iterator)
records <- kc$getRecords(gir)$getRecords()

## transform to string
json <- sapply(records, function(x)
    rawToChar(x$getData()$array()))

json[1]
fromJSON(json[1])
rbindlist(lapply(json, fromJSON))

## #############################################################################
## Kinesis Consumer on localhost
## #############################################################################

export AWS_PROFILE=personal
/usr/bin/java -cp \
"/usr/local/lib/R/site-library/AWR/java/*:/usr/local/lib/R/site-library/AWR.Kinesis/java/*:./" \
com.amazonaws.services.kinesis.multilang.MultiLangDaemon ./app.properties

## #############################################################################
## Kinesis Consumer in ECS
## #############################################################################

ssh -i ~/.ssh/aws-daroczig-from-CARDcom.pem ec2-user@ec2-52-206-229-28.compute-1.amazonaws.com
screen -RRd main
docker ps
less +F logger.log

## #############################################################################
## resharding
## #############################################################################

pgrep app.R
## split shard on UI
## putrecords failed
## check logger.log
## rerun pgrep
## restart putrecords

library(rredis)
redisConnect(host = 'pub-redis-14778.us-west-2-1.1.ec2.garantiadata.com', port = 14778)
flights <- redisKeys('flight:*')
flights <- redisMGet(flights)
flights <- data.table(faa = sub('^flight:', '', names(flights)), N = as.numeric(flights))
setorder(flights, 'N')
for (i in flights[, .I]) {
    flog.debug(paste('Found', flights[i, N], flights[i, faa]))
}
