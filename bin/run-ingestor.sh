#!/bin/bash -x
DIR=$( cd ""$( dirname ""${BASH_SOURCE[0]}"" )"" && pwd )
DATA_DIR=${DIR}/../../data
MODELS_DIR=${DIR}/../../models
STOCKTWITS_SENTIMENT_MODEL_PATH=${MODELS_DIR}/models/fastai_dl2_p2_l10_stocktwits_transfer_learning_sentiment_classification_models_20180829/models/68714clas_2.h5
STOCKTWITS_SENTIMENT_ITOS_MODEL_PATH=${MODELS_DIR}/models/fastai_dl2_p2_l10_stocktwits_transfer_learning_sentiment_classification_models_20180829/stocktwits_posts_lm/tmp/itos.pkl


UNIX_TIMESTAMP=`date +%s`
STDERR_FILE=kcl_ingestor_paperspace_${UNIX_TIMESTAMP}_epoch_tsla.stderr
STDERR_DB_INGESTOR_FILE=db_ingestor_paperspace_${UNIX_TIMESTAMP}_epoch_tsla.stderr
STDERR_SENTIMENT_INFERENCE_FILE=sentiment_inference_paperspace_${UNIX_TIMESTAMP}_epoch_tsla.stderr

export KCL_NODE=dev
export KCL_APPLICATION=automlpredictor-tesla-test-${KCL_NODE}
export KCL_PATH=~/teslamonitor/streams
export KCL_PROPERTIES_FILE=${KCL_NODE}.properties
export KCL_OUTPUT_FILE=${DATA_DIR}/kinesis_${KCL_APPLICATION}_${UNIX_TIMESTAMP}.json

source /root/setup_env.sh

pkill python3.6

cd $DIR/../streams
nohup sh -c "/usr/bin/java -cp /home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/amazon-kinesis-client-1.9.0.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/commons-codec-1.9.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/guava-18.0.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/jmespath-java-1.11.272.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/jackson-dataformat-cbor-2.6.7.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/aws-java-sdk-dynamodb-1.11.272.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/jackson-core-2.6.7.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/jackson-annotations-2.6.0.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/aws-java-sdk-s3-1.11.272.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/aws-java-sdk-kms-1.11.272.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/aws-java-sdk-kinesis-1.11.272.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/commons-lang-2.6.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/aws-java-sdk-core-1.11.272.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/ion-java-1.0.2.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/jackson-databind-2.6.7.1.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/httpcore-4.4.4.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/aws-java-sdk-cloudwatch-1.11.272.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/httpclient-4.5.2.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/commons-logging-1.1.3.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/joda-time-2.8.1.jar:/home/paperspace/anaconda3/lib/python3.6/site-packages/amazon_kclpy/jars/protobuf-java-2.6.1.jar:/home/paperspace/teslamonitor/streams com.amazonaws.services.kinesis.multilang.MultiLangDaemon ${KCL_PROPERTIES_FILE}" 2> ${DATA_DIR}/${STDERR_FILE}  &
sleep 45
cd $DIR

cd ${DIR}/../client
nohup sh -c "python3.6 filter_posts_for_predict_cli -idf $KCL_OUTPUT_FILE -ssh | python3.6 predict_cli.py -if $STOCKTWITS_SENTIMENT_ITOS_MODEL_PATH -tcf $STOCKTWITS_SENTIMENT_MODEL_PATH  | python3.6 db_ingestor_cli.py -ssh" 2> ${DATA_DIR}/${STDERR_SENTIMENT_INFERENCE_FILE} &
