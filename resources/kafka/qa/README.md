# QA Kafka Keytab

Place the QA environment keytab file here:

    resources/kafka/qa/qa.keytab

Set in .env:

    APP_ENV=qa
    KEYTAB_FILE=resources/kafka/qa/qa.keytab
    KAFKA_SASL_KERBEROS_PRINCIPAL=svc-account-qa@EXPRESS-SCRIPTS.COM
