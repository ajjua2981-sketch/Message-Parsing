# PROD Kafka Keytab

Place the PROD environment keytab file here:

    resources/kafka/prod/prod.keytab

Set in .env:

    APP_ENV=prod
    KEYTAB_FILE=resources/kafka/prod/prod.keytab
    KAFKA_SASL_KERBEROS_PRINCIPAL=svc-account-prod@EXPRESS-SCRIPTS.COM
