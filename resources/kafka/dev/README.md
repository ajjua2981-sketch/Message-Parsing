# DEV Kafka Keytab

Place the DEV environment keytab file here:

    resources/kafka/dev/dev.keytab

Set in .env:

    APP_ENV=dev
    KEYTAB_FILE=resources/kafka/dev/dev.keytab
    KAFKA_SASL_KERBEROS_PRINCIPAL=svc-account-dev@EXPRESS-SCRIPTS.COM
