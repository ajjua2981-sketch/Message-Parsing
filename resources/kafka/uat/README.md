# UAT Kafka Keytab

Place the UAT environment keytab file here:

    resources/kafka/uat/uat.keytab

Set in .env:

    APP_ENV=uat
    KEYTAB_FILE=resources/kafka/uat/uat.keytab
    KAFKA_SASL_KERBEROS_PRINCIPAL=svc-account-uat@EXPRESS-SCRIPTS.COM
