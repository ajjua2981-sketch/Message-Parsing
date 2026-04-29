# PROD Kafka Resources

Place these files here (all are gitignored):

| File                        | Purpose                              |
|-----------------------------|--------------------------------------|
| `DKFKpocepa.keytab`         | Kerberos keytab for authentication   |
| `krb5.conf`                 | Kerberos config (KDC address etc.)   |
| `common.pem`                | SSL CA certificate                   |

NOT needed for Python (Java only):
- `jaas.conf`
- `devpkafka.server.truststore.jks`

Set in .env:

    APP_ENV=prod
    KAFKA_SASL_KERBEROS_PRINCIPAL=<your-principal>@EXPRESS-SCRIPTS.COM
