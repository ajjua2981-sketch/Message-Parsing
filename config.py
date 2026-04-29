import os
from dotenv import load_dotenv

load_dotenv()


class KafkaConfig:
    BOOTSTRAP_SERVERS = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "ch3dr1017045.express-scripts.com:9092,"
        "ch3dr1017046.express-scripts.com:9092,"
        "ch3dr1017047.express-scripts.com:9092,"
        "ch3dr1017048.express-scripts.com:9092,"
        "ch3dr1017049.express-scripts.com:9092,"
        "ch3dr1017050.express-scripts.com:9092",
    )
    TOPIC = os.getenv("KAFKA_TOPIC", "your-topic-name")
    GROUP_ID = os.getenv("KAFKA_GROUP_ID", "message-parsing-group")
    AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

    # SASL_SSL: Kerberos auth over SSL (on-prem cluster requirement)
    SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "GSSAPI")
    SASL_KERBEROS_SERVICE_NAME = os.getenv("KAFKA_SASL_KERBEROS_SERVICE_NAME", "cdkafka")
    SASL_KERBEROS_PRINCIPAL = os.getenv("KAFKA_SASL_KERBEROS_PRINCIPAL", "")

    # PEM certificate — resolves to resources/kafka/<env>/common.pem by default
    _env = os.getenv("APP_ENV", "dev")
    SSL_CA_LOCATION = os.getenv("KAFKA_SSL_CA_LOCATION", f"resources/kafka/{_env}/common.pem")

    # On-prem tuning
    SESSION_TIMEOUT_MS = int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000"))
    HEARTBEAT_INTERVAL_MS = int(os.getenv("KAFKA_HEARTBEAT_INTERVAL_MS", "10000"))
    MAX_POLL_INTERVAL_MS = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "300000"))

    @classmethod
    def to_dict(cls) -> dict:
        conf = {
            "bootstrap.servers":     cls.BOOTSTRAP_SERVERS,
            "group.id":              cls.GROUP_ID,
            "auto.offset.reset":     cls.AUTO_OFFSET_RESET,
            "security.protocol":     cls.SECURITY_PROTOCOL,
            "sasl.mechanism":        cls.SASL_MECHANISM,
            "sasl.kerberos.service.name": cls.SASL_KERBEROS_SERVICE_NAME,
            "ssl.ca.location":       cls.SSL_CA_LOCATION,
            "session.timeout.ms":    cls.SESSION_TIMEOUT_MS,
            "heartbeat.interval.ms": cls.HEARTBEAT_INTERVAL_MS,
            "max.poll.interval.ms":  cls.MAX_POLL_INTERVAL_MS,
        }
        if cls.SASL_KERBEROS_PRINCIPAL:
            conf["sasl.kerberos.principal"] = cls.SASL_KERBEROS_PRINCIPAL
        return conf


class OracleConfig:
    USER = os.getenv("ORACLE_USER", "your_db_user")
    PASSWORD = os.getenv("ORACLE_PASSWORD", "your_db_password")
    DSN = os.getenv("ORACLE_DSN", "localhost:1521/ORCL")
    TABLE = os.getenv("ORACLE_TABLE", "YOUR_TABLE_NAME")


class AppConfig:
    # Dot-notation path to the reference ID in the parsed XML dict.
    # Example: "Envelope.Header.ReferenceID" for:
    #   <Envelope><Header><ReferenceID>ABC123</ReferenceID></Header></Envelope>
    REFERENCE_ID_PATH = os.getenv("REFERENCE_ID_PATH", "ns2:Envelope.ns2:body.PAReferenceId")
