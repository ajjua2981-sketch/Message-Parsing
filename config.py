import os
from dotenv import load_dotenv

load_dotenv()


class KafkaConfig:
    # Comma-separated list of on-prem broker addresses
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092,broker3:9092")
    TOPIC = os.getenv("KAFKA_TOPIC", "your-topic-name")
    GROUP_ID = os.getenv("KAFKA_GROUP_ID", "message-parsing-group")
    AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

    # On-prem auth: PLAINTEXT | SASL_PLAINTEXT | SASL_SSL
    SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")

    # Kerberos (GSSAPI) — set if the cluster uses Kerberos auth
    SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "")
    SASL_KERBEROS_SERVICE_NAME = os.getenv("KAFKA_SASL_KERBEROS_SERVICE_NAME", "kafka")
    SASL_KERBEROS_PRINCIPAL = os.getenv("KAFKA_SASL_KERBEROS_PRINCIPAL", "")

    # LDAP / PLAIN — set if the cluster uses username/password auth
    SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
    SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")

    # On-prem tuning
    SESSION_TIMEOUT_MS = int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000"))
    HEARTBEAT_INTERVAL_MS = int(os.getenv("KAFKA_HEARTBEAT_INTERVAL_MS", "10000"))
    MAX_POLL_INTERVAL_MS = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "300000"))

    @classmethod
    def to_dict(cls) -> dict:
        conf = {
            "bootstrap.servers":    cls.BOOTSTRAP_SERVERS,
            "group.id":             cls.GROUP_ID,
            "auto.offset.reset":    cls.AUTO_OFFSET_RESET,
            "security.protocol":    cls.SECURITY_PROTOCOL,
            "session.timeout.ms":   cls.SESSION_TIMEOUT_MS,
            "heartbeat.interval.ms": cls.HEARTBEAT_INTERVAL_MS,
            "max.poll.interval.ms": cls.MAX_POLL_INTERVAL_MS,
        }

        if cls.SASL_MECHANISM == "GSSAPI":
            conf["sasl.mechanism"] = "GSSAPI"
            conf["sasl.kerberos.service.name"] = cls.SASL_KERBEROS_SERVICE_NAME
            if cls.SASL_KERBEROS_PRINCIPAL:
                conf["sasl.kerberos.principal"] = cls.SASL_KERBEROS_PRINCIPAL

        elif cls.SASL_MECHANISM in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
            conf["sasl.mechanism"] = cls.SASL_MECHANISM
            conf["sasl.username"] = cls.SASL_USERNAME
            conf["sasl.password"] = cls.SASL_PASSWORD

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
