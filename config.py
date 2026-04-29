import os
from dotenv import load_dotenv

load_dotenv()


class KafkaConfig:
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    TOPIC = os.getenv("KAFKA_TOPIC", "your-topic-name")
    GROUP_ID = os.getenv("KAFKA_GROUP_ID", "message-parsing-group")
    AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "")
    SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
    SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")

    @classmethod
    def to_dict(cls) -> dict:
        conf = {
            "bootstrap.servers": cls.BOOTSTRAP_SERVERS,
            "group.id": cls.GROUP_ID,
            "auto.offset.reset": cls.AUTO_OFFSET_RESET,
            "security.protocol": cls.SECURITY_PROTOCOL,
        }
        if cls.SASL_MECHANISM:
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
