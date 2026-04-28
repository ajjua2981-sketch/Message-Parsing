def parse_message(message: str) -> dict:
    return {"raw": message}


if __name__ == "__main__":
    sample = "Hello, World!"
    result = parse_message(sample)
    print(result)
