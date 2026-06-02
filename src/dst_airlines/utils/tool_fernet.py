from cryptography.fernet import Fernet
from dotenv import load_dotenv
import os


def encrypt_credentials() -> None:
    """
    Interactive utility to encrypt API credentials.

    Usage:
        python tool_fernet.py

    Requirements:
        - ENCRYPTION_KEY must exist in .env
        - Generate ENCRYPTION_KEY with:
          python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    """

    load_dotenv(".env.docker")

    encryption_key = os.getenv("ENCRYPTION_KEY")

    if not encryption_key:
        raise RuntimeError("Missing ENCRYPTION_KEY in .env")

    try:
        cipher = Fernet(encryption_key.strip().encode("utf-8"))
    except Exception as exc:
        raise RuntimeError(
            "ENCRYPTION_KEY is not a valid Fernet key.\n"
            "Generate one with:\n"
            "python -c \"from cryptography.fernet import Fernet; "
            "print(Fernet.generate_key().decode())\""
        ) from exc

    plain_token = input("Token: ").strip()

    if not plain_token:
        raise RuntimeError("Token cannot be empty")

    encrypted_token = cipher.encrypt(plain_token.encode("utf-8")).decode("utf-8")

    print("\nEncrypted Token:")
    print(encrypted_token)

    print("\nCopy the value above into your .env as:")
    print("TOKEN_AVIATIONSTACK=<encrypted_token>")


if __name__ == "__main__":
    encrypt_credentials()