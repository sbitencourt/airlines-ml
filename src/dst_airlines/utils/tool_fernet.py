from cryptography.fernet import Fernet
from dotenv import load_dotenv
import os


def encrypt_credentials():
    """
    Interactive utility to encrypt API credentials.
    Run manually from command line.
    """

    load_dotenv()  # ensure .env is loaded

    key = os.getenv("ENCRYPTION_KEY")

    if not key:
        raise RuntimeError("Missing ENCRYPTION_KEY in .env")

    try:
        cipher = Fernet(key.strip().encode("utf-8"))
    except Exception:
        raise RuntimeError(
            "ENCRYPTION_KEY is not a valid Fernet key.\n"
            "Generate one with:\n"
            "python -c \"from cryptography.fernet import Fernet; "
            "print(Fernet.generate_key().decode())\""
        )

    token = input("Token: ").strip()

    encrypted_token = cipher.encrypt(token.encode("utf-8"))

    print("\nEncrypted Token:")
    print(encrypted_token.decode())

    print("\nCopy the value above into your .env as TOKEN_AVIONSTACK=")


if __name__ == "__main__":
    encrypt_credentials()