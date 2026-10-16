"""
setup_keypair.py
================
Generates an RSA keypair for Snowflake keypair authentication.
This bypasses MFA/TOTP requirements for service account connections.

Steps:
  1. Run this script → generates rsa_key.p8 (private) + rsa_key.pub (public)
  2. Go to Snowflake → run the ALTER USER SQL printed by this script
  3. Done — ETL pipeline will use keypair auth automatically

Usage:  python setup_keypair.py
"""
import base64, sys
from pathlib import Path

ROOT = Path(__file__).parent

try:
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
except ImportError:
    print("Run: pip install cryptography")
    sys.exit(1)

KEY_DIR  = ROOT / "keys"
KEY_DIR.mkdir(exist_ok=True)
PRIV_KEY = KEY_DIR / "rsa_key.p8"
PUB_KEY  = KEY_DIR / "rsa_key.pub"

# Generate 2048-bit RSA key
print("Generating RSA 2048-bit keypair ...")
private_key = rsa.generate_private_key(
    public_exponent=65537, key_size=2048, backend=default_backend()
)

# Save private key (unencrypted — store securely)
with open(PRIV_KEY, "wb") as f:
    f.write(private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ))
print(f"  Private key: {PRIV_KEY}")

# Save public key
pub_key_pem = private_key.public_key().public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo,
).decode()

with open(PUB_KEY, "w") as f:
    f.write(pub_key_pem)
print(f"  Public key : {PUB_KEY}")

# Strip PEM headers for Snowflake ALTER USER command
pub_stripped = pub_key_pem.replace("-----BEGIN PUBLIC KEY-----", "")\
                           .replace("-----END PUBLIC KEY-----", "")\
                           .replace("\n", "")

print(f"""
================================================================================
NEXT STEP: Run this in Snowflake (Snowsight SQL Editor):

ALTER USER SIDDIQUE SET RSA_PUBLIC_KEY='{pub_stripped}';

Then verify with:
DESC USER SIDDIQUE;
-- Look for RSA_PUBLIC_KEY in the output

After that, the ETL pipeline and dashboard will connect without MFA/TOTP.
================================================================================
""")

# Update .env to use keypair auth
env_path = ROOT / ".env"
env_text = env_path.read_text(encoding="utf-8")

# Update or add private_key_path
if "private_key_path" in env_text:
    import re
    env_text = re.sub(
        r'^private_key_path\s*=.*$', 
        f'private_key_path = "{PRIV_KEY.as_posix()}"',
        env_text, flags=re.MULTILINE
    )
else:
    env_text = env_text.replace(
        'authenticator = "snowflake"',
        f'authenticator = "snowflake"\nprivate_key_path = "{PRIV_KEY.as_posix()}"'
    )

env_path.write_text(env_text, encoding="utf-8")
print(f"Updated .env with private_key_path")
print(f"\nKeypair files:")
print(f"  {PRIV_KEY}")
print(f"  {PUB_KEY}")
