#!/usr/bin/env python3
import jwt
import os
import time
from dotenv import load_dotenv

load_dotenv()

GH_APP_ID = os.getenv("GH_APP_ID")
GH_PEM_PATH = os.getenv("GH_PEM_PATH")

with open(GH_PEM_PATH, "rb") as pem_file:
    signing_key = jwt.jwk_from_pem(pem_file.read())

payload = {
    "iat": int(time.time()),
    "exp": int(time.time()) + 60,
    "iss": GH_APP_ID
}

jwt_instance = jwt.JWT()
encoded_jwt = jwt_instance.encode(payload, signing_key, alg="RS256")
print(encoded_jwt)
