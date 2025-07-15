#!/usr/bin/env python3
"""
Generate a Fernet key for Airflow configuration.
Run this script to generate a valid Fernet key and save it to .env file.
"""

from cryptography.fernet import Fernet
import os

def generate_fernet_key():
    """Generate a new Fernet key."""
    return Fernet.generate_key().decode()

if __name__ == "__main__":
    fernet_key = generate_fernet_key()
    print(f"Generated Fernet key: {fernet_key}")
    
    # Save to .env file
    with open('.env', 'w') as f:
        f.write(f"FERNET_KEY={fernet_key}\n")
    
    print("Fernet key saved to .env file")
    print("You can now run: docker-compose up") 