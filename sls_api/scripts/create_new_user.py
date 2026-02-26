import argparse
import sys

from sls_api.models import User
from sls_api import app

if __name__ == "__main__":
    with app.app_context():
        parser = argparse.ArgumentParser(description="Helper script to create a new API user")
        parser.add_argument("email", help="User email address")
        parser.add_argument("password", help="User password")

        args = parser.parse_args()

        success = User.create_new_user(args.email, args.password)

        if success is None:
            print("Unexpected error creating user. Check API backend logs.")
            sys.exit(1)
        elif success:
            print(f"User {args.email} created successfully!")
            sys.exit(0)
        else:
            print("Unexpected error creating user. Check API backend logs.")
            sys.exit(1)
