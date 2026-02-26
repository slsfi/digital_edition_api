import argparse
import sys

from sls_api.models import User
from sls_api import app

if __name__ == "__main__":
    with app.app_context():
        parser = argparse.ArgumentParser(description="Helper script to set CMS user flag")
        parser.add_argument("email", help="User email address")

        args = parser.parse_args()

        success = User.set_cms_user(args.email)

        if success:
            print(f"User {args.email} now set as CMS user.")
            sys.exit(0)
        else:
            print(f"No such user {args.email}!")
            sys.exit(1)
