import datetime
from flask_sqlalchemy import SQLAlchemy
from passlib.context import CryptContext
import time
from typing import Iterable


pwd_context = CryptContext(
    schemes=["argon2", "pbkdf2_sha512", "pbkdf2_sha256"],
    deprecated="auto"
)

db = SQLAlchemy()


class User(db.Model):
    __tablename__ = 'users'

    ident = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.Unicode(255), unique=True, nullable=False)
    password = db.Column(db.UnicodeText, nullable=False)
    projects = db.Column(db.UnicodeText, nullable=True, comment="Comma-separated list of projects this user has edit rights to")
    created_timestamp = db.Column(db.DateTime, nullable=False, default=datetime.datetime.now, comment="Date and time this user was registered")
    last_login_timestamp = db.Column(db.DateTime, nullable=True, default=None, comment="Date and time this user last logged in")
    tokens_valid_after = db.Column(db.Integer, nullable=True, default=None, comment="Unix timestamp after which this user's tokens are considered valid")
    email_verified = db.Column(db.Boolean, nullable=False, default=False, comment="Whether or not this user has verified their email address")
    cms_user = db.Column(db.Boolean, nullable=False, default=False, comment="Whether or not this user should have CMS/Tools access")
    name = db.Column(db.UnicodeText, nullable=False, comment="Name of user")
    country = db.Column(db.UnicodeText, nullable=True, default=None, comment="Optional country field for user")
    intended_usage = db.Column(db.UnicodeText, nullable=True, default=None, comment="Optional intended usage for user")

    @classmethod
    def create_new_user(cls, name: str, email: str, password: str) -> "User | None":
        """
        Create a new user object in the database and return it
        """
        new_user = cls(
            name=name,
            email=email,
            password=pwd_context.hash(password),
            tokens_valid_after=int(time.time())
        )
        db.session.add(new_user)
        db.session.commit()
        return cls.query.filter_by(email=email).first()

    @classmethod
    def set_name(cls, email: str, name: str) -> bool:
        """
        Update name for user with the specified email
        """
        user = cls.query.filter_by(email=email).first()
        if user:
            user.name = name
            db.session.commit()
            return True
        else:
            return False

    @classmethod
    def set_country(cls, email: str, country: str) -> bool:
        """
        Update country for user with the specified email
        """
        user = cls.query.filter_by(email=email).first()
        if user:
            user.country = country
            db.session.commit()
            return True
        else:
            return False

    @classmethod
    def set_intended_usage(cls, email: str, intended_usage: str) -> bool:
        """
        Update intended usage for user with the specified email
        """
        user = cls.query.filter_by(email=email).first()
        if user:
            user.intended_usage = intended_usage
            db.session.commit()
            return True
        else:
            return False

    @classmethod
    def delete_user(cls, email: str) -> bool:
        """
        Delete the user with the given email
        """
        user = cls.query.filter_by(email=email).first()
        if user:
            db.session.delete(user)
            db.session.commit()
            return True
        else:
            return True

    @classmethod
    def reset_projects(cls, email: str, projects: str) -> bool:
        user = cls.query.filter_by(email=email).first()
        if user:
            user.projects = projects
            db.session.commit()
            return True
        else:
            return False

    @classmethod
    def set_cms_user(cls, email: str) -> bool:
        user = cls.query.filter_by(email=email).first()
        if user:
            user.email_verified = True
            user.cms_user = True
            db.session.commit()
            return True
        else:
            return False

    @classmethod
    def find_by_email(cls, email: str) -> "User | None":
        """
        Returns a User object if one exists for the given email, otherwise None
        """
        return cls.query.filter_by(email=email).first()

    @classmethod
    def reset_password(cls, email: str, password: str) -> bool:
        user = cls.query.filter_by(email=email).first()
        if user:
            user.password = pwd_context.hash(password)
            db.session.commit()
            return True
        else:
            return False

    @classmethod
    def update_login_timestamp(cls, email: str) -> bool:
        """
        Updates last_login_timestamp for user (to be called on successful login)
        Returns true on success
        """
        user = cls.query.filter_by(email=email).first()
        if user:
            user.last_login_timestamp = datetime.datetime.now()
            db.session.commit()
            return True
        else:
            return False

    @classmethod
    def mark_email_verified(cls, email: str) -> bool:
        """
        Marks a user's email address as verified
        Returns true on success
        """
        user = cls.query.filter_by(email=email).first()
        if user:
            user.email_verified = True
            db.session.commit()
            return True
        else:
            return False

    @classmethod
    def check_token_validity(cls, email: str, unix_time: int) -> bool:
        """
        Check if a timestamp from a JWT is after the user's first validity time
        Returns true if and only if:
            - user exists
            - timestamp is after first validity
        """
        user = cls.query.filter_by(email=email).first()
        if user:
            return unix_time >= user.tokens_valid_after
        else:
            return False

    @classmethod
    def reset_token_validity(cls, email: str) -> bool:
        """
        Update token first validity timestamp for user
        Returns true on success
        """
        user = cls.query.filter_by(email=email).first()
        if user:
            user.tokens_valid_after = int(time.time()) + 1
            db.session.commit()
            return True
        else:
            return False

    def get_projects(self) -> Iterable | None:
        """
        Returns a list of all projects the User can edit
        """
        if self.cms_user:
            if self.projects:
                return self.projects.split(",")
        return None

    def check_password(self, password: str) -> bool:
        """
        Verifies that 'password' matches against the stored password hash for the user
        """
        return pwd_context.verify(password, self.password)

    def email_is_verified(self) -> bool:
        """
        Return email verification status
        """
        return self.email_verified

    def can_edit_project(self, project: str) -> bool:
        """
        Returns True if the User can edit the given project
        """
        if self.cms_user:
            if self.projects:
                return project in self.projects.split(",")
            else:
                return False
        else:
            return False
