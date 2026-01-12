import datetime
from flask_sqlalchemy import SQLAlchemy
from passlib.context import CryptContext


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
    email_verified = db.Column(db.Boolean, nullable=False, default=False, comment="Whether or not this user has verified their email address")
    cms_user = db.Column(db.Boolean, nullable=False, default=False, comment="Whether or not this user should have CMS/Tools access")

    @classmethod
    def create_new_user(cls, email, password):
        """
        Create a new user object in the database and return it
        """
        new_user = cls(
            email=email,
            password=pwd_context.hash(password)
        )
        db.session.add(new_user)
        db.session.commit()
        return cls.query.filter_by(email=email).first()

    @classmethod
    def delete_user(cls, email):
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
    def reset_projects(cls, email, projects):
        user = cls.query.filter_by(email=email).first()
        if user:
            user.projects = projects
            db.session.commit()
            return True
        else:
            return False

    @classmethod
    def find_by_email(cls, email):
        """
        Returns a User object if one exists for the given email, otherwise None
        """
        return cls.query.filter_by(email=email).first()

    @classmethod
    def reset_password(cls, email, password):
        user = cls.query.filter_by(email=email).first()
        if user:
            user.password = pwd_context.hash(password)
            db.session.commit()
            return True
        else:
            return False

    @classmethod
    def update_login_timestamp(cls, email):
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
    def mark_email_verified(cls, email):
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

    def get_projects(self):
        """
        Returns a list of all projects the User can edit
        """
        if self.cms_user:
            if self.projects:
                return self.projects.split(",")
        return None

    def check_password(self, password):
        """
        Verifies that 'password' matches against the stored password hash for the user
        """
        return pwd_context.verify(password, self.password)

    def email_is_verified(self):
        """
        Return email verification status
        """
        return self.email_verified

    def can_edit_project(self, project):
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
