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
    def find_by_email(cls, email):
        """
        Returns a User object if one exists for the given email, otherwise None
        """
        return cls.query.filter_by(email=email).first()

    def get_projects(self):
        """
        Returns a list of all projects the User can edit
        """
        if self.projects:
            return self.projects.split(",")
        return None

    def get_token_identity(self):
        """
        Generate the JWT identity for the User
        """
        return {
            "sub": self.email,
            "projects": self.get_projects()
        }

    def check_password(self, password):
        """
        Verifies that 'password' matches against the stored password hash for the user
        """
        return pwd_context.verify(password, self.password)

    def can_edit_project(self, project):
        """
        Returns True if the User can edit the given project
        """
        if self.projects:
            return project in self.projects.split(",")
        else:
            return False
