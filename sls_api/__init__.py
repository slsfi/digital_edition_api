from flask import Flask
from flask_jwt_extended import JWTManager
from flask_cors import CORS
import logging
import os
from ruamel.yaml import YAML
from sys import stdout

app = Flask(__name__)
CORS(app)
yaml = YAML(typ="safe")

# First, set up logging
root_logger = logging.getLogger()
if int(os.environ.get("FLASK_DEBUG", 0)) == 1:
    root_logger.setLevel(logging.DEBUG)
else:
    root_logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(stream=stdout)
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%H:%M:%S'))
root_logger.addHandler(stream_handler)

logger = logging.getLogger("sls_api")

# check what config files exists, so we know what blueprints to load
projects_config_exists = os.path.exists(os.path.join("sls_api", "configs", "digital_editions.yml"))
security_config_exists = os.path.exists(os.path.join("sls_api", "configs", "security.yml"))


# Selectively import and register endpoints based on which configs exist and can be loaded
if projects_config_exists:
    from sls_api.endpoints.metadata import meta
    app.register_blueprint(meta, url_prefix="/digitaledition")
    from sls_api.endpoints.facsimiles import facsimiles
    app.register_blueprint(facsimiles, url_prefix="/digitaledition")
    from sls_api.endpoints.media import media
    app.register_blueprint(media, url_prefix="/digitaledition")
    from sls_api.endpoints.occurrences import occurrences
    app.register_blueprint(occurrences, url_prefix="/digitaledition")
    from sls_api.endpoints.search import search
    app.register_blueprint(search, url_prefix="/digitaledition")
    from sls_api.endpoints.songs import songs
    app.register_blueprint(songs, url_prefix="/digitaledition")
    from sls_api.endpoints.text import text
    app.register_blueprint(text, url_prefix="/digitaledition")
    from sls_api.endpoints.workregister import workregister
    app.register_blueprint(workregister, url_prefix="/digitaledition")
    from sls_api.endpoints.correspondence import correspondence
    app.register_blueprint(correspondence, url_prefix="/digitaledition")

if security_config_exists:
    from sls_api.endpoints.auth import auth
    from sls_api.models import db, User
    with open(os.path.join("sls_api", "configs", "security.yml")) as config_file:
        security_config = yaml.load(config_file.read())
        # handle environment variables in the YAML file
        for setting, value in security_config.items():
            security_config[setting] = os.path.expandvars(value)
    app.config["JWT_SECRET_KEY"] = security_config["secret_key"]
    app.config["JWT_TOKEN_LOCATION"] = 'headers'
    app.config["SQLALCHEMY_DATABASE_URI"] = security_config["user_database"]
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    # handle database reconnects in user database backend
    # setting pool_recycle to 300 invalidates old connections after 5 minutes
    # this should prevent errors due to database timeouts (database may not allow long-lived connections)
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"pool_size": 5, "max_overflow": 10, "pool_recycle": 300}

    jwt = JWTManager(app)
    db.init_app(app)

    try:
        # on load, after uwsgi forks, recycle the database connection pool
        # this prevents forks from trying to use invalid connections from the master process
        # wrap this in a try-except since we sometimes run code manually and then this is not possible
        # as uwsgi cannot be imported when python is run manually
        from uwsgidecorators import postfork

        @postfork
        def _recycle_db_pool():
            with app.app_context():
                db.engine.dispose()
    except ImportError:
        logger.warning("Skipping uwsgi postfork as importing uwsgi failed...")
        pass

    app.register_blueprint(auth, url_prefix="/auth")

    with app.app_context():
        # ensure database exists and is populated with test user
        db.create_all()
        if User.find_by_email("test@test.com") is None:
            User.create_new_user("test@test.com", "test")

if projects_config_exists and security_config_exists:
    """
    If we have both a projects config (digital_edition.yml) and a security config (security.yml), load tools endpoints for JWT-protected writing to database
    """
    from sls_api.endpoints.tools.collections import collection_tools
    app.register_blueprint(collection_tools, url_prefix="/digitaledition")
    from sls_api.endpoints.tools.events import event_tools
    app.register_blueprint(event_tools, url_prefix="/digitaledition")
    from sls_api.endpoints.tools.files import file_tools
    app.register_blueprint(file_tools, url_prefix="/digitaledition")
    from sls_api.endpoints.tools.groups import group_tools
    app.register_blueprint(group_tools, url_prefix="/digitaledition")
    from sls_api.endpoints.tools.publications import publication_tools
    app.register_blueprint(publication_tools, url_prefix="/digitaledition")
    from sls_api.endpoints.tools.publishing import publishing_tools
    app.register_blueprint(publishing_tools, url_prefix="/digitaledition")

logger.info(" * Loaded endpoints: {}".format(", ".join(app.blueprints)))


# after every request, make sure CORS headers are set for response
@app.after_request
def set_access_control_headers(response):
    if "Access-Control-Allow-Origin" not in response.headers:
        response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "X-Requested-With, Content-Type, Accept, Origin, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT"

    if response.headers.get("Content-Type") == "application/json":
        response.headers["Content-Type"] = "application/json;charset=utf-8"

    return response
