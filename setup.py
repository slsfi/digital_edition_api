from setuptools import setup

setup(
    name='sls_api',
    packages=['sls_api'],
    version="1.4.1",
    include_package_data=True,
    install_requires=[
        'argon2-cffi==23.1.0',
        'beautifulsoup4==4.12.3',
        'elasticsearch==7.17.12',
        'flask==3.1.0',
        'flask-jwt-extended==4.7.1',
        'flask-sqlalchemy==3.1.1',
        'flask-cors==5.0.0',
        'lxml==5.3.0',
        'mysqlclient==2.2.6',
        'passlib==1.7.4',
        'Pillow==11.0.0',
        'psycopg2-binary==2.9.10',
        'raven[flask]==6.10.0',
        'ruamel.yaml==0.18.6',
        'requests==2.32.3',
        'sqlalchemy==2.0.36',
        'werkzeug==3.1.3',
        'uwsgi==2.0.28'
    ]
)
