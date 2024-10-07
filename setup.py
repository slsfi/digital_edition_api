from setuptools import setup

setup(
    name='sls_api',
    packages=['sls_api'],
    version="1.4.1",
    include_package_data=True,
    install_requires=[
        'argon2-cffi==23.1.0',
        'beautifulsoup4==4.12.3',
        'elasticsearch==7.17.9',
        'flask==3.0.3',
        'flask-jwt-extended==4.6.0',
        'flask-sqlalchemy==3.1.1',
        'flask-cors==5.0.0',
        'lxml==5.2.2',
        'mysqlclient==2.2.4',
        'passlib==1.7.4',
        'Pillow==10.3.0',
        'psycopg2-binary==2.9.9',
        'raven[flask]==6.10.0',
        'ruamel.yaml==0.18.6',
        'requests==2.32.2',
        'sqlalchemy==2.0.30',
        'werkzeug==3.0.3',
        'uwsgi==2.0.25.1'
    ]
)
