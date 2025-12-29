from setuptools import setup

setup(
    name='sls_api',
    packages=['sls_api'],
    version="1.4.1",
    include_package_data=True,
    install_requires=[
        'argon2-cffi==25.1.0',
        'beautifulsoup4==4.13.5',
        'elasticsearch==7.17.12',
        'flask==3.1.2',
        'flask-jwt-extended==4.7.1',
        'flask-sqlalchemy==3.1.1',
        'flask-cors==6.0.1',
        'lxml==6.0.1',
        'mysqlclient==2.2.7',
        'passlib==1.7.4',
        'Pillow==11.3.0',
        'psycopg2-binary==2.9.10',
        'ruamel.yaml==0.18.15',
        'requests==2.32.5',
        'saxonche==12.8.0',
        'sqlalchemy==2.0.43',
        'werkzeug==3.1.3',
        'uwsgi==2.0.30'
    ]
)
