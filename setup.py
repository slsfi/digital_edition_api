from setuptools import setup

setup(
    name='sls_api',
    packages=['sls_api'],
    version="1.5.0",
    include_package_data=True,
    install_requires=[
        'argon2-cffi==25.1.0',
        'beautifulsoup4==4.14.3',
        'elasticsearch==7.17.12',
        'flask==3.1.2',
        'flask-jwt-extended==4.7.1',
        'flask-sqlalchemy==3.1.1',
        'flask-cors==6.0.2',
        'lxml==6.0.2',
        'passlib==1.7.4',
        'Pillow==12.0.0',
        'psycopg2==2.9.11',
        'PyMySQL==1.1.2',
        'ruamel.yaml==0.18.17',
        'requests==2.32.5',
        'saxonche==12.9.0',
        'sqlalchemy==2.0.45',
        'werkzeug==3.1.4',
        'uwsgi==2.0.31'
    ]
)
