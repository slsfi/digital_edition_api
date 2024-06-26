# REST API for SLS Generic Digital Editions project
- Flask-driven REST API
- Runs on Python 3.11
---
Copyright 2018-2024 Svenska Litteratursällskapet i Finland, r.f.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
---
- For full license text, see `LICENSE` file.

- Installation details:
    - Create config files from _example files in `config` folder
      - Note that environment variables may be used in the YAML files if desired, they are parsed during startup.
    - Ensure volume paths in `docker-compose.yml` point at the correct host and container folders
    - Add SSH private key contents to `ssh_key` file.
    - run `docker-compose build` in root folder containing `Dockerfile` and `docker-compose.yml`

- Running in Production
    - Add SSH private key contents to `ssh_key` file.
    - Start api using `docker-compose up -d`
    - Please note that the default port is 8000, this can be changed in `docker-compose.yml`
    - API can then be accessed at http://127.0.0.1:8000

- Manually testing the API without Docker, using a python virtualenv (not recommended)
    - `source /path/to/virtualenv/bin/activate` or `/path/to/virtualenv/Scripts/activate_this.bat` on Windows
    - `pip install --upgrade -e .`
    - `export FLASK_APP=/path/to/sls_api` or `set FLASK_APP=/path/to/sls_api` on Windows
    - `export FLASK_DEBUG=1` or `set FLASK_DEBUG=1`on Windows to activate DEBUG mode
    - By using the user `test@test.com` with the password `test`, access to all projects in granted in DEBUG mode
    - `flask run` - note that this uses port 5000 by default

### /auth endpoints
- Enables JWT-based authentication towards protected endpoints
- Provides registration, login, and token refresh for users

### /digitaleditions endpoints
- Endpoints used for the SLS Generic Digital Edition platform
- Port of older PHP apis:
    - https://github.com/slsfi/digital_editions_API
    - https://github.com/slsfi/digital_editions_xslt
- Needs connection details for database servers and paths to folders for XML, HTML, and XSL files
    - configs/digital_editions.yml
    - See digital_editions_example.yml for specifics

### SSH configuration
- If API needs pull/push access to private git repositories (defined in `configs/digital_editions.yml`)
    - Mount SSH keys and/or ssh_config files in `/home/uwsgi/.ssh/` inside the container
