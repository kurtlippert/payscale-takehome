# Getting Started
Welcome to the take home project! By following the steps below, you will run an API that will accept a csv file, create some metadata, and the store the contents of the csv. It's pretty bare-bones, so feel free to add things to it as you wish. You'll find some todos littered through the code and you'll also notice that there are no tests.

The real challenge to take the data that is incoming and help clean it up. Specifically, all of the primary keys of the employees (the employee ids) and the foreign key of the job code both need to be trimmed and uppercased. Good luck!

## To set up the db
First off, you need a postgres instance to connect to. One way to do this would be pull and run an image from docker.
```bash
docker pull postgres:10.7
docker run --name takehomedb -e POSTGRES_PASSWORD=test -e POSTGRES_USER=test -e POSTGRES_DB=fileLoader -p 5432:5432 postgres:10.7
```

Then you need to create the tables and schema necessary. In the root of this project start your virtual environment and load the project's dependencies.
```bash
# Create the virtual environment
python3.7 -m venv .venv
# Activate the virtual environment. See venv docs for platform-specific activation commands
# (https://docs.python.org/3/library/venv.html#creating-virtual-environments). The below one is for
# Posix platforms
source .venv/bin/activate
pip install -r requirements.txt
```

Setup the db by running the Alembic migrations
```bash
cd db
alembic upgrade head
```

The database is a postgres db. You should now see the tables public.alembic_version, public.file_content, and public.upload_file have been created.

## Testing the service initially
In the root of this project, be sure that the virtual environment is up. Then run `flask run`.

If you have completed the above, you can now navigate to localhost:5000, you should see a "Hello, Take Home Project!"

To do an upload, POST to http://localhost:5000/processUpload with a form-data body of file with the employee-test.csv found in the data folder. You should see entries added in the upload_file, data_frame tables and a new table for the data_frame_content created.